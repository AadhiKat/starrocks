// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "runtime/rap_index_preloader.h"

#include <atomic>
#include <iostream>
#include <stdexcept>

#ifndef RAP_PRELOAD_STANDALONE
#include <gtest/gtest.h>
#endif

namespace {
using P = starrocks::RapIndexPreloader;
using namespace std::chrono_literals;

void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }

P::Request request(std::string id, std::string file = "a") {
    return {std::move(id), "gs://bucket/" + file, "gs://bucket/index", "g1", "model", 100, 11, 0, 3, 4};
}

struct Latch {
    std::mutex mutex;
    std::condition_variable cv;
    int started = 0;
    bool released = false;
    void enter() {
        std::unique_lock lock(mutex);
        ++started;
        cv.notify_all();
        cv.wait(lock, [&] { return released; });
    }
    void await(int n) {
        std::unique_lock lock(mutex);
        check(cv.wait_for(lock, 5s, [&] { return started >= n; }), "loader did not reach latch");
    }
    void release() { std::lock_guard lock(mutex); released = true; cv.notify_all(); }
};
struct ReleaseOnExit { Latch& latch; ~ReleaseOnExit() { latch.release(); } };

P::Snapshot done(P& pool, const std::string& id) {
    P::Snapshot s;
    auto until = std::chrono::steady_clock::now() + 5s;
    do {
        check(pool.status(id, &s), "receipt missing");
        if (s.terminal) return s;
        std::this_thread::sleep_for(1ms);
    } while (std::chrono::steady_clock::now() < until);
    throw std::runtime_error("receipt did not complete");
}

P::Prepared loaded(std::atomic<int>& admits) {
    P::Result r{"LOADED"}; r.bytes = 3; r.attempts = 1; r.read_ns = 7;
    return {r, [&admits, r] { ++admits; return r; }};
}

void success_and_identity() {
    Latch latch;
    std::atomic<int> calls{0}, admits{0};
    P pool([&](const auto&) { ++calls; latch.enter(); return loaded(admits); }, [](const auto&) { return true; });
    ReleaseOnExit release{latch};
    auto r = request("a");
    check(pool.submit(r).state == "ACCEPTED", "first admission");
    latch.await(1);
    check(pool.submit(r).state == "EXISTING", "same id idempotent");
    auto duplicate = r; duplicate.id = "b";
    check(pool.submit(duplicate).id == "a", "same work shares original receipt");
    auto conflict = r; conflict.generation = "g2";
    check(pool.submit(conflict).state == "CONFLICT", "id cannot change generation");
    conflict = r; conflict.directory += "2";
    check(pool.submit(conflict).state == "CONFLICT", "id cannot change root");
    conflict = r; conflict.field_id++;
    check(pool.submit(conflict).state == "CONFLICT", "id cannot change schema");
    latch.release();
    auto s = done(pool, "a");
    check(s.result.state == "LOADED" && s.result.bytes == 3 && s.result.read_ns == 7, "loader evidence preserved");
    check(calls == 1 && admits == 1 && pool.counters().deduplicated == 2, "singleflight counts");
    check(pool.counters().active == 0 && pool.counters().reserved_bytes == 0, "reservation released");
}

void bounded_queue_and_cancel() {
    Latch latch;
    std::atomic<int> calls{0}, admits{0};
    P pool([&](const auto&) { ++calls; latch.enter(); return loaded(admits); }, [](const auto&) { return true; }, 2, 1, 4);
    ReleaseOnExit release{latch};
    check(pool.submit(request("a")).state == "ACCEPTED", "admit active");
    latch.await(1);
    check(pool.submit(request("b", "b")).state == "ACCEPTED", "admit queued");
    check(pool.submit(request("c", "c")).state == "FULL", "queue capacity enforced");
    check(pool.cancel("b") && done(pool, "b").result.state == "CANCELLED", "queued cancel receipt");
    check(calls == 1, "cancelled queued job never fetched");
    check(pool.submit(request("c", "c")).state == "ACCEPTED", "terminal receipt evicted for new work");
    P::Snapshot old;
    check(!pool.status("b", &old), "evicted receipt is unknown");
    latch.release();
    check(done(pool, "a").result.state == "LOADED", "first completes");
    check(done(pool, "c").result.state == "LOADED", "queued replacement completes");
    check(calls == 2 && admits == 2, "only admitted jobs loaded");
}

void running_cancel_and_stale() {
    for (bool cancel : {false, true}) {
        Latch latch;
        std::atomic<bool> current{true};
        std::atomic<int> admits{0};
        P pool([&](const auto&) { latch.enter(); return loaded(admits); }, [&](const auto&) { return current.load(); });
        ReleaseOnExit release{latch};
        pool.submit(request("a")); latch.await(1);
        if (cancel) check(pool.cancel("a"), "cancel running");
        else current = false;
        latch.release();
        check(done(pool, "a").result.state == (cancel ? "CANCELLED" : "STALE"), "late admission blocked");
        check(admits == 0, "cancel/stale never inserts cache entry");
    }
}

void byte_budget_and_worker_limit() {
    Latch latch;
    std::atomic<int> calls{0}, admits{0};
    P pool([&](const auto&) { ++calls; latch.enter(); return loaded(admits); }, [](const auto&) { return true; }, 4, 2, 4);
    ReleaseOnExit release{latch};
    auto too_big = request("large"); too_big.max_bytes = 5;
    check(pool.submit(too_big).state == "REFUSED", "oversized reservation refused");
    pool.submit(request("a")); latch.await(1);
    pool.submit(request("b", "b"));
    P::Snapshot b;
    check(pool.status("b", &b) && b.result.state == "QUEUED", "byte budget holds second worker");
    check(pool.counters().active == 1 && pool.counters().reserved_bytes == 4, "active byte budget exact");
    latch.release();
    done(pool, "a"); done(pool, "b");
    check(calls == 2 && pool.counters().reserved_bytes == 0, "budget reused and released");
}

void failures_and_shutdown() {
    std::atomic<int> admits{0};
    P pool([&](const auto& r) -> P::Prepared {
        if (r.id == "throw") throw std::runtime_error("private remote body");
        return loaded(admits);
    }, [](const auto&) { return true; });
    pool.submit(request("throw"));
    check(done(pool, "throw").result.state == "ERROR", "loader exception contained");
    pool.submit(request("good", "b"));
    check(done(pool, "good").result.state == "LOADED", "worker survives load failure");
    pool.shutdown();
    check(pool.submit(request("late")).state == "REFUSED", "shutdown refuses new jobs");
    P refused([](const auto&) -> P::Prepared { throw std::runtime_error("must not load"); },
              [](const auto&) -> bool { throw std::runtime_error("bad eligibility probe"); });
    check(refused.submit(request("x")).state == "REFUSED", "eligibility error is not permission");
}

void shutdown_during_load() {
    Latch latch;
    std::atomic<int> admits{0};
    P pool([&](const auto&) { latch.enter(); return loaded(admits); }, [](const auto&) { return true; }, 2, 1);
    ReleaseOnExit release{latch};
    pool.submit(request("a")); latch.await(1);
    pool.submit(request("b", "b"));
    std::thread stopper([&] { pool.shutdown(); });
    // Observe queued cancellation as the shutdown barrier before releasing I/O.
    auto b = done(pool, "b");
    latch.release();
    stopper.join();
    check(b.result.state == "CANCELLED" && done(pool, "a").result.state == "CANCELLED", "shutdown drains cancellation");
    check(admits == 0 && pool.counters().active == 0, "shutdown cannot admit late cache result");
}
} // namespace

#ifdef RAP_PRELOAD_STANDALONE
int main() {
    try {
        success_and_identity(); bounded_queue_and_cancel(); running_cancel_and_stale();
        byte_budget_and_worker_limit(); failures_and_shutdown(); shutdown_during_load();
        std::cout << "PASS: 6 real preloader lifecycle/concurrency cases\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n'; return 1;
    }
}
#else
TEST(RapIndexPreloaderTest, SuccessAndIdentity) { success_and_identity(); }
TEST(RapIndexPreloaderTest, BoundedQueueAndCancel) { bounded_queue_and_cancel(); }
TEST(RapIndexPreloaderTest, RunningCancelAndStale) { running_cancel_and_stale(); }
TEST(RapIndexPreloaderTest, ByteBudgetAndWorkerLimit) { byte_budget_and_worker_limit(); }
TEST(RapIndexPreloaderTest, FailuresAndShutdown) { failures_and_shutdown(); }
TEST(RapIndexPreloaderTest, ShutdownDuringLoad) { shutdown_during_load(); }
#endif
