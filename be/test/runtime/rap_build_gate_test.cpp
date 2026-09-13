// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/rap_build_gate.h"

#include <atomic>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#ifndef RAP_GATE_STANDALONE
#include <gtest/gtest.h>
#endif

namespace {
using starrocks::RapBuildGate;

void check(bool value, const char* name) {
    if (!value) throw std::runtime_error(name);
}

void lifecycle() {
    RapBuildGate gate("boot1");
    RapBuildGate::Spec a{"a", "hash-a", "gs://bucket/a", "col"};
    RapBuildGate::Spec b{"b", "hash-b", "gs://bucket/b", "col"};
    check(!gate.acquire("a"), "unknown attempt");
    check(!gate.open("wrong", 0, a), "wrong boot");
    check(gate.open("boot1", 0, a), "open");
    check(gate.open("boot1", 0, a), "open acknowledgement recovery");
    auto lease = gate.acquire("a");
    check(lease != nullptr && gate.snapshot().active_builders == 1, "lease counted");
    check(!gate.open("boot1", 1, b), "overlapping attempt");
    check(!gate.fence("boot1", 1, "a", "wrong"), "wrong hash fence");
    check(gate.fence("boot1", 1, "a", "hash-a"), "fence");
    check(!gate.acquire("a"), "late builder rejected");
    check(!gate.open("boot1", 0, a), "open cannot reopen fence");
    check(!gate.open("boot1", 1, b), "fenced is not drained");
    check(lease->spec().directory == a.directory, "immutable output");
    lease.reset();
    check(gate.snapshot().active_builders == 0, "lease released");
    check(gate.open("boot1", 1, b), "next attempt");
    check(!gate.open("boot1", 0, a), "delayed old open");
    check(!gate.acquire("a"), "superseded token");
    check(!gate.fence("boot1", 1, "a", "hash-a"), "delayed old fence");
    check(gate.acquire("b") != nullptr, "next attempt unaffected");
    check(gate.fence("boot1", 2, "b", "hash-b"), "close next attempt");
    check(!gate.open("boot1", 0, a), "stale open after newer fence");

    RapBuildGate restarted("boot2");
    check(!restarted.open("boot1", 0, a), "restarted backend rejects old open");
    check(!restarted.acquire("a"), "restarted backend rejects old query");
    check(!restarted.fence("boot1", 1, "a", "hash-a"), "restart is not a drain receipt");
    check(restarted.fence("boot2", 1, "a", "hash-a"), "abort before delayed open");
    check(!restarted.open("boot2", 0, a), "aborted delayed open");
    check(restarted.open("boot2", 1, b), "recover after aborted open");
}

void concurrency() {
    RapBuildGate gate("boot");
    check(gate.open("boot", 0, {"a", "h", "gs://b/a", "v"}), "concurrent open");
    std::atomic<bool> start{false};
    std::atomic<int> admitted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            while (!start.load()) std::this_thread::yield();
            for (int j = 0; j < 1000; ++j) {
                if (auto lease = gate.acquire("a")) ++admitted;
            }
        });
    }
    start = true;
    check(gate.fence("boot", 1, "a", "h"), "concurrent fence");
    for (int i = 0; i < 1000; ++i) check(!gate.acquire("a"), "post-fence acquisition");
    for (auto& thread : threads) thread.join();
    check(gate.snapshot().active_builders == 0, "all leases drained");
}

// Line bridge for Python integration tests: executes the production C++ gate,
// not a Python replacement for its lifecycle rules. HTTP parsing is a separate boundary.
void bridge(const std::string& boot) {
    RapBuildGate gate(boot);
    std::vector<std::unique_ptr<RapBuildGate::Lease>> leases;
    std::string line;
    while (std::getline(std::cin, line)) {
        std::istringstream input(line);
        std::string op, boot, token, hash, directory, column;
        uint64_t generation = 0;
        input >> op;
        bool ok = true;
        if (op == "open") {
            input >> boot >> generation >> token >> hash >> directory >> column;
            ok = gate.open(boot, generation, {token, hash, directory, column});
        } else if (op == "fence") {
            input >> boot >> generation >> token >> hash;
            ok = gate.fence(boot, generation, token, hash);
        } else if (op == "acquire") {
            input >> token;
            auto lease = gate.acquire(token);
            ok = lease != nullptr;
            if (lease) leases.push_back(std::move(lease));
        } else if (op == "release") {
            leases.clear();
        } else if (op != "status") {
            ok = false;
        }
        auto s = gate.snapshot();
        std::cout << ok << ' ' << s.boot << ' ' << s.generation << ' ' << s.fenced << ' ' << s.active_builders
                  << ' ' << (s.spec.token.empty() ? "-" : s.spec.token)
                  << ' ' << (s.spec.request_sha256.empty() ? "-" : s.spec.request_sha256)
                  << ' ' << (s.spec.directory.empty() ? "-" : s.spec.directory)
                  << ' ' << (s.spec.column.empty() ? "-" : s.spec.column) << std::endl;
    }
}
} // namespace

#ifdef RAP_GATE_STANDALONE
int main(int argc, char** argv) {
    try {
        if (argc >= 2 && std::string(argv[1]) == "--bridge") bridge(argc == 3 ? argv[2] : "test-boot");
        else {
            lifecycle();
            concurrency();
            std::cout << "PASS lifecycle and concurrent fence/acquire tests\n";
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
#else
TEST(RapBuildGateTest, Lifecycle) { lifecycle(); }
TEST(RapBuildGateTest, ConcurrentFence) { concurrency(); }
#endif
