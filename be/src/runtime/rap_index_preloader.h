// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace starrocks {

// Bounded BE-local preload admission. The service injects storage/cache operations;
// runtime owns no filesystem credentials, cache handles or query/fragment pointers.
class RapIndexPreloader {
public:
    struct Request {
        std::string id;
        std::string file_uri;
        std::string directory;
        std::string generation;
        std::string column;
        uint64_t file_size = 0;
        uint64_t file_rows = 0;
        int64_t modification_time = 0;
        int32_t field_id = -1;
        uint64_t max_bytes = 0;

        bool same_work(const Request& r) const {
            return std::tie(file_uri, directory, generation, column, file_size, file_rows, modification_time,
                            field_id, max_bytes) ==
                   std::tie(r.file_uri, r.directory, r.generation, r.column, r.file_size, r.file_rows,
                            r.modification_time, r.field_id, r.max_bytes);
        }
    };

    struct Result {
        std::string state = "QUEUED";
        uint64_t bytes = 0;
        uint64_t attempts = 0;
        int64_t exists_ns = 0;
        int64_t open_ns = 0;
        int64_t size_ns = 0;
        int64_t read_ns = 0;
        int64_t parse_ns = 0;
        int64_t admit_ns = 0;
    };

    struct Prepared {
        Result result;
        // Only short cache admission, never remote I/O. Serialized with cancel/stop.
        std::function<Result()> admit;
    };

    struct Snapshot {
        Request request;
        Result result;
        int64_t elapsed_ns = 0;
        bool terminal = false;
    };

    struct Admission {
        std::string state;
        std::string id;
    };

    struct Counters {
        uint64_t submitted = 0;
        uint64_t deduplicated = 0;
        uint64_t rejected = 0;
        uint64_t completed = 0;
        uint64_t active = 0;
        uint64_t reserved_bytes = 0;
    };

    using Loader = std::function<Prepared(const Request&)>;
    using Eligible = std::function<bool(const Request&)>;

    RapIndexPreloader(Loader loader, Eligible eligible, size_t capacity = 64, size_t workers = 2,
                      uint64_t byte_budget = 16 * 1024 * 1024)
            : _loader(std::move(loader)),
              _eligible(std::move(eligible)),
              _capacity(capacity),
              _worker_limit(workers),
              _byte_budget(byte_budget) {}
    ~RapIndexPreloader() { shutdown(); }

    Admission submit(const Request& request) {
        std::lock_guard lock(_mutex);
        if (_stopping || !_capacity || !_worker_limit || !request.max_bytes || request.max_bytes > _byte_budget ||
            !eligible(request)) {
            ++_counters.rejected;
            return {"REFUSED", request.id};
        }
        if (auto it = _jobs.find(request.id); it != _jobs.end()) {
            if (!it->second->snapshot.request.same_work(request)) {
                ++_counters.rejected;
                return {"CONFLICT", request.id};
            }
            ++_counters.deduplicated;
            return {"EXISTING", request.id};
        }
        // Different client ids still coalesce identical work while it is in flight.
        for (const auto& [id, job] : _jobs) {
            if (!job->snapshot.terminal && job->snapshot.request.same_work(request)) {
                ++_counters.deduplicated;
                return {"EXISTING", id};
            }
        }
        if (_jobs.size() >= _capacity) {
            auto oldest = _jobs.end();
            for (auto it = _jobs.begin(); it != _jobs.end(); ++it) {
                if (it->second->snapshot.terminal &&
                    (oldest == _jobs.end() || it->second->sequence < oldest->second->sequence)) oldest = it;
            }
            if (oldest == _jobs.end()) {
                ++_counters.rejected;
                return {"FULL", request.id};
            }
            _jobs.erase(oldest);
        }
        // Lazy workers: a disabled/unused endpoint creates no background threads.
        while (_threads.size() < _worker_limit) {
            try {
                _threads.emplace_back([this] { work(); });
            } catch (...) {
                if (_threads.empty()) {
                    ++_counters.rejected;
                    return {"UNAVAILABLE", request.id};
                }
                break; // A smaller worker pool is safe; never exceed the limit.
            }
        }
        auto job = std::make_shared<Job>();
        job->snapshot.request = request;
        job->sequence = ++_sequence;
        job->created = Clock::now();
        _jobs.emplace(request.id, job);
        _queue.push_back(job);
        ++_counters.submitted;
        _cv.notify_all();
        return {"ACCEPTED", request.id};
    }

    bool status(const std::string& id, Snapshot* out) const {
        std::lock_guard lock(_mutex);
        auto it = _jobs.find(id);
        if (it == _jobs.end()) return false; // Expired receipts are unknown, never READY.
        *out = it->second->snapshot;
        if (!out->terminal) out->elapsed_ns = ns(Clock::now() - it->second->created);
        return true;
    }

    bool cancel(const std::string& id) {
        std::lock_guard lock(_mutex);
        auto it = _jobs.find(id);
        if (it == _jobs.end()) return false;
        auto& job = it->second;
        if (job->snapshot.terminal) return true; // Cannot undo completed cache admission.
        job->cancelled = true;
        if (job->snapshot.result.state == "QUEUED") {
            _queue.erase(std::remove(_queue.begin(), _queue.end(), job), _queue.end());
            finish(job, Result{"CANCELLED"});
        }
        _cv.notify_all();
        return true;
    }

    Counters counters() const {
        std::lock_guard lock(_mutex);
        return _counters;
    }

    void shutdown() {
        {
            std::lock_guard lock(_mutex);
            _stopping = true;
            for (auto& [id, job] : _jobs) job->cancelled = true;
            for (auto& job : _queue) finish(job, Result{"CANCELLED"});
            _queue.clear();
            _cv.notify_all();
        }
        // Running filesystem calls finish under connector timeouts; never detach
        // threads that could outlive the filesystem registry or DataCache.
        for (auto& thread : _threads) if (thread.joinable()) thread.join();
        _threads.clear();
    }

private:
    using Clock = std::chrono::steady_clock;
    struct Job {
        Snapshot snapshot;
        Clock::time_point created;
        uint64_t sequence = 0;
        bool cancelled = false;
    };
    static int64_t ns(Clock::duration d) { return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count(); }
    bool eligible(const Request& r) noexcept {
        try { return _eligible(r); } catch (...) { return false; }
    }
    void finish(const std::shared_ptr<Job>& job, Result result) {
        job->snapshot.result = std::move(result);
        job->snapshot.terminal = true;
        job->snapshot.elapsed_ns = ns(Clock::now() - job->created);
        ++_counters.completed;
    }
    void work() {
        while (true) {
            std::shared_ptr<Job> job;
            {
                std::unique_lock lock(_mutex);
                _cv.wait(lock, [&] {
                    return _stopping || (!_queue.empty() &&
                        _queue.front()->snapshot.request.max_bytes <= _byte_budget - _counters.reserved_bytes);
                });
                if (_stopping) return;
                job = _queue.front();
                _queue.pop_front();
                _counters.reserved_bytes += job->snapshot.request.max_bytes;
                ++_counters.active;
                job->snapshot.result.state = "LOADING";
            }
            Prepared prepared;
            try {
                prepared = eligible(job->snapshot.request) ? _loader(job->snapshot.request)
                                                           : Prepared{Result{"STALE"}, {}};
            } catch (...) {
                prepared.result.state = "ERROR"; // No remote error text or payload in receipts.
            }
            {
                std::lock_guard lock(_mutex);
                if (_stopping || job->cancelled) {
                    prepared.result.state = "CANCELLED";
                } else if (!eligible(job->snapshot.request)) {
                    prepared.result.state = "STALE";
                } else if (prepared.admit) {
                    try {
                        prepared.result = prepared.admit();
                    } catch (...) {
                        prepared.result.state = "ERROR";
                    }
                }
                _counters.reserved_bytes -= job->snapshot.request.max_bytes;
                --_counters.active;
                finish(job, std::move(prepared.result));
                _cv.notify_all();
            }
        }
    }

    Loader _loader;
    Eligible _eligible;
    const size_t _capacity;
    const size_t _worker_limit;
    const uint64_t _byte_budget;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    bool _stopping = false;
    uint64_t _sequence = 0;
    Counters _counters;
    std::map<std::string, std::shared_ptr<Job>> _jobs;
    std::deque<std::shared_ptr<Job>> _queue;
    std::vector<std::thread> _threads;
};

} // namespace starrocks
