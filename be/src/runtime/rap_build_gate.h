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

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace starrocks {

// One bounded, process-local maintenance slot. No TTL may reopen an old attempt.
// A boot identity and compare-and-swap generation fence delayed control requests;
// the attempt token fences delayed scan builders independently of FE task state.
class RapBuildGate {
public:
    struct Spec {
        std::string token;
        std::string request_sha256;
        std::string directory;
        std::string column;

        bool operator==(const Spec& other) const {
            return token == other.token && request_sha256 == other.request_sha256 && directory == other.directory &&
                   column == other.column;
        }
    };

    struct Snapshot {
        std::string boot;
        uint64_t generation = 0;
        Spec spec;
        bool fenced = true;
        uint64_t active_builders = 0;
    };

private:
    struct State {
        std::mutex mutex;
        Snapshot value;
    };

public:
    class Lease {
    public:
        ~Lease() {
            std::lock_guard lock(_state->mutex);
            --_state->value.active_builders;
        }
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        const Spec& spec() const { return _spec; }

    private:
        friend class RapBuildGate;
        Lease(std::shared_ptr<State> state, Spec spec) : _state(std::move(state)), _spec(std::move(spec)) {}
        std::shared_ptr<State> _state;
        const Spec _spec;
    };

    explicit RapBuildGate(std::string boot) : _state(std::make_shared<State>()) {
        _state->value.boot = std::move(boot);
    }

    static RapBuildGate& instance();

    Snapshot snapshot() const {
        std::lock_guard lock(_state->mutex);
        return _state->value;
    }

    // False is a conflict, not permission to retry with newly read preconditions.
    bool open(const std::string& boot, uint64_t previous_generation, const Spec& spec) {
        std::lock_guard lock(_state->mutex);
        auto& v = _state->value;
        if (boot != v.boot || spec.token.empty() || previous_generation == std::numeric_limits<uint64_t>::max()) {
            return false;
        }
        if (v.generation == previous_generation + 1 && v.spec == spec) {
            return !v.fenced; // A lost open acknowledgement must not reopen a fenced attempt.
        }
        if (v.generation != previous_generation || !v.fenced || v.active_builders != 0 || v.spec.token == spec.token) {
            return false;
        }
        v.spec = spec;
        ++v.generation;
        v.fenced = false;
        return true;
    }

    bool fence(const std::string& boot, uint64_t generation, const std::string& token,
               const std::string& request_sha256) {
        std::lock_guard lock(_state->mutex);
        auto& v = _state->value;
        // Abort an open whose acknowledgement was lost, even if its request has
        // not arrived yet. Reserve its generation as closed before returning.
        if (boot == v.boot && generation > 0 && generation - 1 == v.generation && v.fenced &&
            v.active_builders == 0 && !token.empty()) {
            v.generation = generation;
            v.spec = {token, request_sha256, "", ""};
            return true;
        }
        if (boot != v.boot || generation != v.generation || token != v.spec.token ||
            request_sha256 != v.spec.request_sha256 || token.empty()) {
            return false;
        }
        v.fenced = true;
        return true;
    }

    // Lease construction, counting and fencing share one lock. Keep the lease alive
    // until the final synchronous object close (including exceptional exits).
    std::unique_ptr<Lease> acquire(const std::string& token) {
        std::lock_guard lock(_state->mutex);
        auto& v = _state->value;
        if (token.empty() || token != v.spec.token || v.fenced) return nullptr;
        auto lease = std::unique_ptr<Lease>(new Lease(_state, v.spec));
        ++v.active_builders;
        return lease;
    }

private:
    std::shared_ptr<State> _state;
};

} // namespace starrocks
