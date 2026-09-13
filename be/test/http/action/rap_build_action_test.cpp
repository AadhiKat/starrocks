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

#include "http/action/rap_build_action.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <atomic>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "base/testutil/assert.h"
#include "platform/http/ev_http_server.h"
#include "platform/http/http_client.h"
#include "platform/http/http_request.h"
#include "runtime/rap_build_gate.h"

namespace starrocks {

// Real request parsing, dispatch, handler and gate, with only the FE auth RPC
// replaced. No BE service process, catalog, storage root or remote object store.
class RapBuildActionTest : public testing::Test {
protected:
    void SetUp() override {
        before = RapBuildGate::instance().snapshot();
        ASSERT_TRUE(before.fenced);
        ASSERT_EQ(0, before.active_builders);
        token = token_for(before.generation + 1);
        server = std::make_unique<EvHttpServer>("127.0.0.1", 0, 1);
        ASSERT_TRUE(server->register_handler(GET, "/api/rap/build", &handler));
        ASSERT_TRUE(server->register_handler(POST, "/api/rap/build", &handler));
        server->set_auth_verifier([&](HttpRequest* request, HttpHandler::RequiredPrivilege privilege)
                                         -> std::optional<EvHttpServer::AuthVerifyFailure> {
            ++auth_calls;
            required.store(static_cast<int>(privilege));
            const auto& identity = request->header("Authorization");
            if (identity == "test-allow") return std::nullopt;
            EvHttpServer::AuthVerifyFailure failure;
            failure.http_status = identity.empty() ? HttpStatus::UNAUTHORIZED : HttpStatus::FORBIDDEN;
            failure.body = "{\"denied\":true}";
            return failure;
        });
        ASSERT_OK(server->start());
        url = "http://127.0.0.1:" + std::to_string(server->get_real_port()) + "/api/rap/build";
    }

    void TearDown() override {
        if (server) {
            server->stop();
            server->join();
        }
        auto state = RapBuildGate::instance().snapshot();
        if (!state.spec.token.empty()) {
            EXPECT_TRUE(RapBuildGate::instance().fence(state.boot, state.generation, state.spec.token,
                                                      state.spec.request_sha256));
        }
        EXPECT_EQ(0, state.active_builders);
    }

    static std::string token_for(uint64_t generation) {
        std::ostringstream out;
        out << "rap_build_" << std::hex << std::setw(24) << std::setfill('0') << generation;
        return out.str();
    }

    std::string body(const std::string& op, uint64_t generation, const std::string& boot = "") const {
        return "{\"op\":\"" + op + "\",\"boot\":\"" + (boot.empty() ? before.boot : boot) +
               "\",\"generation\":" + std::to_string(generation) + ",\"token\":\"" + token +
               "\",\"request_sha256\":\"" + std::string(64, 'a') +
               "\",\"directory\":\"gs://unit-test/attempt\",\"column\":\"model\"}";
    }

    std::pair<long, std::string> request(HttpMethod method, const std::string& payload = "",
                                         const std::string& identity = "test-allow") {
        HttpClient client;
        auto init = client.init(url);
        EXPECT_TRUE(init.ok()) << init.to_string();
        if (!init.ok()) return {-1, "initialization failed"};
        client.set_method(method);
        client.set_timeout_ms(5000);
        client.set_fail_on_error(false);
        if (!identity.empty()) client.set_header("Authorization", identity);
        client.set_content_type("application/json");
        if (method == POST) client.set_payload(payload);
        std::string response;
        auto status = client.execute(&response);
        EXPECT_TRUE(status.ok()) << status.to_string();
        return {client.get_http_status(), response};
    }

    RapBuildAction handler;
    std::unique_ptr<EvHttpServer> server;
    RapBuildGate::Snapshot before;
    std::string token;
    std::string url;
    std::atomic<int> auth_calls{0};
    std::atomic<int> required{-1};
};

TEST_F(RapBuildActionTest, AuthenticationAndOperatePrivilegePrecedeMutation) {
    EXPECT_TRUE(handler.need_auth());
    EXPECT_EQ(HttpHandler::RequiredPrivilege::OPERATE, handler.required_privilege());
    EXPECT_EQ(401, request(POST, body("open", before.generation), "").first);
    EXPECT_EQ(403, request(POST, body("open", before.generation), "test-deny").first);
    EXPECT_EQ(before.generation, RapBuildGate::instance().snapshot().generation);
    EXPECT_EQ(static_cast<int>(HttpHandler::RequiredPrivilege::OPERATE), required.load());
    EXPECT_EQ(2, auth_calls.load());
    EXPECT_EQ(200, request(GET).first);
}

TEST_F(RapBuildActionTest, MalformedDuplicateAndOversizedBodiesDoNotMutate) {
    const auto valid = body("open", before.generation);
    const auto duplicate = valid.substr(0, valid.size() - 1) + ",\"token\":\"" + token + "\"}";
    for (const auto& input : {std::string("{"), std::string("[]"), duplicate, std::string(8193, ' ')}) {
        EXPECT_EQ(400, request(POST, input).first);
        EXPECT_EQ(before.generation, RapBuildGate::instance().snapshot().generation);
    }
}

TEST_F(RapBuildActionTest, LostOpenAckReconcilesAndFenceWaitsForLease) {
    const auto open = body("open", before.generation);
    ASSERT_EQ(200, request(POST, open).first); // Discard the response, as if the caller lost its acknowledgement.
    ASSERT_EQ(200, request(POST, open).first);
    EXPECT_EQ(before.generation + 1, RapBuildGate::instance().snapshot().generation);
    auto lease = RapBuildGate::instance().acquire(token);
    ASSERT_NE(nullptr, lease);
    auto response = request(POST, body("fence", before.generation + 1));
    ASSERT_EQ(200, response.first);
    rapidjson::Document parsed;
    ASSERT_FALSE(parsed.Parse(response.second.c_str()).HasParseError());
    EXPECT_TRUE(parsed["fenced"].GetBool());
    EXPECT_EQ(1, parsed["active_builders"].GetUint64());
    EXPECT_EQ(nullptr, RapBuildGate::instance().acquire(token));
    lease.reset();
    response = request(GET);
    ASSERT_FALSE(parsed.Parse(response.second.c_str()).HasParseError());
    EXPECT_EQ(0, parsed["active_builders"].GetUint64());
    EXPECT_EQ(409, request(POST, open).first);
}

TEST_F(RapBuildActionTest, StaleBootAndGenerationAreConflicts) {
    EXPECT_EQ(409, request(POST, body("open", before.generation, "wrong-boot")).first);
    EXPECT_EQ(409, request(POST, body("open", before.generation + 10)).first);
    EXPECT_EQ(before.generation, RapBuildGate::instance().snapshot().generation);
    ASSERT_EQ(200, request(POST, body("open", before.generation)).first);
    EXPECT_EQ(409, request(POST, body("fence", before.generation)).first);
    EXPECT_FALSE(RapBuildGate::instance().snapshot().fenced);
}

TEST_F(RapBuildActionTest, FenceBeforeDelayedOpenReservesClosedGeneration) {
    ASSERT_EQ(200, request(POST, body("fence", before.generation + 1)).first);
    EXPECT_EQ(409, request(POST, body("open", before.generation)).first);
    EXPECT_EQ(nullptr, RapBuildGate::instance().acquire(token));
    auto state = RapBuildGate::instance().snapshot();
    EXPECT_TRUE(state.fenced);
    EXPECT_EQ(0, state.active_builders);
    token = token_for(before.generation + 2);
    EXPECT_EQ(200, request(POST, body("open", before.generation + 1)).first);
}

} // namespace starrocks
