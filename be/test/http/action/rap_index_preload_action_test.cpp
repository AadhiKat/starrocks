// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "http/action/rap_index_preload_action.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <atomic>
#include <optional>

#include "base/testutil/assert.h"
#include "platform/http/ev_http_server.h"
#include "platform/http/http_client.h"
#include "platform/http/http_request.h"

namespace starrocks {

class RapIndexPreloadActionTest : public testing::Test {
protected:
    std::atomic<int> loads{0};
    std::atomic<bool> enabled{true};
    RapIndexPreloader pool{[&](const auto&) {
        ++loads;
        return RapIndexPreloader::Prepared{RapIndexPreloader::Result{"LOADED"}, {}};
    }, [&](const auto&) { return enabled.load(); }};
    RapIndexPreloadAction handler{&pool, [&](const auto&) { return loads > 0; }};
    EvHttpServer server{"127.0.0.1", 0, 1};
    std::string url;
    const std::string id = std::string(32, 'a');

    void SetUp() override {
        ASSERT_TRUE(server.register_handler(GET, "/api/rap/index_preload", &handler));
        ASSERT_TRUE(server.register_handler(POST, "/api/rap/index_preload", &handler));
        server.set_auth_verifier([](HttpRequest* request, HttpHandler::RequiredPrivilege privilege)
                                        -> std::optional<EvHttpServer::AuthVerifyFailure> {
            EXPECT_EQ(HttpHandler::RequiredPrivilege::OPERATE, privilege);
            if (request->header("Authorization") == "test-operator") return std::nullopt;
            EvHttpServer::AuthVerifyFailure error;
            error.http_status = HttpStatus::UNAUTHORIZED;
            error.body = "denied";
            return error;
        });
        ASSERT_OK(server.start());
        url = "http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/api/rap/index_preload";
    }
    void TearDown() override { server.stop(); server.join(); pool.shutdown(); }
    std::string body() const {
        return "{\"op\":\"submit\",\"id\":\"" + id +
               "\",\"file_uri\":\"gs://bucket/table/data/a.parquet\",\"directory\":\"gs://bucket/indices\","
               "\"generation\":\"g1\",\"column\":\"model\",\"file_size\":100,\"file_rows\":3,"
               "\"modification_time\":0,\"field_id\":3,\"max_bytes\":1024}";
    }
    std::pair<long, std::string> call(HttpMethod method, const std::string& body = "",
                                     const std::string& identity = "test-operator", const std::string& suffix = "") {
        HttpClient client;
        EXPECT_OK(client.init(url + suffix));
        client.set_method(method);
        client.set_timeout_ms(5000);
        client.set_fail_on_error(false);
        if (!identity.empty()) client.set_header("Authorization", identity);
        if (method == POST) { client.set_content_type("application/json"); client.set_payload(body); }
        std::string response;
        EXPECT_OK(client.execute(&response));
        return {client.get_http_status(), response};
    }
};

TEST_F(RapIndexPreloadActionTest, AuthorizationAndDisabledAdmission) {
    EXPECT_TRUE(handler.need_auth());
    EXPECT_EQ(401, call(POST, body(), "").first);
    EXPECT_EQ(0, pool.counters().submitted);
    enabled = false;
    EXPECT_EQ(503, call(POST, body()).first);
    EXPECT_EQ(0, loads);
}

TEST_F(RapIndexPreloadActionTest, MalformedDuplicateAndOversizedRequests) {
    const auto valid = body();
    const auto duplicate = valid.substr(0, valid.size() - 1) + ",\"id\":\"" + id + "\"}";
    for (const auto& invalid : {std::string("{"), std::string("[]"), duplicate, std::string(8193, ' ')}) {
        EXPECT_EQ(400, call(POST, invalid).first);
    }
    EXPECT_EQ(0, pool.counters().submitted);
}

TEST_F(RapIndexPreloadActionTest, IdempotentSubmissionAndStatus) {
    ASSERT_EQ(200, call(POST, body()).first);
    ASSERT_EQ(200, call(POST, body()).first);
    auto status = call(GET, "", "test-operator", "?id=" + id);
    ASSERT_EQ(200, status.first);
    rapidjson::Document parsed;
    ASSERT_FALSE(parsed.Parse(status.second.c_str()).HasParseError());
    EXPECT_EQ(1, parsed["protocol"].GetUint64());
    EXPECT_EQ(id, parsed["id"].GetString());
    EXPECT_EQ(1, pool.counters().submitted);
    auto conflict = body();
    conflict.replace(conflict.find("g1"), 2, "g2");
    EXPECT_EQ(409, call(POST, conflict).first);
    EXPECT_EQ(404, call(GET, "", "test-operator", "?id=" + std::string(32, 'b')).first);
}

TEST_F(RapIndexPreloadActionTest, UnsafePathsAndByteBudgetsRefused) {
    auto traversal = body();
    traversal.replace(traversal.find("data/a.parquet"), 14, "../a.parquet");
    EXPECT_EQ(400, call(POST, traversal).first);
    auto too_big = body();
    too_big.replace(too_big.find("1024"), 4, "8388609");
    EXPECT_EQ(400, call(POST, too_big).first);
    EXPECT_EQ(0, pool.counters().submitted);
}

} // namespace starrocks
