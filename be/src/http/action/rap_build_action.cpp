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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <set>
#include <string>

#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "runtime/rap_build_gate.h"

namespace starrocks {

void RapBuildAction::handle(HttpRequest* req) {
    auto& gate = RapBuildGate::instance();
    bool ok = true;
    if (req->method() != HttpMethod::GET) {
        const std::string body = req->get_request_body();
        rapidjson::Document input;
        if (body.size() > 8192 || input.Parse(body.data(), body.size()).HasParseError() || !input.IsObject()) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "invalid RAP control request");
            return;
        }
        std::set<std::string> names;
        for (auto it = input.MemberBegin(); it != input.MemberEnd(); ++it) {
            if (!names.insert(std::string(it->name.GetString(), it->name.GetStringLength())).second) ok = false;
        }
        auto text = [&](const char* key) -> std::string {
            if (!input.HasMember(key) || !input[key].IsString()) return {};
            return {input[key].GetString(), input[key].GetStringLength()};
        };
        const auto op = text("op");
        const auto boot = text("boot");
        const auto token = text("token");
        const auto hash = text("request_sha256");
        auto hex = [](const std::string& s, size_t n) {
            return s.size() == n && s.find_first_not_of("0123456789abcdef") == std::string::npos;
        };
        ok = ok && token.size() == 34 && token.compare(0, 10, "rap_build_") == 0 && hex(token.substr(10), 24) &&
             hex(hash, 64) && input.HasMember("generation") && input["generation"].IsUint64();
        if (!ok) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "invalid RAP control identity");
            return;
        }
        const auto generation = input["generation"].GetUint64();
        if (op == "open") {
            const auto directory = text("directory");
            const auto column = text("column");
            if (directory.size() > 4096 || (directory.compare(0, 5, "gs://") != 0 &&
                                           directory.compare(0, 5, "s3://") != 0) ||
                directory.find_first_of("\r\n\0", 0, 3) != std::string::npos || column.empty() || column.size() > 256 ||
                column.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") !=
                        std::string::npos) {
                HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "invalid RAP build specification");
                return;
            }
            ok = gate.open(boot, generation, {token, hash, directory, column});
        } else if (op == "fence") {
            ok = gate.fence(boot, generation, token, hash);
        } else {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "unknown RAP control operation");
            return;
        }
    }
    const auto state = gate.snapshot();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("protocol");
    writer.Uint(1);
    writer.Key("ok");
    writer.Bool(ok);
    writer.Key("boot");
    writer.String(state.boot.data(), state.boot.size());
    writer.Key("generation");
    writer.Uint64(state.generation);
    writer.Key("token");
    writer.String(state.spec.token.data(), state.spec.token.size());
    writer.Key("request_sha256");
    writer.String(state.spec.request_sha256.data(), state.spec.request_sha256.size());
    writer.Key("directory");
    writer.String(state.spec.directory.data(), state.spec.directory.size());
    writer.Key("column");
    writer.String(state.spec.column.data(), state.spec.column.size());
    writer.Key("fenced");
    writer.Bool(state.fenced);
    writer.Key("active_builders");
    writer.Uint64(state.active_builders);
    writer.EndObject();
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    HttpChannel::send_reply(req, ok ? HttpStatus::OK : HttpStatus::CONFLICT, buffer.GetString());
}

} // namespace starrocks
