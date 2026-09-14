// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "http/action/rap_index_preload_action.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <set>
#include <limits>

#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

namespace starrocks {

void RapIndexPreloadAction::handle(HttpRequest* req) {
    auto fail = [&](HttpStatus code) { HttpChannel::send_reply(req, code, "invalid or unavailable RAP preload"); };
    std::string id = req->param("id");
    std::string admission;
    auto valid_id = [](const std::string& s) {
        return s.size() == 32 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
    };
    if (req->method() == HttpMethod::POST) {
        const auto body = req->get_request_body();
        rapidjson::Document d;
        if (body.size() > 8192 || d.Parse(body.data(), body.size()).HasParseError() || !d.IsObject()) {
            fail(HttpStatus::BAD_REQUEST); return;
        }
        std::set<std::string> fields;
        for (auto it = d.MemberBegin(); it != d.MemberEnd(); ++it) {
            if (!fields.emplace(it->name.GetString(), it->name.GetStringLength()).second) {
                fail(HttpStatus::BAD_REQUEST); return;
            }
        }
        auto text = [&](const char* k) -> std::string {
            return d.HasMember(k) && d[k].IsString() ? std::string(d[k].GetString(), d[k].GetStringLength()) : "";
        };
        id = text("id");
        if (!valid_id(id)) { fail(HttpStatus::BAD_REQUEST); return; }
        const auto op = text("op");
        if (op == "cancel") {
            if (!_preloader->cancel(id)) { fail(HttpStatus::NOT_FOUND); return; }
        } else if (op == "submit") {
            RapIndexPreloader::Request r;
            r.id = id;
            r.file_uri = text("file_uri");
            r.directory = text("directory");
            r.generation = text("generation");
            r.column = text("column");
            auto remote = [](const std::string& s) {
                return s.size() > 5 && s.size() <= 4096 &&
                       (s.compare(0, 5, "gs://") == 0 || s.compare(0, 5, "s3://") == 0) &&
                       s.find_first_of("\r\n\0", 0, 3) == std::string::npos &&
                       s.find("/../") == std::string::npos && s.find("/./") == std::string::npos &&
                       s.compare(s.size() - 3, 3, "/..") != 0 && s.compare(s.size() - 2, 2, "/.") != 0 &&
                       s.back() != '/';
            };
            auto uint = [&](const char* k) { return d.HasMember(k) && d[k].IsUint64(); };
            if (!remote(r.file_uri) || !remote(r.directory) || r.column.empty() || r.column.size() > 256 ||
                r.column.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != std::string::npos ||
                r.generation.empty() || r.generation.size() > 256 || r.generation.find('\0') != std::string::npos ||
                !uint("file_size") || !uint("file_rows") || !uint("max_bytes") ||
                !d.HasMember("modification_time") || !d["modification_time"].IsInt64() ||
                !d.HasMember("field_id") || !d["field_id"].IsInt()) {
                fail(HttpStatus::BAD_REQUEST); return;
            }
            r.file_size = d["file_size"].GetUint64();
            r.file_rows = d["file_rows"].GetUint64();
            r.max_bytes = d["max_bytes"].GetUint64();
            r.modification_time = d["modification_time"].GetInt64();
            r.field_id = d["field_id"].GetInt();
            if (!r.file_size || !r.file_rows || r.field_id < 0 || r.modification_time < 0 ||
                r.file_size > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
                r.file_rows > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
                !r.max_bytes || r.max_bytes > 8 * 1024 * 1024) {
                fail(HttpStatus::BAD_REQUEST); return;
            }
            auto result = _preloader->submit(r);
            admission = result.state;
            id = result.id;
            if (admission == "CONFLICT") { fail(HttpStatus::CONFLICT); return; }
            if (admission == "FULL" || admission == "UNAVAILABLE" || admission == "REFUSED") {
                fail(HttpStatus::SERVICE_UNAVAILABLE); return;
            }
        } else { fail(HttpStatus::BAD_REQUEST); return; }
    }
    if (!valid_id(id)) { fail(HttpStatus::BAD_REQUEST); return; }
    RapIndexPreloader::Snapshot s;
    if (!_preloader->status(id, &s)) { fail(HttpStatus::NOT_FOUND); return; }
    const auto counts = _preloader->counters();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> w(buffer);
    w.StartObject();
    auto text = [&](const char* k, const std::string& v) {
        w.Key(k); w.String(v.data(), static_cast<rapidjson::SizeType>(v.size()));
    };
    auto number = [&](const char* k, uint64_t v) { w.Key(k); w.Uint64(v); };
    number("protocol", 1);
    text("id", id);
    text("admission", admission);
    text("state", s.result.state);
    text("generation", s.request.generation);
    w.Key("terminal"); w.Bool(s.terminal);
    w.Key("resident_now"); w.Bool(_resident(s.request));
    number("elapsed_ns", s.elapsed_ns);
    number("bytes", s.result.bytes);
    number("attempts", s.result.attempts);
    number("exists_ns", s.result.exists_ns);
    number("open_ns", s.result.open_ns);
    number("size_ns", s.result.size_ns);
    number("read_ns", s.result.read_ns);
    number("parse_ns", s.result.parse_ns);
    number("admit_ns", s.result.admit_ns);
    number("active", counts.active);
    number("reserved_bytes", counts.reserved_bytes);
    number("submitted", counts.submitted);
    number("deduplicated", counts.deduplicated);
    number("rejected", counts.rejected);
    number("completed", counts.completed);
    w.EndObject();
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    HttpChannel::send_reply(req, HttpStatus::OK, buffer.GetString());
}

} // namespace starrocks
