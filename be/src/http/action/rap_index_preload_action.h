// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include "platform/http/http_handler.h"
#include "runtime/rap_index_preloader.h"

namespace starrocks {

class RapIndexPreloadAction final : public HttpHandler {
public:
    using Resident = std::function<bool(const RapIndexPreloader::Request&)>;
    RapIndexPreloadAction(RapIndexPreloader* preloader, Resident resident)
            : _preloader(preloader), _resident(std::move(resident)) {}
    void handle(HttpRequest* req) override;
    RequiredPrivilege required_privilege() const override { return RequiredPrivilege::OPERATE; }

private:
    RapIndexPreloader* _preloader;
    Resident _resident;
};

} // namespace starrocks
