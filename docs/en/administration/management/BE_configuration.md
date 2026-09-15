---
displayed_sidebar: docs
description: "StarRocks BE configuration reference: complete list of BE parameters configurable in be.conf or via SQL."
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE Configuration

<BEConfigMethod />

<CNConfigMethod />

## View BE configuration items

You can view the BE configuration items using the following command:

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## Configure BE parameters

<PostBEConfig />

<StaticBEConfigNote />

## Parameter groups

### Experimental RAP index preloading (development fork)

`rap_index_preload_enable` is a mutable Boolean, disabled by default. It enables operator-directed
preloading through the BE's `/api/rap/index_preload` endpoint using the normal HTTP OPERATE authorization.
This is an experimental fork feature, not an upstream release availability statement.

Requests must match the currently configured `rap_index_dir` and a nonempty `rap_index_generation`.
The caller supplies the immutable data-file URI, size, row count, field ID, column and effective scan
modification time. The loader checks sidecar identity and CRC before cache admission. Ordinary readers
validate the cached entry against the actual Parquet metadata again.

Each BE lazily starts at most two workers, retains at most 64 job records and reserves at most 16 MiB
of concurrent sidecar input capacity, with an 8 MiB per-request ceiling. These input limits are not
total process-memory limits: decoded indexes use the existing bounded parsed-index cache and compete
with demand entries. Preloading does not pin entries or implement an SSD tier.

The caller must select target BEs; automatic placement and snapshot-triggered scheduling are not included.
Status reports historical completion separately from current cache residency. Cancellation prevents
pending cache admission but cannot interrupt an active filesystem operation or undo a completed insertion.
The service joins its workers during shutdown, subject to existing connector I/O timeouts.

This endpoint uses server filesystem credentials, not credentials supplied by a query. Keep it disabled
unless the approved operator and storage-access model are suitable. A preload failure does not modify
table data or the ordinary demand-read fallback. Measure preload time and cache footprint separately
from complete-request latency; a preloaded hit is not a cold remote read.

The parameters are grouped in these categories:

- [Logging](./BE_parameters/log_server_meta.md)
- [Server](./BE_parameters/log_server_meta.md)
- [Metadata and Cluster management](./BE_parameters/log_server_meta.md)
- [Query engine](./BE_parameters/query_loading.md)
- [Loading and unloading](./BE_parameters/query_loading.md)
- [Statistic report](./BE_parameters/stats_storage.md)
- [Storage](./BE_parameters/stats_storage.md)
- [Shared-data](./BE_parameters/shared_lake_other.md)
- [Data Lake](./BE_parameters/shared_lake_other.md)
- [Other](./BE_parameters/shared_lake_other.md)
