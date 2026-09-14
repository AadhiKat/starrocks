---
displayed_sidebar: docs
description: "StarRocks BE 配置参数完整参考：be.conf 或 SQL 命令可配置的所有 BE 参数。"
keywords: ['Canshu']
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

# BE 配置项

## 实验性 RAP 索引预加载（开发分支）

`rap_index_preload_enable` 是可动态修改的布尔配置，默认关闭。开启后，操作员可以通过 BE 的
`/api/rap/index_preload` 接口预加载索引，使用现有 HTTP OPERATE 授权机制。这是开发分支功能，
不表示任何上游发行版本已经支持。

请求必须匹配当前 `rap_index_dir` 和非空的 `rap_index_generation`，并提供不可变数据文件 URI、
文件大小、行数、字段 ID、列名和扫描使用的修改时间。加载器校验索引身份与 CRC，普通读取器仍会
根据实际 Parquet 元数据再次校验缓存条目。

每个 BE 最多按需启动两个工作线程、保留 64 条任务记录，并限制并发索引输入容量为 16 MiB，
单请求最多 8 MiB。这不是进程总内存限制；解码后的索引使用现有有界缓存，与普通读取共享 LRU。
该功能不固定缓存条目，也不新增 SSD 缓存层。

调用方必须选择目标 BE；本阶段不提供自动放置或快照触发调度。任务完成状态与当前缓存驻留状态
分别报告。取消可阻止尚未进行的缓存准入，但不能中断正在执行的文件系统调用或撤销已完成的插入。
服务关闭时等待工作线程结束，耗时取决于现有连接器 I/O 超时。

接口使用服务端文件系统凭据，而非查询凭据。只有在操作员与存储访问模型获准后才应开启。
预加载失败不修改表数据，也不改变普通读取的回退机制。预加载时间、缓存空间与完整请求延迟应
分别度量；预加载命中不能作为远程冷读结果。

<BEConfigMethod />

## 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## 配置 BE 参数

<PostBEConfig />

<StaticBEConfigNote />

## 参数组

参数分为以下几类：

- [日志](./BE_parameters/log_server_meta.md)
- [服务器](./BE_parameters/log_server_meta.md)
- [元数据与集群管理](./BE_parameters/log_server_meta.md)
- [查询引擎](./BE_parameters/query_loading.md)
- [导入导出](./BE_parameters/query_loading.md)
- [统计报告](./BE_parameters/stats_storage.md)
- [存储](./BE_parameters/stats_storage.md)
- [存算分离](./BE_parameters/shared_lake_other.md)
- [数据湖](./BE_parameters/shared_lake_other.md)
- [其他](./BE_parameters/shared_lake_other.md)
