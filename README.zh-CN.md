# DataLink

[![License](https://img.shields.io/badge/License-Apache%202-4EB1BA.svg?style=socialflat-square)](LICENSE)
[![EN doc](https://img.shields.io/badge/Document-English-blue.svg?style=socialflat-square)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文-blue.svg?style=socialflat-square)](README.zh-CN.md)

[![Stargazers over time](https://starchart.cc/jinsyin/datalink.svg)](https://starchart.cc/jinsyin/datalink)

DataLink 是一个建立在 DataX（开发中）、Spark、Flink 之上的轻量化数据集成框架，它提供了一套全场景的数据集成解决方案，
满足大、小规模数据的实时、离线、全量、增量集成场景。DataLink 内部定义了一个统一的 **Pipeline** 抽象，
用户可以自由地将其转换为 DataX、Spark SQL、Flink SQL 或 Flink CDC，而不需要任何的外部依赖。

## 📚 概念

| 名称对象       | 说明                                                                                                                                                                     |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Node`     | 数据处理节点，分为 ExtractNode（ScanExtractNode/CdcExtractNode）、TransformNode 和 LoadNode 三种节点，<br/> 分别用于数据的抽取（E）、转换（T）和加载（L），而 ScanExtractNode 和 CdcExtractNode 分别用于批量集成和 CDC 集成 |
| `Pipeline` | 数据处理管道，用于定义一个完整的数据抽取、转换和加载流程                                                                                                                                           | 

## ✨ 功能

* 支持转换为 Spark SQL
* 支持转换为 Flink SQL
* 适配官方支持的连接器/数据源

## 🛠 使用

```xml
<dependency>
    <groupId>io.github.jinsyin</groupId>
    <artifactId>datalink</artifactId>
    <version>0.0.2</version>
</dependency>
```

## 🚀 支持

DataLink 目前已为各个引擎适配了多种数据源，理论上官方引擎支持的数据源都可以适配（敬请期待）。

| Type       | Spark Extract | Spark Load | Flink Scan Extract | Flink CDC Extract | Flink Load |
|------------|---------------|------------|--------------------|-------------------|------------|
| MySQL      | √             |            | √                  | √                 |            |
| Oracle     | √             |            | √                  | √                 |            |
| Dameng     | √             |            | √                  |                   |            |
| PostgreSQL | √             |            | √                  |                   |            |
| Greenplum  | √             |            | √                  |                   |            |
| Kafka      |               |            | √                  | √                 |            |
| Amoro      |               | √          |                    |                   | √          |
