# Yarn

## 功能

- 本质：将多台机器的CPU和内存从逻辑上构建一个整体对外提供运行环境的统一服务
- 功能：将所有的分布式程序，利用yarn的分布式资源进行分布式运行

## 应用场景

- 适合于运行所有分布式的程序
  - 根据分布式程序中任务划分的规则，将每个小任务运行是在分布式容器的不同机器上

## 架构

- 分布式主从架构
  - 主：ResourceManager
  - 从：NodeManager