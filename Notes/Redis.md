# Redis

## 介绍

- 本质：分布式的**基于内存**的NoSQL**数据库**
  - 数据库：用于存储数据的
  - 分布式：解决了高并发和存储能力的问题
  - 特点：
    - 基于内存
    - 所有的数据都会存储在内存中，所有的读写都直接操作内存
      - 问题
        - 内存：小、易丢失
      - 解决
        - 小：集群分布式构成**分布式内存**
        - 易丢失：虽然数据都在内存中，但磁盘中保留一份数据，每次重新启动都会从磁盘中将数据加载到内存
      - 问题：写入磁盘又需要提供高性能的读写，如何实现？
        - 数据安全和性能必须二选一
        - 保证数据安全的情况的前提下提供最好的性能
- 功能：基于数据库设计，实现数据存储
- 实现：基于内存的数据存储
- 数据库分类：
  - RDBMS：关系型数据库管理系统
    - MySQL、Oracle、SQLServer
    - 特点：
      - 一般都支持SQL语句
      - 允许数据之间的关联
      - 存储容量较小，存储数据量如果较大性能就会下降
    - 区别：性能的区别
  - NoSQL(Not Only SQL)：非关系型数据库
    - Redis、Hbase、MongoDB
    - 特点：
      - 每种NoSQL的特点都不一样
      - 为了追求小数据量高性能读写如Redis
      - 为了解决大数据量的高性能读写如HBase

## 应用场景

- 传统的web开发
  - 用于实现读缓存
    - 传统网站架构
      - 同时大量的并发读写请求MySQL，而MySQL无法响应支持这种高并发的场景，导致请求失败
  - 引入Redis
    - 实现读写分离，写入MySQL，将大量的高并发的**读**请求提交给Redis来实现
- 大数据的应用场景
  - 适合于高并发的场景
  - 适合于读写性能要求非常高的场景

## 特点

- 分布式
- 基于内存
- 基于C语言开发，对内存的管理、编译、数据的存储更加的高效
- 支持高并发：并发量：单台机器10w/s
- 不能代替MySQL
  - MySQL：支持复杂的业务，以及复杂数据存储，支持SQL，更加稳定
  - Redis：高并发高性能，存在数据丢失的隐患，存储结构比较单一，不能满足业务存储

## Windows上使用Redis

- 架构
  - Standalone：单节点

- 使用
  - 启动服务端
    - Redis-server.exe
  - 启动客户端
    - Redis-cli.exe

- 数据结构
  - KV结构：
    - 整个Redis中所有的数据都是以KV进行读写的
    - 通过K来读写Value
  - 数据类型
    - K：Redis中每条数据即每个KV的K都是String类型的(**永远都是String类型**)
    - V：Redis的V有五种类型结构
      - String：字符串类型
      - Hash：类似于Java中的Map集合
        -  Java：
          - Map1（K：string， map2：HashMap）
      - List：集合类型，有序可重复集合
      - Set：集合类型，无序不可重复
      - Zset：集合类型，有序不可重复集合
        - 类似于Java中的TreeMap

## 数据类型和语法

### String

### Hash

- 语法

  - 单个属性写入：hset

    ~~~
    hset	K	V[]
    ~~~

  - 单个属性读取：hget

    ~~~
    hget	K	v1
    ~~~

  - 批量添加：hmset

    ~~~
    hmset 	K 	V[k1  v1  K2  v2 ……]
    ~~~

  - 批量化读取：hmget

    ~~~
    hmget K K1 K2 K3
    ~~~

  - 获取所有的属性：hgetall

    ~~~
    hgetall K
    ~~~

  - 删除Hash中的某个元素：hdel

    ~~~
    hdel  K  K1
    ~~~

### List

- 应用：有序可重复的集合的数据

  - 类似于Java中的List
  - 用于存放一系列有序变化的数据

- 使用

  - 插入

    - 左序插入：lpush

      ~~~shell
      lpush K V[e1 e2 e3 e4]
      ~~~

      ~~~shell
      #实际存放的是
      K  e4 e3 e2 e1
      ~~~

      

    - 右序插入：rpush

      ~~~
      rpush K V[e1 e2 e3 e4]
      ~~~

      ~~~shell
      #实际存放的是
      K  e1 e2 e3 e4
      ~~~

      

  - 读取：lrange

    ~~~
    lrange K start end
    ~~~

    ~~~shell
    #左序查询：起始位置为0
    lrange list1  0  1
    #右序查询
    
    ~~~

  - 长度：llen

    ~~~shell
    llen K
    ~~~

  - 删除元素

    - 左边删除：lpop

      ~~~shell
      lpop K
      ~~~

    - 右边删除：rpop

### Set

- 应用：无序不可重复的集合，用于去重统计等等

  - 类似于Java中的set集合

- 使用：

  - 插入数据：sadd

    ~~~
    sadd  K  V[e1 e2 e3...]
    ~~~

  - 查询数据：smembers

    ~~~
    smembers K
    ~~~

  - 元素判断：sismember

    - 判断当前set元素是否是该set的成员

      ~~~
      sismembers  K  e
      ~~~

  - 元素的移除：srem

    ~~~
    srem  K  e
    ~~~

### Zset

- 应用：有序不可重复的集合，一般用于排序取TopN

- 使用

  - 添加元素：zadd

    ~~~
    zadd  	K   [score1 V1 score2 V2 score3 V3]
    ~~~

  - 查询：zrange

    - 默认升序

    ~~~
    zrange  K   start  end    [withscores]
    ~~~

    > 在使用Redis时，不建议存储double类型的score，因为其在底层会有精度为题
    >
    > 如果需要存储double类型，将其转换为int类型
    >
    > 写:20.01 x 100 = 2001
    >
    > 读:2001 / 100 = 20.01

  - 倒叙查询：zrevrange

    ~~~
    zrevrange  K  start  end [withscores]
    ~~~

  - 取集合长度：zcard

    ~~~
    zcard K
    ~~~

  - 移除元素：zrem

    ~~~
    zrem   K   Vkey
    ~~~

    

### 通用命令

- `key *` ：列举当前数据库中的所有KV
- `del`：用于删除当前数据库中的某个KV
  - `del name`
- `exists`：用于判断当前数据库中是否存在某个key
  - `exists K`
- type：用于查看某个K的类型
  - `type K`
- select ：切换数据库

- move：用于实现数据库之间key的移动

  ~~~shell
  #切换进1数据库
  select 1
  #将数据库1中的s2移动至0数据库下
  move s2 0
  ~~~

  