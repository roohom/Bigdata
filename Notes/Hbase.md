# Hbase

## 介绍

- 起源
  - 21世纪的前三驾马车
    - GFS ------------------->  HDFS
    - MapReduce--------> MapReduce
    - Bigtable-------------> Hbase

- 背景：大数据量的数据要求高性能的读写
  - 为什么不采用HDFS？
    - 基于文件的颗粒度，必须对整体文件进行操作，读写磁盘
    - 慢
  - 需要设计一款数据库工具，能进行大数据量的实时随机读写的存储
    - MySQL：小数据量，不能解决大数据量的问题
    - Redis：能满足性能要求，不能满足大数据量的内存成本要求，安全性较差
    - HDFS：能解决大数据量，不能满足实时
  - **怎么解决大数据量？**
    - 需要做分布式
  - **怎么解决高性能的读写？**
    - 基于内存存储
  - 内存的成本高，易丢失，不可能满足所有数据的存储！
    - 现象：越新的数据，被处理概率越大，越老的数据，被处理的概率相对较小
    - 解决：将新的数据存储在内存中，对于老的数据达到一定条件时将内存中的数据写入磁盘[写入HDFS]
      - 冷热数据分离
      - 老的数据在HDFS
      - 新的数据在内存
  - **数据存储在磁盘，如何保证数据安全？**
    - HDFS：基于硬盘做了备份[数据冗余机制]
    - 操作系统：做磁盘冗余阵列RAID1
    - Hbase直接基于硬盘存储，硬盘损坏会导致数据丢失，要考虑数据副本
      - Hbase底层对于文件的存储直接选用了HDFS来保证数据安全性
  - **内存的数据丢失如何解决？**
    - 操作日志WAL也就是HLog
      - Write Ahead Log：预写日志
      - 预写日志记录内存中所有数据的操作
  - 总结：实现分布式高性能读写
    - 基于分布式内存优先对数据读写
    - 所有老的数据**持久化**在HDFS
  - **如果数据在HDFS，从HDFS读，如何解决性能问题？**
    - 如何能在一个文件中快速找到一条数据？
      - 构建有序


-------------------------------
## 功能

- 是一个基于分布式内存和HDFS实现存储的随机、实时读写的NoSQL数据库
  - 实现数据的存储
  - 提供数据的读写

## 应用场景

- 电商：订单存储（超过半年的历史订单需要另外勾选查询）
  - 历史订单的存储管理以及查询
- 游戏：操作日志
  - 对大量操作日志进行实时的统计分析处理
- 金融：消费记录
  - 管理查询所有消费记录
- 电信：账单通话记录
- 交通：监控车辆信息

## 特点及概念

### 特点

- 分布式：多台机器来搭建集群实现分布式存储
- 内存：基于分布式内存，数据优先写入了机器的内存
  - 内存中的数据达到一定条件，会将内存的数据写入HDFS成为文件
- NoSQL：每个NoSQL都有自己的特点
- Hbase基于**列存储**，KV结构的数据库

### 概念

| 概念   | MySQL                                | HBase                                                        |
| ------ | ------------------------------------ | ------------------------------------------------------------ |
| 数据库 | database                             | namespace                                                    |
| 表     | table                                | namespace:table                                              |
| 行     | 主键：primary key                    | 行键：rowkey                                                 |
| 列簇   | 无                                   | column family：对列的分组                                    |
| 列     | column：每一行有多了，每一列是一致的 | column：每一行可以有多了，每一行的列可以不一样，任何一列必须属于某一个列簇，cf:colName |
| 多版本 | 无                                   | VERSIONS，一列的值可以存储多个版本                           |
| 时间戳 | 默认无，可以有                       | 默认有                                                       |

- Namespace：命名空间，就是MySQL中数据库的概念，用于区分数据存储
  - Hbase默认会自带两个namespace：default，Hbase
- Table：表，区分更细的数据的划分
  - 任何一张表必须属于某一个namespace
  - 除了default namespace下的表为，其他任何的namespace下的表在使用时都需要加namespace来访问，即`namespace:tableName`，namespace实际是表名的一部分
- Rowkey：行键，类似于MySQL中的主键
  - 功能：
    - 唯一标识一行的数据
    - 构建索引【整个HBASE只有这一个索引，不能有其他索引】
      - rowkey是HBASE的**唯一索引**
    - HBASE底层默认按照ASCII码【**字典顺序**】对Rowkey进行排序，以提高查询效率
      - 牺牲一定写的代价换取基于有序的高性能的查询
      - 决定了分区的规则
  - 是HBASE中表非常特殊的一类，每张HBASE表都自带这一列，这一列不属于任何列簇
  - 难点：Rowkey的值由开发者自行设定
    - Rowkey的值的设计决定了查询效率
  - 问题：**只有按照RowKey查询才走索引查询，其他所有查询都直接走全表扫描？**
    - 解决：
      - 将查询条件组合作为RowKey  =>  Rowkey的设计
      - 二级索引：基于一级索引之上构建一层索引
- Column Family：列簇，对列进行分组
  - 分组是为了提高性能，减少查询数据时的比较
  - 如何分组？
    - 组名自定义，可以任意，一般有标识度即可
    - 将拥有相似IO属性的列放在一组
    - 两组
      - CF1：经常被读写的列放在一组
      - CF2：不经常被读写的列
- Column：列，类似于MySQL中的列
  - HBASE中每个Rowkey，可以拥有不用的列
  - 除了Rowkey，任何一列都必须属于某一个列簇
  - 引用列`cf:colName`
- VERSIONS：多版本，HBASE中允许一列存储多个版本的值
  - **列簇级别**
    - 如果配置某个列簇的版本个数为2，那么此列簇下所有的单元都具有2个版本
  - HBASE允许存储历史版本的值，行和列相交是单元格组
  - 默认HBASE查询时，默认会返回最新的值(默认版本数为1)
  - 如何区分一列的多个版本的值？
    - 默认通过时间戳来进行区分不同版本的值
    - 每个RowKey的每一列自带时间戳，用于区分多版本
- TimeStamp：HBASE中每一个Rowkey的每一列默认自带这个值，会随着数据的更新时间而变化
  - 用于区分多版本

### 列存储

- 与其他数据库进行对比
  - MySQL：按行存储，写入读取都是行级操作
    - insert：必须指定一行每一列的值，每一行都有固定的列，如果不指定列，值为null
    - select：先对符合条件的行读取，再对列进行过滤
  - Redis：按照K V结构行存储
- Hbase：按列存储
  - 最小颗粒度：列
  - 可以为每一行构建不同的列
  - 插入：put
    - put每次只能为某一行插入一列
- **设计思想？为什么这么设计成列存储呢？**
  - 优点：直接基于列进行读写，提高查询的性能
  - 按行存储
    - 先读取所有符合条件的行，再进行对列的过滤
  - 按列存储
    - **直接读取需要的列**

## HBASE架构

- 分布式主从架构
  - Hmaster：主节点，负责管理类操作
  - HRegionServer：从节点，有多台，用于构建分布式内存
    - HBASE是一个数据库，将一条数据写入HBASE，如何实现分布式存储？
    - 分的规则：将一张表划分成多个region，不同的region分布在不同的RegionServer中
      - HBASE中分区的规则
        - 写入一条数据根据分区规则，决定写入哪个分区，写入到对应分区所在的regionServer上
      - 类似：将一个文件拆分成多个块，讲不同的块存储在不同的DN上
- HDFS：是按HBASE底层基于数据磁盘持久化的存储
  - 达到一定的条件，HRegionServer内存总存储的数据会Flush到HDFS上存储为文件
- Zookeeper
  - 辅助选举，实现高可用HA，避免Master单点故障
  - 用于存储关键性数据

## 配置

- 配置zookeeper时为什么要写三个机器的地址及端口

  - 这与zookeeper是否是分布式的无关
  - 避免由于在连接其中一台机器时，而恰好该机器宕机了，则自动会尝试连接其他机器

- 当初配置hadoop上更改了哪些文件

  - 三个env文件

  - 四个site文件

  - 一个slaves

    - 内容是集群中三台机器的地址

    - 本地优先计算

      > slaves文件里面记录的是集群里所有DataNode的主机名，到底它是怎么作用的呢？slaves文件只作用在NameNode上面，比如我在slaves里面配置了
      > host1
      > host2
      > host3
      > 三台机器，这时候如果突然间新增了一台机器，比如是host4，会发现在NN上host4也自动加入到集群里面了，HDFS的磁盘容量上来了，这下子不是出问题了？假如host4不是集群的机器，是别人的机器，然后配置的时候指向了NN，这时候NN没有做判断岂不是把数据也有可能写到host4上面？这对数据安全性影响很大。所以可以在hdfs-site.xml里面加限制。
      >
      > dfs.hosts
      > /home/hadoop-2.0.0-cdh4.5.0/etc/hadoop/slaves
      > 这相当于是一份对于DN的白名单，只有在白名单里面的主机才能被NN识别。配置了这个之后，就能排除其他DN了。slaves中的内容可以是主机名也可以是IP地址。

- hbase.rootdir：用于指定HBASE的数据文件存储在hdfs的什么位置

  - 必须是完整的hdfs路径，包含头部

  - 如果HDFS做了HA

    - namenode

      ~~~
      hdfs://mycluster
      ~~~

    - **HBASE如何知道谁是Active谁是Namenode？**

      - 将hdfs-site.xml和core-site.xml放入HBASE的conf目录下

- 启动与关闭

  - 先启动HDFS和Zookeeper

    - HDFS：等待HDFS退出安全模式再启动Hbase

      ~~~shell
      start-dfs.sh
      ~~~

    - Zookeeper

      ~~~shell
      /export/servers/zookeeper-3.4.6/bin/start-zk-all.sh
      ~~~

    - 启动Hbase

      ~~~
      start-hbase.sh
      ~~~

    - 关闭hbase

      ~~~
      stop-hbase.sh
      ~~~

      

## 客户端操作

- HBASE Shell

  - 直接使用hbase shell启动

    ~~~shell
    hbase shell
    ~~~

### DDL操作

- 查看命令方法：`Help 'command'`

- namespace

  - 列举：`list_namespace`

  - 创建：`create_namespace`

    ~~~shell
    create_namespace 'ns1'，{'PROPERTY_NAME'=>'PROPERTY_VALUE'}
    ~~~

  - 删除：`drop_namespace`

- table

  - 列举：`list`

    - 只能列举用户表，系统表不能被列出

  - 创建：`create`

    - ns：表示namespace

    - t1：表示表的名称

    - f1：表示列簇的名称

    - 语法：创建表的时候至少给定表名和一个列簇

      ~~~
      create 't1','f1','f2','f3'
      create 't1',{NAME=>'f1',VERSIONS=>1,TTL=》2592000,BLOCKCACHE=true}
      ~~~

      

  - 删除：drop

    - 直接删除表会报错：`Table xxx is enabled.Disable it first.`
    - 所有的表的结构删除或者修改之前，要先确认这张表没有对外提供服务，是一个**禁用**状态
      - 如果删除，要先禁用`disable`
      - 如果修改，要先禁用，后修改，再启用`enable`

  - 查看：desc

    ~~~
    desc 'student:stu_info'
    ~~~



### DML操作

- put：用于**插入/更新**数据

  ~~~shell
  put 'ns1:t1','r1','f1:c1','value',[ts1]
  ~~~

  - 参数含义
    - ns1：表示namespace
    - t1：表示表名
    - r1：表示rowkey
    - f1：列簇的名称
    - c1：表示列的名称
    - value：这一列的值
    - ts1：时间戳

- get：用于**读取**数据(一条rowkey数据)，必须指定rowkey

  - 是HBASE中最快的读取数据的方式（使用rowkey索引）

- scan：用于**扫描**数据

  - 用法一：全表扫描

    ~~~
    scan 'student:stu_info'
    ~~~

  - 用法二：scan+过滤器

    - 工作中最常用的方式，可以根据查询条件返回所有符合条件的数据

    - 范围过滤[左闭右开）

      - **RowKey是前缀匹配的**
      - STARTROW：从哪一条rowkey开始
      - STOPROW：结束于哪一条rowkey

      ~~~
      scan 'student:stu_info',{STARTROW=>'20200920_001'}
      ~~~

      

- delete：用于**删除**数据

  - 如果不加版本默认删除最新版本
  - deleteall：删除所有版本

## 存储设计

### 存储概念

- 分布式存储
  - 分布式内存：RegionServer
  - 分布式磁盘：HDFS
  
- 如何实现的：将HBASE中的表构建成分布式的表
  - HBASE中的每张表可以对应多个分区[Region]
    - 默认创建只有一个分区（Region）
  
- 与HDFS的区别

  | 概念     | HDFS             | HBase        |
  | -------- | ---------------- | ------------ |
  | 分类     | 目录             | NameSpace    |
  | 存储类型 | 文件             | 表           |
  | 分的机制 | 分块：Block      | 分区：Region |
  | 存储节点 | DataNode         | RegionServer |
  | 规则     | 大表：128M一个块 | RowKey范围   |

  

- Region：HBASE中表的分区，一张HBASE表可以有多个region，每个region存储在不同的RegionServer中
  - **是HBASE做负载均衡的最小单元**
  - 类似于HDFS中的文件的块
  - 一个Region只会归某一个RegionServer所管理
  - 一个RegionServer可以管理多个region
  - **如何决定数据会写入一张表的哪一个Region中?分区规则是什么？**
    - 分区规则：
      - 整个HBASE中的所有数据都是按照**字典顺序【ASCII码的前缀诸位比较**】进行排序的，所有数据存储时每个分区都有一个范围
        - startKey
        - endKey
      - 规则：按照rowkey所属的范围来决定写入哪个分区
      - Situation1：**默认**创建的表只有1个分区Region
        - region0：负无穷~正无穷
      - Situation2：创建表的时候指定分区的划分
        - region0：-oo~100
        - region1：100~200
        - region2：200~300
        - ……
        - region9：900~+oo
  - Store：列族，按照列族划分不同的Store，这个表有几个列族，region中就有几个Store【**一个Store代表一个列族**】
    - 设计目的：将不同的列区分存储，就是列族的划分
    - 一个Region里有多个Store
    - MemStore：内存区域
      - 每个Store都有一个
      - 数据先写入MemStore
    - StoreFile：HFILE，物理存储在HDFS上的文件
      - 每个Store中有0或者多个StoreFile文件
      - 达到一定条件之后，Memstore中的数据会被Flush刷写到HDFS变成StoreFile文件

### 存储模型

- 假设执行`put 'ns1:t1','r1','f1:c1','value',[ts1]`
  - 步骤
    - :one:根据表名请求元数据找到对应的所有Region信息
    - :two:根据RowKey决定存储到哪个region中
    - :three:将写入请求提交给这个region所在的regionserver中
    - :four:根据列族进行判断，决定写入哪个Store中(也会写入memstore，当达到一定条件时，memstore中的数据会被刷写到HDFS变成storefile文件)



### 存储流程

#### 写入

- Step1：根据表名找到这张表对应的所有Region信息
  - **问：怎么能得到表所对应的所有的Region信息？**
    - 通过元数据来获取
    - HBase自带两张表
      - hbase：meta：记录hbase中所有用户表的元数据信息
        - 两种RowKey
          - 以表明作为rowkey
          - 以region名作为rowkey
      - hbase：namespace：记录了当前hbase中所有namespace的信息
    - 通过put语句中的表名对meta表进行**前缀匹配**，就能得到这张表所有的region信息2
  - **问：如何能知道Meta表所对应的region位置？**
    - meta表所对应的region信息都记录在zookeeper中
  - HBASE中所有的客户端都要先连接zookeeper
  
- Step2：根据Rowkey以及表的region起始范围进行比较，得到要写入的region
- Step3：将写入请求提交给这个region所在的regionServer
  - **问：如何能知道这个region所在的regionserver是哪个？**
    - 通过元数据来获取这个region所对应的regionserver的地址
- Step4：regionserver将输入写入对应的region，根据列族判断写入哪个Store
- Step5：先写WAL(HLog)，然后将数据写入MemStore
- Step6：写入流程结束，返回客户端
  - Flush：当menstore中的数据达到一定条件，会触发将内存中的数据刷写如HDFS变成Sorefile文件
  - Compact：将多个storefile文件进行合并成大文件
    - Hbase没有删除和更新，删除和更新都是插入一条数据
    - 老的数据被标记为更新状态或者是删除状态
    - 这个阶段会真正从物理上删除被标记的数据
  - Split：如果一个region存储的数据到达一定阈值，一个region会被等分为两个region
    - 分摊单个region存储数据过多，负载过高
    - 分由regionserver来分，两个region的去向由Master来分配

#### 读取

- step1：根据表名从元数据获取对应的region信息
- step2：
  - 有rowkey
  - 无rowkey
- step3：根据列族来读取对应Store的数据
- step4：读
  - 先读memstore
  - 如果读memstore【写缓存】没有，就去读BlockCache【读缓存】
  - 最后读StoreFile
    - 第一次读取：如果memsotre没有就迫不得已地去读StoreFile
    - 以后再去读，就会将读到的数据先写入BlockCache(默认开启)，避免二次读浪费时间
    - 缓存释放策略：LRU算法(最近最少被使用)

## 角色功能

### HMaster

- 主要负责集群管理
  - 节点管理：regionserver的状态管理
    - 故障转移
  - 元数据管理：用于接收所有DDL操作请求
    - 管理meta表以及namespace表的数据
    - 与zookeeper连接，将一些管理类的元数据存储在zookeeper中
  - region管理：负责管理每个region的分配
    - 故障恢复
    - split阶段的分配

### HRegionServer

- 接收客户端所有region的读写请求
- 管理region存储数据：分割
- 维护：
  - WAL
  - MemCache
  - BlockCache
  - 将内存的数据Flush成为StoreFile文件

### Zookeeper

- 构建HA：辅助选举
- 存储关键性的管理类元数据



### HDFS

- 持久化的实现

## HBASE Java API

> 注意：**所有的HBASE客户端所连接的服务端都是Zookeeper**
>
> `Conf.set("hbase.zookeeper.quorum")`，`"node1:2181,node2:2181,node3:2181"`
>
> Get操作时一个Result对象就代表一个RowKey数据对象





## Hbase与MapReduce的集成

### 应用场景

- HBASE：分布式存储
- MapReduce、Spark：分布式计算
- 大数据的本质：一系列大数据的处理软件工具对大量数据进行分析处理
  - 存储：HBASE
  - 计算：MapReduce

### 集成原理

- MapReduce五大阶段
  - Input
    - TableInputFormat
  - Map
  - Shuffle
  - Reduce
  - Outptut
    - TableOutputFormat

### 读HBASE数据

- 规则
  - Driver类
    - 读HBASE提供的封装后的方法
    - 

----------