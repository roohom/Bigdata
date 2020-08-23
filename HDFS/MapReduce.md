# 脑裂问题

## 形成

- 如果Active的ZKFC故障，导致ZK中的临时节点file1被删除
- Standby的ZKFC发现file1被删除，认为active的NN1故障了，于是standby的ZKFC创建了file1
- 并且NN2转换为active状态，其实NN1也是active

## 解决

- 让ZKFC1和ZKFC2同时创建一个一个临时节点file1，谁创建成功就是active，否则是standby
  - 假设zkfc1是active，zkfc2为standby
  - zkfc1除了创建临时节点file1之外创建成功之后还会创建永久节点file2

- 正常情况下：
  - zkfc1发现NN1故障，zkfc1会主动地断开与zookeeper的链接，并且会自动删除file2
  - 这是file1和file2都被删除了，zkfc2发现file1没了，重新创建，NN2成为active
- 特殊情况下：
  - zkfc1故障，但是NN1还是active，file1消失了，但是file2仍然存在
  - zkfc2发现file1消失了，于是去创建file1，并且还需要创建file2，但是发现file2还在
    - 说明NN1没有故障，还是active
    - 但是NN2也已经成为active，ZKFC2会通过隔离机制杀死NN1



# MapReduce

## 编程模型

### 功能

- 用于开发分布式程序，本身是一套已经开发封装好的分布式编程的API
- 根据用户定义的数据处理逻辑，自动将这个逻辑拆分成多个小的任务逻辑
- 在分布式运行环境中，会将每个小的任务在不同的计算节点上运行

### 思想

- 分而治之
- 讲一个大的处理逻辑，拆分成多个小的处理逻辑

### 应用场景

- 用于开发离线的分布式程序，对时效性要求不高的场景

- 时效性：分钟级别

### 阶段

- 正常的数据处理的阶段划分：
  - 读取数据：将数据读取到程序
  - 处理数据：将读取到的数据进行处理，得到结果
  - 输出结果：将结果进行保存
- **MapReduce的五大阶段**
  - Input：负责整个分布式程序的数据的输入
    - 默认输入类：TextInputFormat
    - 方法：setInputPath(path)
    - 默认读取的Path：由fs.defaultFS决定
      - 集群中默认读取HDFS
  - **Map**：**分**的过程，启动多个Map Task进程来处理每个Task
    - 处理逻辑：由Map方法决定
      - map方法会将input读取到的数据进行处理
      - 至于如何处理，这由自己定义的代码逻辑决定
  - **Shuffle**：默认提供的功能有分组和排序
    - 对Map阶段输出的数据进行分组和排序
  - **Reduce**：**合**的过程，默认启动一个Reduce Task进程将上面的结果进行合并
    - 处理逻辑：由reduce方法决定
      - reduce方法会将上一步的数据进行聚合处理
      - 至于如何聚合，这由自己的定义的代码逻辑决定
  - Output：整个分布式程序结果的输出
    - 默认输入类：TextOutputFormat
    - 方法：setOutputPath(path)
    - 默认写入的Path：由fs.defaultFS决定
      - 集群中默认写入HDFS





## 编程规则

### Driver类

- 包含main方法的类，用于运行程序

  - 作为程序的入口
  - 负责调用run方法

- 官方推荐规则：让此类继承Configured，实现Tool接口

  ~~~java
  public class Driver extends Configured implements Tool
  ~~~

  - 重写方法：run方法
    - 用于构建、配置、提交MapReduce的程序

### Input类

- MapReduce提供了多种输入的类，主要负责读取数据到MapReduce程序中
- **默认**输入类：TextInputFormat
  - 用于读取文件
  - 将文件的每一行准换为一个KV结构
  - K：行的偏移量
  - V：行的内容

### Mapper类

- Map阶段中每个MapTask需要实例化一个Mapper类
- 将自己负责处理的数据调用Mapper类中的map方法来进行处理
  - 每条KV调用一次map方法
- 规则：继承mapper类
  - 重写map方法

### Reducer类

- Reduce阶段中每个ReduceTask需要实例化一个Reduce类
- 将自己负责处理的每一组数据调用Reduce类中reduce方法来进行处理
- 规则：继承Reduce类
  - 重写reduce方法

### Output类

- 默认输出类：TextOutputFormat



### 数据结构

整个MapReduce中所有的数据都以KV形式存在

### 数据类型

不能直接使用java类型，使用Hadoop封装好的序列化类型

~~~java
int 	intWritable
String  Text
double  DoubleWritable    
null 	NullWritable
...    
~~~

- 为什么要这样封装？
  - Hadoop对java中的类型进行了封装，实现了序列化
  - 序列化：
    - 两台机器之间需要传输数据
    - 传输数据：直接发送即可
    - 传输对象：必须经过序列化
      - 序列化：把对象封装成数据进行传输
      - 反序列化：把数据解析成对象



## 编程模板







