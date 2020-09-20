import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Set;

/**
 * @ClassName: RedisTest
 * @Author: Roohom
 * @Function: 测试Redis连接和基础操作
 * @Date: 2020/9/15 16:52
 * @Software: IntelliJ IDEA
 */
public class RedisTest {

    Jedis jedis = null;

    @Before
    public void getReidsClient() {
        //连接池对象配置
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);

        //构建连接池对象
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "192.168.88.221", 6379);
        //从连接池获得连接
        jedis = jedisPool.getResource();
    }

    @Test
    public void baseTest() {
        //set/get/incr/decr/append/expire/exists/setex/ttl
        System.out.println("=======设置s1并读取========");
        jedis.select(0);
        jedis.set("s1", "1");
        String s1 = jedis.get("s1");
        System.out.println(s1);

        System.out.println("=======设置s2并读取==========");
        jedis.set("s2", "hello jedis");
        String s2 = jedis.get("s2");
        System.out.println(s2);

        System.out.println("========s1增加1并读取==========");
        jedis.incr("s1");
        String s11 = jedis.get("s1");
        System.out.println(s11);

        System.out.println("========s1减一并读取==========");
        jedis.decr("s1");
        String s12 = jedis.get("s1");
        System.out.println(s12);

        System.out.println("=========s2追加内容并读取===========");
        jedis.append("s2", " hello python");
        String s21 = jedis.get("s2");
        System.out.println(s21);

        System.out.println("=========s1减100并读取=========");
        jedis.decrBy("s1", 100);
        String s13 = jedis.get("s1");
        System.out.println(s13);

        System.out.println("=========s1的状态=========");
        Boolean s14 = jedis.exists("s1");
        System.out.println(s14);

        System.out.println("=========设置s1的生存周期为5秒============");
        jedis.expire("s1", 5);
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000) {
            //循环执行三秒
        }
        Boolean aBoolean = jedis.exists("s1");
        System.out.println("s1的状态为:" + aBoolean);
        //再等待两秒钟
        long current = System.currentTimeMillis();
        int i = 0;
        while (System.currentTimeMillis() - current < 3000) {
            //循环执行两秒钟
            i++;
        }
        System.out.println("3秒内循环执行了" + i + "次");
        Boolean s15 = jedis.exists("s1");
        System.out.println("s1的状态为:" + s15);
        System.out.println("=============================");

        jedis.setex("s3", 5, "hadoop");

    }

    //del/lpush/rpush/lpop/rpop/linsert/rpoplpush
    @Test
    public void testList() {
        jedis.del("list1","list2");
        jedis.lpush("list1", "1", "2", "3");
        jedis.rpush("list1", "4", "5", "6");
        List<String> list1 = jedis.lrange("list1", 0, -1);
        System.out.println(list1);

        //在第一个1的前面插入9
        jedis.linsert("list1", BinaryClient.LIST_POSITION.BEFORE, "1", "9");

        //将一个集合的最后一个弹出放入另一个集合的左边
        jedis.lpush("list2", "1", "7", "4", "1");
        List<String> list11 = jedis.lrange("list2", 0, -1);
        System.out.println(list11);
        jedis.rpoplpush("list1", "list2");
        List<String> list12 = jedis.lrange("list1", 0, -1);
        List<String> list2 = jedis.lrange("list2", 0, -1);
        System.out.println(list12);
        System.out.println(list2);

    }

    //hset/hget/hgetAll/hkeys/hvals/hdel/del
    @Test
    public void testHash() {

    }

    //sadd/smembers/sinter/scard/srem/sismember
    //sinter用于取集合交集
    @Test
    public void testSet() {
        jedis.sadd("set1", "1", "2", "3", "4", "3", "2", "1");
        Set<String> set1 = jedis.smembers("set1");
        System.out.println(set1);
        Long set11 = jedis.scard("set1");
        System.out.println(set11);

        jedis.sadd("set2","1","4","8","6");
        Set<String> sinter = jedis.sinter("set1", "set2");
        System.out.println(sinter);
    }

    //zadd/zscore/zrank/zrevrangeWithScore/zrangeWithScores/zrem
    @Test
    public void testZset()
    {
        jedis.zadd("zset1",20.01,"www.jike.com");
        jedis.zadd("zset1",50.01,"www.baidu.com");
        jedis.zadd("zset1",30.01,"www.imdb.com");

        Set<Tuple> zset1 = jedis.zrangeWithScores("zset1", 0, -1);
        for (Tuple tuple : zset1) {
            System.out.println(tuple.getScore());
            System.out.println(tuple.getElement());
        }

        Double zset11 = jedis.zscore("zset1", "www.jike.com");
        System.out.println(zset11);

        //获取排名，从0开始
        Long zset12 = jedis.zrank("zset1", "www.baidu.com");
        System.out.println(zset12);
    }


    @After
    public void jedisRelease() {
        jedis.close();
    }
}
