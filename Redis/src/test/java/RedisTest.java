import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
        System.out.println("3秒内循环执行了"+i+"次");
        Boolean s15 = jedis.exists("s1");
        System.out.println("s1的状态为:" + s15);
        System.out.println("=============================");

        jedis.setex("s3", 5, "hadoop");

    }

    @After
    public void jedisRelease() {
        jedis.close();
    }
}
