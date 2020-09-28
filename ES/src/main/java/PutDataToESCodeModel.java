import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

/**
 * @ClassName: PutDataToESCodeModel
 * @Author: Roohom
 * @Function: 向ES写入数据的代码模板
 * @Date: 2020/9/27 21:36
 * @Software: IntelliJ IDEA
 */
public class PutDataToESCodeModel {
    /**
     * 初始化一个es的客户端对象
     */
    TransportClient client = null;
    /**
     * 构建一个索引库的名称
     */
    String indexName = "bigdata23";
    /**
     * 定义Type的名称
     */
    String typeName = "article";

    @Before
    public void getEsClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").build();
        //构建客户端对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node1"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node2"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node3"), 9300));
    }


    /**
     * 写入一条Json数据
     */
    @Test
    public void putJsonData() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "Nikon Z6");
        jsonObject.put("price", "11499");
        jsonObject.put("product_from", "Japan");
        String jsonString = JSONObject.toJSONString(jsonObject);
        IndexRequestBuilder requestBuilder = client.prepareIndex(indexName, typeName, "1").setSource(jsonString, XContentType.JSON);
        requestBuilder.execute().actionGet();

    }


    @Test
    public void putMapData() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("name", "Nikon D6");
        map.put("age", "10");
        map.put("phone", "110");
        client.prepareIndex(indexName, typeName, "2").setSource(map).get();
    }

    @Test
    public void putBeanData() {
        Person person = new Person();
        person.setName("李诗馨");
        person.setAge(23);
        person.setPhone("170");
        //将Java Bean转换为Json字符串
        String jsonString = JSON.toJSONString(person);
        client.prepareIndex(indexName, typeName, "4").setSource(jsonString, XContentType.JSON).get();
    }

    @Test
    public void putDataBulk() {
        //构建一个批量的请求的集合
        BulkRequestBuilder bulk = client.prepareBulk();
        Person person1 = new Person();
        person1.setName("顾欣");
        person1.setAge(25);
        person1.setPhone("199");
        Person person2 = new Person();
        person2.setName("吴辛夷");
        person2.setAge(23);
        person2.setPhone("177");
        String string1 = JSON.toJSONString(person1);
        String string2 = JSON.toJSONString(person2);
        //将JavaBean转换为Json字符串
        IndexRequestBuilder source1 = client.prepareIndex(indexName, typeName, "5").setSource(string1, XContentType.JSON);
        IndexRequestBuilder source2 = client.prepareIndex(indexName, typeName, "6").setSource(string2, XContentType.JSON);
        //放入请求的集合
        bulk.add(source1);
        bulk.add(source2);
        //对请求进行批量执行
        bulk.get();
    }

    /**
     * 释放连接等资源
     */
    @After
    public void close() {
        client.close();
    }
}
