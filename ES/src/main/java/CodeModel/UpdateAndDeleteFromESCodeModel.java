package CodeModel;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @ClassName: CodeModel.UpdateAndDeleteFromESCodeModel
 * @Author: Roohom
 * @Function: 更新与删除ES数据代码模板
 * @Date: 2020/9/28 22:04
 * @Software: IntelliJ IDEA
 */
public class UpdateAndDeleteFromESCodeModel {
    TransportClient client = null;
    String indexName = "bigdata23";
    String typeName = "article";

    /**
     * 用于构建一个ES的客户端的实例
     */
    @Before
    public void getEsClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node1"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node2"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node3"), 9300));

    }

    /**
     * 更新数据
     */
    @Test
    public void udpateData() {
        Person person = new Person("宋元淑", 22, "177");
        String string = JSON.toJSONString(person);
        client.prepareUpdate(indexName, typeName, "2").setDoc(string, XContentType.JSON).get();
    }

    /**
     * 数据删除
     */
    @Test
    public void deleteData() {
        //根据id进行删除
//        client.prepareDelete(indexName, typeName, "1").get();

        //根据查询条件进行删除
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .source(indexName)
                .filter(QueryBuilders.termQuery("phone","199"))
                .get();
        //删库跑路
//        client.admin().indices().prepareDelete(indexName).get();
    }


    /**
     * 释放资源
     */
    @After
    public void close() {
        client.close();
    }
}
