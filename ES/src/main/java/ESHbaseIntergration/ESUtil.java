package ESHbaseIntergration;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: ESUtile
 * @Author: Roohom
 * @Function: ES读写的工具类
 * @Date: 2020/9/29 17:05
 * @Software: IntelliJ IDEA
 */
public class ESUtil {
    static String indexName = "articles";
    static String typeName = "article";

    /**
     * 获取ES连接
     *
     * @return ES连接客户端对象
     * @throws UnknownHostException 可能的异常
     */
    public static TransportClient getEsClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").build();
        return new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node1"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node2"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node3"), 9300));
    }


    /**
     * 将Excel的数据批量写入ES
     *
     * @param esArticles 存放esArticle(自定义的Java Bean)的集合
     * @throws UnknownHostException 可能的异常
     */
    public static void write(List<EsArticle> esArticles) throws UnknownHostException {
        TransportClient esClient = getEsClient();
        BulkRequestBuilder bulk = esClient.prepareBulk();
        //迭代集合里面的每条数据，用于装入bulk中
        for (EsArticle esArticle : esArticles) {
            //将自定义的Java Bean转换成Json数据
            String string = JSON.toJSONString(esArticle);
            IndexRequestBuilder requestBuilder = esClient.prepareIndex(indexName, typeName, esArticle.getId()).setSource(string, XContentType.JSON);
            bulk.add(requestBuilder);
        }
        //批量执行
        bulk.get();
    }


    /**
     * 根据关键词检索ES
     *
     * @param keyword 关键词
     * @return 装入了搜索结果的集合
     * @throws UnknownHostException 可能的异常
     */
    public static List<EsArticle> read(String keyword) throws UnknownHostException {
        List<EsArticle> list = new ArrayList<>();
        TransportClient esClient = getEsClient();
        SearchResponse title = esClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(QueryBuilders.matchQuery("title", keyword))
                .get();
        SearchHit[] hits = title.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            EsArticle esArticle = JSON.parseObject(sourceAsString, EsArticle.class);
            list.add(esArticle);
        }
        return list;
    }
}
