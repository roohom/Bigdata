import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.naming.event.EventContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: GetDataFromESModel
 * @Author: Roohom
 * @Function: 实现自ES读取数据的代码模板
 * @Date: 2020/9/28 21:09
 * @Software: IntelliJ IDEA
 */
public class GetDataFromESCodeModel {
    TransportClient client = null;
    //构建一个索引库的名称
    String indexName = "bigdata23";
    //定义一个Type的名称
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

    @Test
    public void getDataFromDocumentId() {
        //查询某个document的数据
        GetResponse documentFields = client.prepareGet(indexName, typeName, "4").get();
        String sourceAsString = documentFields.getSourceAsString();
        System.out.println(sourceAsString);
    }

    /**
     * 使用Match模糊匹配全部数据
     */
    @Test
    public void getDataByMatchAll() {
        SearchResponse searchResponse = client.prepareSearch(indexName)
                //设置索引类型
                .setTypes(typeName)
                //设置查询器
                .setQuery(QueryBuilders.matchAllQuery())
                //设置显示多少个，类似于SQL中的limit
                .setSize(11)
                //执行
                .get();
        //json的第二层数据是我们想要的
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 范围查询
     */
    @Test
    public void getDataByRange() {
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                //范围查询:查询年龄在20到25之间，大于等于20小于等于25
                .setQuery(QueryBuilders.rangeQuery("age").gte(20).lt(25))
                .setSize(11)
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 词条精准查询，不做查询分词
     */
    @Test
    public void getDataByTerm() {
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(QueryBuilders.termQuery("name", "吴辛夷"))
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * Match匹配，做分词的分词匹配
     */
    @Test
    public void getDataByMatch() {
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(QueryBuilders.matchQuery("name", "吴辛夷"))
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 通配符的单个词的模糊查询:?和*
     */
    @Test
    public void getDataByWildcard() {
        SearchResponse wild = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(QueryBuilders.wildcardQuery("name", "李*"))
                .get();
        SearchHit[] hits = wild.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


    /**
     * 多条件的Bool查询
     */
    @Test
    public void getDataByBool() {
        RangeQueryBuilder age = QueryBuilders.rangeQuery("age").gte(20).lt(25);
        TermQueryBuilder phone = QueryBuilders.termQuery("phone", "170");
        RangeQueryBuilder id = QueryBuilders.rangeQuery("id").gte(2).lte(6);
        //age和phone的合并
        BoolQueryBuilder ageAndPhone = QueryBuilders.boolQuery().must(age).must(phone);

        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(QueryBuilders.boolQuery().should(ageAndPhone).should(id))
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 分页查询 类似于SQL中的limit
     */
    @Test
    public void getDataByLimit()
    {
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setTypes(typeName)
                .setFrom(0)
                .setSize(3)
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    @After
    public void close() {
        client.close();
    }
}
