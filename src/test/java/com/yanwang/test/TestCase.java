package com.yanwang.test;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: Administrator
 * Date: 2017/11/6
 * Time: 14:15
 */
public class TestCase {

    @Test
    public void testClient1() throws UnknownHostException {
        InetSocketTransportAddress node1 = new InetSocketTransportAddress(InetAddress.getByName("node1"), 9300);
        InetSocketTransportAddress node2 = new InetSocketTransportAddress(InetAddress.getByName("node2"), 9300);
        InetSocketTransportAddress node3 = new InetSocketTransportAddress(InetAddress.getByName("node3"), 9300);

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node1)
                .addTransportAddress(node2)
                .addTransportAddress(node3);

        client.close();
    }

    /**
     * client.transport.sniff ：设为true时，使客户端去嗅探整个集群的状态，把集群中其他机器的IP地址加到客户端中
     * client.transport.ignore_cluster_name：设为true时，忽略连接节点集群验证
     * client.transport.ping_timeout：ping一个节点的响应时间，默认5秒
     * client.transport.nodes_sampler_interval：sample/ping节点的时间间隔，默认5秒
     *
     * @throws UnknownHostException
     */
    @Test
    public void testClient2() throws UnknownHostException {
        InetSocketTransportAddress node = new InetSocketTransportAddress(InetAddress.getByName("node1"), 9300);

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node);

        client.close();
    }

    public TransportClient prepareClient() {
        TransportClient client = null;
        try {
            InetSocketTransportAddress node = new InetSocketTransportAddress(InetAddress.getByName("node1"), 9300);
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch")
                    .put("client.transport.sniff", true) // 设为true时，使客户端去嗅探整个集群的状态，把集群中其他机器的IP地址加到客户端种
                    .build();
            client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(node);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    @Test
    public void testCreateIndex() throws IOException {
        TransportClient client = prepareClient();

        String typeName = "secilog";

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("settings")
                .field("number_of_shards", 3) // 设置分片数量
                .field("number_of_replicas", 2) // 设置副本数量
                .endObject()
                .endObject()
                .startObject()
                .startObject(typeName)
                .startObject("properties")
                .startObject("type")
                .field("type", "string")
                .field("store", "yes")
                .endObject()
                .startObject("eventCount")
                .field("type", "long")
                .field("store", "yes")
                .endObject()
                .startObject("eventDate")
                .field("type", "date")
                .field("format", "dateOptionalTime")
                .field("store", "yes")
                .endObject()
                .startObject("message")
                .field("type", "string")
                .field("index", "not_analyzed")
                .field("store", "yes")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        System.out.println(mapping.string());

        String indexName = "secisland";

        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName).setSource(mapping);

        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute().actionGet();
        if (createIndexResponse.isAcknowledged()) {
            System.out.println("Index created.");
        } else {
            System.err.println("Index creation failed.");
        }
    }

    @Test
    public void testCreateIndex2() throws IOException {
        TransportClient client = prepareClient();

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "kimchy");
        json.put("postDate", "2013-01-30");
        json.put("message", "trying out Elasticsearch");

        IndexResponse indexResponse = client.prepareIndex("fendo", "fendodate")
                .setSource(json)
                .get();

        System.out.println(indexResponse.getResult());
    }


    @Test
    public void testCreateDocument() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";
        String id = "1";

        XContentBuilder source = XContentFactory.jsonBuilder();
        source.startObject()
                .field("type", "syslog")
                .field("eventCount", 1)
                .field("eventDate", new Date())
                .field("message", "secilog insert doc test")
                .endObject();

        IndexResponse indexResponse = client.prepareIndex(indexName, typeName, id)
                .setSource(source).get();

        System.out.println("index:" + indexResponse.getIndex() + " insert doc id: " + indexResponse.getId());

    }


    @Test
    public void testBatchCreateDocument() throws IOException {
        TransportClient client = prepareClient();

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "james")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .endObject()
                )
        );
        BulkResponse bulkResponse = bulkRequest.get();
        System.out.println(bulkResponse.hasFailures());
        if (bulkResponse.hasFailures()) {
            System.out.println("处理失败");
        }
    }

    /**
     * UpdateRequest方式
     *
     * @throws IOException
     */
    @Test
    public void testUpdateDocument1() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName);
        updateRequest.type(typeName);
        updateRequest.id("1");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("type", "file")
                .endObject());

        UpdateResponse updateResponse = client.update(updateRequest).actionGet();
        System.out.println(updateResponse);
    }

    /**
     * 更新插入：如果存在就更新，如果不存在就插入
     *
     * @throws IOException
     */
    @Test
    public void testUpdateDocument2() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";
        IndexRequest indexRequest = new IndexRequest(indexName, typeName, "3")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("type", "syslog")
                        .field("eventCount", 2)
                        .field("eventDate", new Date())
                        .field("message", "secilog insert doc test")
                        .endObject());

        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, "3")
                .doc(XContentFactory.jsonBuilder().startObject().field("type", "file").endObject())
                .upsert(indexRequest);

        UpdateResponse updateResponse = client.update(updateRequest).actionGet();
        System.out.println(updateResponse);
    }

    /**
     * prepareUpdate方式
     *
     * @throws IOException
     */
    @Test
    public void testUpdateDocument3() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";

        UpdateResponse updateResponse = client.prepareUpdate(indexName, typeName, "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("type", "file")
                        .endObject())
                .get();
        System.out.println(updateResponse);
    }

    /**
     * operationThreaded：设置为true，在不同的线程里执行此次操作
     */
    @Test
    public void testGetDocument() {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";
        GetResponse getResponse = client.prepareGet(indexName, typeName, "1")
                .setOperationThreaded(false)
                .get();
        Map<String, Object> source = getResponse.getSource();

        long version = getResponse.getVersion();
        String id = getResponse.getId();
        System.out.println("version: " + version);
        System.out.println("id: " + id);
    }

    @Test
    public void testMultiGetDocument() {
        TransportClient client = prepareClient();

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("secisland", "secilog", "1")
                .add("new_secisland", "secilog", "1")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse getResponse = itemResponse.getResponse();
            if (getResponse.isExists()) {
                String json = getResponse.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    @Test
    public void testDeleteDocument() {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";
        String id = "4";
        DeleteResponse deleteResponse = client.prepareDelete(indexName, typeName, id).get();
        System.out.println(deleteResponse);

        id = "3";
        deleteResponse = client.prepareDelete(indexName, typeName, id).get();
        System.out.println(deleteResponse);
    }

    @Test
    public void testDeleteByQueryDocument() {
        TransportClient client = prepareClient();

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "kimchy"))
                .source("fendo") // 索引名
                .get(); // 执行

        System.out.println(response);
        System.out.println(response.getDeleted()); // 删除文档的数量
    }


    @Test
    public void testMapping() throws IOException {
        TransportClient client = prepareClient();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.index("secisland");

        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 2);
        createIndexRequest.settings(settings);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("logType")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject();
        createIndexRequest.mapping("secilog", xContentBuilder);

        CreateIndexResponse createIndexResponse = client.admin().indices().create(createIndexRequest).actionGet();
        if (createIndexResponse.isAcknowledged()) {
            System.out.println("Index created.");
        } else {
            System.err.println("Index creation failed.");
        }
    }


    @Test
    public void testDeleteIndex() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";

        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(indexName);
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(indicesExistsRequest).actionGet();
        if (indicesExistsResponse.isExists()) {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(indexName).execute().actionGet();
            System.out.println(deleteIndexResponse.isAcknowledged());
        }
    }


    @Test
    public void testGetIndex() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";

        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);

        GetIndexResponse getIndexResponse = client.admin().indices().getIndex(getIndexRequest).actionGet();
        getIndexResponse.indices();
        getIndexResponse.settings();
        getIndexResponse.mappings();
    }


    @Test
    public void testUpdateMapping() throws IOException {
        TransportClient client = prepareClient();

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("user")
                .startObject("properties")
                .startObject("name")
                .startObject("properties")
                .startObject("first")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .startObject("user_id")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        System.out.println(xContentBuilder.string());

        PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping("secisland").setType("user").setSource(xContentBuilder).execute().actionGet();

        System.out.println(putMappingResponse.isAcknowledged());

        // =================================================

        xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("user")
                .startObject("properties")
                .startObject("name")
                .startObject("properties")
                .startObject("last")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .startObject("user_id")
                .field("type", "string")
                .field("index", "not_analyzed")
                .field("ignore_above", 100)
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        System.out.println(xContentBuilder.string());

        putMappingResponse = client.admin().indices().preparePutMapping("secisland").setType("user").setSource(xContentBuilder).execute().actionGet();

        System.out.println(putMappingResponse.isAcknowledged());
    }


    @Test
    public void testReindex() throws IOException {
        TransportClient client = prepareClient();

        ReindexRequestBuilder reindexRequestBuilder = ReindexAction.INSTANCE.newRequestBuilder(client).source("secisland").destination("new_secisland");
        BulkByScrollResponse bulkByScrollResponse = reindexRequestBuilder.execute().actionGet();

        System.out.println(bulkByScrollResponse);

    }


    @Test
    public void testQuery() {
        TransportClient client = prepareClient();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("eventCount", 1));

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("eventDate");
        rangeQueryBuilder.to(new Date().getTime());

        boolQueryBuilder.filter(rangeQueryBuilder);

        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("eventDate").order(SortOrder.DESC);

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("secisland")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, false))
                .setTypes("secilog");

        searchRequestBuilder.setQuery(boolQueryBuilder).addSort(fieldSortBuilder);
        System.out.println(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();
        System.out.println(searchResponse.toString());

    }

}
