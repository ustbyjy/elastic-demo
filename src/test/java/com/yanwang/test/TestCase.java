package com.yanwang.test;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
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

    @Test
    public void testClient2() throws UnknownHostException {
        InetSocketTransportAddress node = new InetSocketTransportAddress(InetAddress.getByName("node1"), 9300);

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true) // 设为true时，使客户端去嗅探整个集群的状态，把集群中其他机器的IP地址加到客户端种
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
                .field("number_of_shards", 1) // 设置分片数量
                .field("number_of_replicas", 0) // 设置副本数量
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
    public void testUpdateDocument1() throws IOException {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName);
        updateRequest.type(typeName);
        updateRequest.id("1");
        updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("type", "file").endObject());

        UpdateResponse updateResponse = client.update(updateRequest).actionGet();
        System.out.println(updateResponse);
    }


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


    @Test
    public void testGetDocument() {
        TransportClient client = prepareClient();

        String indexName = "secisland";
        String typeName = "secilog";
        GetResponse getResponse = client.prepareGet(indexName, typeName, "1").get();
        Map<String, Object> source = getResponse.getSource();

        long version = getResponse.getVersion();
        String id = getResponse.getId();
        System.out.println("version: " + version);
        System.out.println("id: " + id);
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

}
