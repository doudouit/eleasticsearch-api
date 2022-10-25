package com.allen.es.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.IOException;

public class ESClient {

    private static ESClient ESClient;
    private String host = "localhost:9200";
    private RestClientBuilder builder;
    private static RestClient restClient;
    private static ElasticsearchClient client;
    private static Sniffer sniffer;

    private ESClient(){
    }

    public static ESClient getInstance() {
        if (ESClient == null) {
            synchronized (ESClient.class) {
                if (ESClient == null) {
                    ESClient = new ESClient();
                    ESClient.initBuilder();
                }
            }
        }
        return ESClient;
    }

    public RestClientBuilder initBuilder() {
        String[] hosts = host.split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            String[] host = hosts[i].split(":");
            httpHosts[i] = new HttpHost(host[0], Integer.parseInt(host[1]), "http");
        }

        builder = RestClient.builder(httpHosts);

        //region 在Builder中设置请求头
        //  1.设置请求头
        /*Header[] defaultHeaders = new Header[]{
                new BasicHeader("Content-Type", "application/json")
        };
        builder.setDefaultHeaders(defaultHeaders);*/

        return builder;
    }

    public ElasticsearchClient getRestClient() {
        if (restClient == null) {
            synchronized (ESClient.class) {
                if (restClient == null) {
                    // Create the low-level client
                    restClient = builder.build();
                    // Create the transport with a Jackson mapper
                    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                    // And create the API client
                    client = new ElasticsearchClient(transport);

                    //十秒刷新并更新一次节点
                    sniffer = Sniffer.builder(restClient)
                            .setSniffIntervalMillis(5000)
                            .setSniffAfterFailureDelayMillis(15000)
                            .build();
                }
            }
        }

        return client;

    }

    /**
     *
     * 关闭sniffer client
     */
    public void closeClient() {
        if (null != client) {
            try {
                sniffer.close();    //需要在highClient close之前操作
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
