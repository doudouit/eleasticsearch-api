package com.allen.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.NodeStatistics;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ApiTypeHelper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author 窦建新
 * @description
 * @date 2022/10/24 9:44
 */
@Slf4j
public class LowLevelClient {

    @Test
    public void createClient() throws Exception {
        //tag::create-client
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);
        //end::create-client

        //tag::first-reques
        SearchResponse<Object> search = client.search(s -> s
                        .index("product"),
                Object.class);

        for (Hit<Object> hit : search.hits().hits()) {
            System.out.println(hit.source());
        }
        //end::first-request
    }

    @Test
    @SneakyThrows
    public void createIndex() {
        // Create the low-level client
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);

        client.indices().create(c -> c.index("products"));
    }

    @Test
    @SneakyThrows
    public void asyncClient() {
        // Create the low-level client
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);

        if (client.exists(b -> b.index("products").id("foo")).value()) {
            log.info(" product exists");
        }

        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);

        asyncClient
                .exists(b -> b.index("products").id("foo"))
                .whenComplete((response, exception) -> {
                    if (exception != null) {
                        log.error("Failed to index");
                    } else {
                        log.info(" Product exists");
                    }
                });
    }

    @Test
    public void test() {
        NodeStatistics stats = NodeStatistics.of(b -> b
                .total(1)
                .failed(0)
                .successful(1)
        );

        // The `failures` list was not provided.
        // - it's not null
        assertNotNull(stats.failures());
        // - it's empty
        assertEquals(0, stats.failures().size());
        // - and if needed we can know it was actually not defined
        assertFalse(ApiTypeHelper.isDefined(stats.failures()));
    }

}
