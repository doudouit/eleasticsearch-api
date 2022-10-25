package com.allen.es.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import com.allen.es.dto.ResultDto;
import com.allen.es.entity.CarSerialBrand;
import com.allen.es.util.ESClient;
import lombok.SneakyThrows;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/car")
public class ClientController {

    ElasticsearchClient client = ESClient.getInstance().getRestClient();

    /*RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
    // Create the transport with a Jackson mapper
    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    // And create the API client
    ElasticsearchClient client = new ElasticsearchClient(transport);*/

    //region index
    @RequestMapping("/init")
    @SneakyThrows
    public String indexInit() {
        // 方式一： 从resources目录下读取
        InputStream input = this.getClass().getClassLoader()
                .getResourceAsStream("some-index.json");
        CreateIndexRequest request = CreateIndexRequest.of(b -> b
                .index("test_index")
                .withJson(input)
                .settings(IndexSettings.of(s -> s
                        .numberOfShards("3")
                        .numberOfReplicas("2")
                ))
        );

        // 方式二： 组装
        Map<String, Property> propertyMap = new HashMap<>(16);
        propertyMap.put("id", new Property(new LongNumberProperty.Builder()
                .index(true)
                .store(true)
                .build()));
        propertyMap.put("name", new Property(new TextProperty.Builder()
                .index(true)
                .analyzer("ik_max_word")
                .store(true)
                .build()));
        TypeMapping typeMapping = new TypeMapping.Builder()
                .properties(propertyMap)
                .build();
        IndexSettings indexSettings = new IndexSettings.Builder()
                .numberOfShards("2")
                .numberOfReplicas("3")
                .build();
        CreateIndexRequest request2 = CreateIndexRequest.of(b -> b
                .index("test_index2")
                .mappings(typeMapping)
                .settings(indexSettings)
        );

        // 方式三： lambda表达式组装
        CreateIndexRequest request3 = CreateIndexRequest.of(b -> b
                .index("test_index3")
                .mappings(TypeMapping.of(t -> t
                        .properties("message", new Property(new TextProperty.Builder()
                                .index(true)
                                .store(true)
                                .build()))
                        .properties("phone", new Property(new LongNumberProperty.Builder()
                                .build()))
                ))
                .settings(s -> s
                        .numberOfShards("3")
                        .numberOfReplicas("2")
                )
        );

        CreateIndexResponse createIndexResponse = client.indices().create(request3);
        if (Boolean.TRUE.equals(createIndexResponse.acknowledged())) {
            System.out.println("创建index成功!");
        } else {
            System.out.println("创建index失败!");
        }
        return createIndexResponse.index();
    }

    @RequestMapping("/carInfo")
    @SneakyThrows
    public ResultDto carInfo(@RequestParam(value = "keyword", required = true) String keyword,
                             @RequestParam(value = "from", required = true) Integer from,
                             @RequestParam(value = "size", required = true) Integer size) {
        SearchResponse<CarSerialBrand> response = client.search(SearchRequest.of(s -> s
                .index("car")
                .query(Query.of(q -> q
                        .match(MatchQuery.of(m -> m
                                .field("series_name")
                                .query(keyword)
                        ))
                ))
                .from(from)
                .size(size)
        ), CarSerialBrand.class);
        ResultDto<Object> resultDto = new ResultDto<>();
        resultDto.setData(response.hits().hits());
        return resultDto;
    }
}
