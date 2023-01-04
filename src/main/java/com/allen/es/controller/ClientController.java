package com.allen.es.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.StoredScript;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.FuzzyQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.json.JsonData;
import com.allen.es.dto.ResultDto;
import com.allen.es.entity.CarSerialBrand;
import com.allen.es.util.ESClient;
import lombok.SneakyThrows;
import org.elasticsearch.client.Request;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    /**
     * 分页查询
     *
     * @param keyword
     * @param from
     * @param size
     * @return
     */
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
        List<CarSerialBrand> carSerialBrands = response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        resultDto.setData(carSerialBrands);
        return resultDto;
    }

    //region 滚动查询
    @RequestMapping("/scroll")
    @SneakyThrows
    public ResultDto scroll(String scrollId) {
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index("car")
                .scroll(Time.of(t -> t.time("1m")))
                .size(2));
        SearchResponse<CarSerialBrand> response = scrollId == null
                ? client.search(searchRequest, CarSerialBrand.class)
                : client.scroll(ScrollRequest.of(s -> s.scrollId(scrollId)), CarSerialBrand.class);
        List<CarSerialBrand> list = response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        ResultDto res = new ResultDto();
        res.setTag(response.scrollId());
        res.setData(list);
        return res;
    }

    //region fuzzy
    @RequestMapping("/fuzzy")
    @SneakyThrows
    public List<CarSerialBrand> fuzzy(String name) {
        SearchResponse<CarSerialBrand> response = client.search(SearchRequest.of(s -> s
                .index("car")
                .query(Query.of(q -> q
                        .fuzzy(FuzzyQuery.of(f -> f
                                .field("brand_name.keyword")
                                .value(name)
                                .fuzziness("auto")
                        ))
                ))
        ), CarSerialBrand.class);
        List<CarSerialBrand> list = response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());

        return list;
    }


    //region Bulk
    @RequestMapping("/bulk")
    @SneakyThrows
    public ResultDto bulk() {
        BulkResponse bulkResponse = client.bulk(b -> b
                .index("car")
                .operations(BulkOperation.of(o -> o
                        .delete(DeleteOperation.of(d -> d
                                .id("300")
                        ))
                ))
                .operations(BulkOperation.of(o -> o
                        .update(UpdateOperation.of(u -> u
                                .id("301")
                                .action(UpdateAction.of(a -> a
                                        .doc(new CarSerialBrand())
                                ))
                        ))
                ))
                .operations(BulkOperation.of(o -> o
                        .index(IndexOperation.of(i -> i
                                .index("car")
                                .id("33333")
                                .document(new CarSerialBrand())
                        ))
                ))
        );
        return null;
    }

    //region Search template
    @RequestMapping("/templateSearch")
    @SneakyThrows
    public ResultDto templateSearch() {
        Request scriptRequest = new Request("POST", "_scripts/test_template_search");
        scriptRequest.setJsonEntity(
                "{" +
                        "  \"script\": {" +
                        "    \"lang\": \"mustache\"," +
                        "    \"source\": {" +
                        "      \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                        "      \"size\" : \"{{size}}\"" +
                        "    }" +
                        "  }" +
                        "}");
        PutScriptRequest putScriptRequest = new PutScriptRequest.Builder()
                .id("_scripts/test_template_search")
                .script(StoredScript.of(s -> s
                        .source(
                                "{" +
                                        "  \"script\": {" +
                                        "    \"lang\": \"mustache\"," +
                                        "    \"source\": {" +
                                        "      \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                                        "      \"size\" : \"{{size}}\"" +
                                        "    }" +
                                        "  }" +
                                        "}"
                        )
                ))
                .build();
        client.putScript(putScriptRequest);

        Map<String, JsonData> scriptParams = new HashMap<>();
        scriptParams.put("field", JsonData.of("master_brand_name"));
        scriptParams.put("value", JsonData.of("一汽"));
        scriptParams.put("size", JsonData.of(5));

        SearchTemplateResponse<CarSerialBrand> response = client.searchTemplate(SearchTemplateRequest.of(t -> t
                .index("car")
                .params(scriptParams)
                .id("test_template_search")
        ), CarSerialBrand.class);
        return null;
    }
}
