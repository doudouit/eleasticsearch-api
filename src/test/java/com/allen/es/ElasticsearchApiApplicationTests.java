package com.allen.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.allen.es.entity.CarSerialBrand;
import com.allen.es.entity.Product;
import com.allen.es.service.CarSerialBrandService;
import com.allen.es.service.ProductService;
import com.allen.es.util.ESClient;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SpringBootTest
class ElasticsearchApiApplicationTests {

    @Resource
    private ProductService productService;

    @Resource
    private CarSerialBrandService carService;

    @Test
    void contextLoads() {
    }

    ElasticsearchClient client = ESClient.getInstance().getRestClient();

    @Test
    @SneakyThrows
    public void esCRUD() {
        // 导入数据
//        create(client);
        // 查询
//        get(client);
//        getAll(client);
//        update(client);
//        delete(client);

//        multiSearch(client);
//        aggSearch(client);
//        batchInsertData(client);
//        multiGet(client);
        bulkInit(client);
    }


    @SneakyThrows
    public void create(ElasticsearchClient client) {
        List<Product> list = productService.list();
        for (Product product : list) {
            System.out.println(product.getCreateTime().toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            IndexResponse res = client.index(IndexRequest.of(i -> i
                    .index("product2")
                    .id(product.getId().toString())
                    .document(product)
            ));
            System.out.println(res.result());
        }
    }


    @SneakyThrows
    public void get(ElasticsearchClient client) {
        GetResponse<Product> response = client.get(GetRequest.of(p -> p
                .index("product2")
                .id("1")
        ), Product.class);
        String index = response.index();//获取索引名称
        String id = response.id();//获取索引id
        System.out.println("index:" + index);
        System.out.println("id:" + id);
        System.out.println(response.source());
    }

    @SneakyThrows
    public void getAll(ElasticsearchClient client) {
        SearchResponse<Product> response = client.search(SearchRequest.of(s -> s
                .index("product2")
        ), Product.class);
        HitsMetadata<Product> hits = response.hits();
        List<Hit<Product>> hitList = hits.hits();
        for (Hit<Product> productHit : hitList) {
            System.out.println("res" + productHit.source());
        }
    }

    @SneakyThrows
    public void update(ElasticsearchClient client) {
        Product product = new Product();
        product.setName("update name");
        UpdateResponse<Product> response = client.update(UpdateRequest.of(u -> u
                .index("product2")
                .id("2")
                .doc(product)
        ), Product.class);
        System.out.println(response.result());
    }

    @SneakyThrows
    public void delete(ElasticsearchClient client) {
        DeleteResponse response = client.delete(DeleteRequest.of(d -> d
                .index("product2")
                .id("1")
        ));
        System.out.println(response.result());
    }

    @SneakyThrows
    public void multiSearch(ElasticsearchClient client) {
        SearchResponse<Product> response = client.search(SearchRequest.of(s -> s
                .index("product2")
                /*.query(Query.of(q -> q
                        .term(TermQuery.of(t -> t
                                .field("name")
                                .value("xiaomi")
                        ))
                ))*/
                .query(QueryBuilders.term()
                        .field("name")
                        .value("xiaomi")
                        .build()._toQuery())
                .postFilter(QueryBuilders.range()
                        .field("price")
                        .from("0")
                        .to("4000")
                        .build()._toQuery())
                .from(0)
                .size(3)
        ), Product.class);

        HitsMetadata<Product> searchHits = response.hits();
        List<Hit<Product>> hits = searchHits.hits();
        for (Hit<Product> hit : hits) {
            System.out.println("res" + hit.source());
        }
    }

    @Test
    @SneakyThrows
    public void aggSearch(ElasticsearchClient client) {
        SearchResponse<Product> response = client.search(SearchRequest.of(s -> s
                        .index("product2")
                        /*.aggregations("group_by_month", AggregationBuilders.dateHistogram()
                                .field("createTime")
                                .calendarInterval(CalendarInterval.Month)
                                .build()._toAggregation()
                        )*/
                        .aggregations("group_by_month", Aggregation.of(a -> a
                                        .dateHistogram(DateHistogramAggregation.of(d -> d
                                                .field("createTime")
                                                .calendarInterval(CalendarInterval.Month)
                                                .format("yyyy-MM-dd")
                                        ))
                                        .aggregations("by_tag", Aggregation.of(b -> b
                                                .terms(TermsAggregation.of(t -> t.field("tags.keyword")))
                                                .aggregations("avg_price", Aggregation.of(c -> c
                                                        .avg(AverageAggregation.of(f -> f.field("price")))
                                                ))
                                        ))
                                )
                        )
        ), Product.class);

        List<Hit<Product>> hits = response.hits().hits();
        Map<String, Aggregate> map = response.aggregations();
        Aggregate group_by_month = map.get("group_by_month");
        DateHistogramAggregate histogram = group_by_month.dateHistogram();
        for (DateHistogramBucket dateBucket : histogram.buckets().array()) {
            System.out.println("\n\n月份：" + dateBucket.keyAsString() + "\n计数：" + dateBucket.docCount());
            Aggregate by_tag = dateBucket.aggregations().get("by_tag");
            List<StringTermsBucket> tagsBucket = by_tag.sterms().buckets().array();
            for (StringTermsBucket tagBucket : tagsBucket) {
                System.out.println("\t标签名称：" + tagBucket.key() + "\n\t数量：" + tagBucket.docCount());
                Aggregate avg_price = tagBucket.aggregations().get("avg_price");
                AvgAggregate avg = avg_price.avg();
                System.out.println("\t平均价格：" + avg.value() + "\n");
            }
        }
    }

    @Test
    @SneakyThrows
    public void batchInsertData(ElasticsearchClient client) {
        Product product = new Product();
        product.setPrice(3999.00);
        product.setDesc("xioami");
        BulkRequest request = BulkRequest.of(b -> b
                .index("test_index")
                .operations(BulkOperation.of(o -> o
                        .create(CreateOperation.of(c -> c
                                .id("1")
                                .document(product)
                        ))
                ))
                .operations(BulkOperation.of(o -> o
                        .create(CreateOperation.of(c -> c
                                .id("2")
                                .document(product)
                        ))
                ))
        );


        BulkResponse response = client.bulk(request);
        System.out.println("数量:" + response.items().size());
    }

    @Test
    @SneakyThrows
    public void multiGet(ElasticsearchClient client) {
        MgetResponse<Product> response = client.mget(MgetRequest.of(m -> m
                .index("product2")
                .ids(Arrays.asList("1", "2"))
        ), Product.class);
        for (MultiGetResponseItem<Product> responseItem : response.docs()) {
            System.out.println(responseItem.result().source());
        }
    }

    @Test
    @SneakyThrows
    public void bulkInit(ElasticsearchClient client) {
        BooleanResponse exists = client.indices().exists(ExistsRequest.of(e -> e
                .index("car")
        ));
        if (!exists.value()) {
            CreateIndexResponse indexResponse = client.indices().create(CreateIndexRequest.of(c -> c
                    .index("car")
                    .settings(IndexSettings.of(i -> i
                            .numberOfShards("3")
                            .numberOfReplicas("2")
                    ))
            ));
        }

        List<CarSerialBrand> list = carService.list();
        List<BulkOperation> bulkOperations = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            int j = i;
            bulkOperations.add(BulkOperation.of(b -> b
                    .create(CreateOperation.of(c -> c
                            .id(String.valueOf(j))
                            .document(list.get(j))
                    ))
            ));
        }
        // 批量插入数据， 更新、删除同理
        BulkRequest request = BulkRequest.of(b -> b
                .index("car")
                .operations(bulkOperations)
        );

        BulkResponse response = client.bulk(request);
        System.out.println("数量：" + response.items().size());

    }
}
