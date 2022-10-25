package com.allen.es.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.allen.es.entity.Goods;
import com.allen.es.service.IElasticSearchService;
import com.allen.es.util.ESClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 窦建新
 * @description
 * @date 2022/10/24 17:17
 */
@Slf4j
@Service
public class ElasticSearchServiceImpl<T> implements IElasticSearchService<T> {

    ElasticsearchClient client = ESClient.getInstance().getRestClient();

    @Override
    public boolean seeIndexIsNoExists(String indexName) {
        try {
            BooleanResponse booleanResponse = client.indices()
                    .exists(ExistsRequest.of(r -> r.index(indexName)));
            return booleanResponse.value();
        } catch (IOException e) {
            log.error("see Index Is No Exists error", e);
        }

        return false;
    }

    @Override
    public boolean createIndex(String indexName, int numOfShards, Map<String, Property> properties) {
        // 创建的映射处理
        TypeMapping typeMapping = new TypeMapping.Builder()
                .properties(properties)
                .build();

        IndexSettings indexSettings = new IndexSettings.Builder()
                .numberOfShards(String.valueOf(numOfShards))
                .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                .index(indexName)
                .mappings(typeMapping)
                .settings(indexSettings)
                .build();

        try {
            return Optional
                    .ofNullable(client.indices().create(createIndexRequest).acknowledged())
                    .orElse(Boolean.FALSE);
        } catch (IOException e) {
            log.error("create Index error", e);
        }

        return false;
    }

    @Override
    public boolean deleteIndex(List<String> indexNameList) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder()
                .index(indexNameList)
                .build();

        try {
            return client.indices()
                    .delete(deleteIndexRequest).acknowledged();
        } catch (IOException e) {
            log.error("delete Index error", e);
        }

        return false;
    }


    @Override
    public T getById(String index, String id, Class<T> clazz) {
        GetRequest getRequest = new GetRequest.Builder()
                .index(index)
                .id(id)
                .build();
        try {
            return client.get(getRequest, clazz)
                    .source();
        } catch (IOException e) {
            log.error("get By Id error", e);
        }
        return null;
    }

    @Override
    public List<T> getByIdList(String index, List<String> idList, Class<T> clazz) {
        try {
            List<T> tList = new ArrayList<>(idList.size());
            for (String id : idList) {

                GetRequest getRequest = new GetRequest.Builder()
                        .index(index)
                        .id(id)
                        .build();

                T source = client.get(getRequest, clazz)
                        .source();

                tList.add(source);
            }

            return tList;
        } catch (IOException e) {
            log.error("get By Id List error", e);
        }
        return null;
    }

    @Override
    public List<T> searchByPages(String index, Integer pageNo, Integer pageSize, Class<T> clazz) {
        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(Collections.singletonList(index))
                .from(pageNo)
                .size(pageSize)
                .build();

        List<T> res = new ArrayList<>();

        try {
            SearchResponse<T> searchResponse = client.search(searchRequest, clazz);
            HitsMetadata<T> hitsMetadata = searchResponse.hits();
            hitsMetadata.hits().forEach(action -> res.add(action.source()));
            return res;
        } catch (IOException e) {
            log.error("search By Pages error", e);
        }

        return null;
    }


    @Override
    public List<T> searchByQuery(String index, String queryString, Integer pageNo, Integer pageSize, Class<T> clazz) {
        // 1.构建查询的对象
        QueryStringQuery stringQuery = new QueryStringQuery.Builder()
                .fields("name", "description")
                .query(queryString)
                .build();

        Query query = new Query.Builder()
                .queryString(stringQuery)
                .build();

        // 2.搜索
        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(index)
                .from(pageNo)
                .size(pageSize)
                .query(query)
                .build();

        try {
            return client.search(searchRequest, clazz)
                    .hits().hits().stream()
                    .map(Hit::source)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("search By Query error", e);
        }
        return null;
    }

    @Override
    public List<T> searchByQueryHighlight(String index, String queryString, Integer pageNo, Integer pageSize, Class<T> clazz) {
        // 1.构建查询的对象
        QueryStringQuery stringQuery = new QueryStringQuery.Builder()
                .fields("name", "description")
                .query(queryString)
                .build();

        Query query = new Query.Builder()
                .queryString(stringQuery)
                .build();

        // 高亮显示
        HighlightField highlightField = new HighlightField.Builder()
                .matchedFields("name")
                .preTags("<span style=\"color:red\">")
                .postTags("</span>")
                .build();

        Highlight highlight = new Highlight.Builder()
                .fields("name", highlightField)
                .requireFieldMatch(false)
                .build();

        // 2.搜索请求
        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(index)
                .from(pageNo)
                .size(pageSize)
                .query(query)
                .highlight(highlight)
                .build();

        try {
            return client.search(searchRequest, clazz)
                    .hits()
                    .hits()
                    .stream()
                    .map(mapper -> {
                        String name = mapper.highlight()
                                .get("name")
                                .get(0);

                        Goods goods = (Goods) mapper.source();
                        Goods anElse = Optional.ofNullable(goods)
                                .orElse(new Goods());
                        anElse.setName(name);
                        return mapper.source();
                    }).collect(Collectors.toList());
        } catch (IOException e) {
            log.error("search By Query Highlight error", e);
        }

        return null;
    }

}
