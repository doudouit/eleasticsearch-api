package com.allen.es.controller;

import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import com.allen.es.dto.ResponseResult;
import com.allen.es.entity.Goods;
import com.allen.es.service.impl.ElasticSearchServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 窦建新
 * @version V1.0
 * @project SpringBoot-ElasticSearch-Pro
 * @date Created in 2022/10/25 /005 18:48
 * @description
 **/
//@Api("ElasticSearch Api Controller")
@RequestMapping("elasticSearch-controller")
@RestController
public class ElasticSearchController {

    /*@Autowired
    private IElasticSearchService elasticSearchService;*/

    private ElasticSearchServiceImpl<Goods> elasticSearchService;

    @Autowired
    public void setElasticSearchService(ElasticSearchServiceImpl<Goods> elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }

    //@ApiOperation("查看索引是否存在")
    @GetMapping("seeIndexIsNoExists")
    public boolean seeIndexIsNoExists(String indexName) {
        return elasticSearchService.seeIndexIsNoExists(indexName);
    }

    //@ApiOperation("创建索引")
    @GetMapping("createIndex")
    public ResponseResult createIndex(String indexName) {
        Map<String, Property> propertyMap = new HashMap<>(16);
        propertyMap.put("id", new Property(new LongNumberProperty.Builder().index(true).store(true).build()));
        propertyMap.put("name", new Property(new TextProperty.Builder().index(true).analyzer("ik_max_word").store(true).build()));

        if (elasticSearchService.createIndex(indexName, 1, propertyMap)) {
            return ResponseResult.ok();
        }
        return ResponseResult.error();
    }

    //@ApiOperation("删除索引")
    @PostMapping("deleteIndex")
    public ResponseResult deleteIndex(@RequestParam List<String> indexNameList) {
        if (elasticSearchService.deleteIndex(indexNameList)) {
            return ResponseResult.ok();
        }
        return ResponseResult.error();
    }


    //@ApiOperation(value = "根据id获取文档数据")
    @GetMapping("/api/v1/getById/{index}/{id}")
    public ResponseResult getById(@PathVariable String index, @PathVariable String id) {
        return ResponseResult.ok().data("goods", elasticSearchService.getById(index, id, Goods.class));
    }

    //@ApiOperation(value = "根据id数组获取文档数据")
    @PostMapping("/api/v1/getByIdList")
    public ResponseResult getByIdList(@RequestParam(value = "ids") List<String> ids,
                                      @RequestParam(value = "index") String index) {
        return ResponseResult.ok().data("goods", elasticSearchService.getByIdList(index, ids, Goods.class));
    }

    //@ApiOperation(value = "分页查询文档")
    @PostMapping("/getAll")
    public ResponseResult getAll(String index, int pageNo, int pageSize) {
        return ResponseResult.ok().data("goods", elasticSearchService.searchByPages(index, pageNo, pageSize, Goods.class));
    }

    //@ApiOperation(value = "分页条件查询")
    @PostMapping("/querySearch")
    public ResponseResult getAll(String index, String queryString, Integer pageNo, Integer pageSize) {
        return ResponseResult.ok().data("goods", elasticSearchService.searchByQuery(index, queryString, pageNo, pageSize, Goods.class));
    }

    //@ApiOperation(value = "分页条件查询高亮")
    @PostMapping("/api/v1/searchByQueryHighlight")
    public ResponseResult searchByQueryHighlight(String index, String queryString, Integer pageNo, Integer pageSize) {
        return ResponseResult.ok().data("goods", this.elasticSearchService.searchByQueryHighlight(index, queryString, pageNo, pageSize, Goods.class));
    }

}