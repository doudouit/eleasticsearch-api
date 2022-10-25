package com.allen.es.service;


import co.elastic.clients.elasticsearch._types.mapping.Property;

import java.util.List;
import java.util.Map;

/**
 * @author 窦建新
 * @description
 * @date 2022/10/24 17:14
 */
public interface IElasticSearchService<T> {
    /**
     * 查看索引是否存在
     *
     * @param indexName 索引名称
     * @return boolean true，代表存在，false，代表不存在
     */
    boolean seeIndexIsNoExists(String indexName);
    /**
     * 创建索引
     *
     * @param indexName   索引名称
     * @param numOfShards 分片数
     * @param properties  属性
     * @return boolean true，代表创建成功，false，代表创建失败
     */
    boolean createIndex(String indexName, int numOfShards, Map<String, Property> properties);

    /**
     * 删除索引
     *
     * @param indexNameList 索引名称列表
     * @return boolean
     */
    boolean deleteIndex(List<String> indexNameList);



    /**
     * 根据id获取文档
     *
     * @param index index
     * @param id    id
     * @param clazz clazz 把查询的结果封装成对象的实体
     * @return T
     */
    T getById(String index, String id, Class<T> clazz);

    /**
     * 根据id列表获取文档
     *
     * @param index  index
     * @param idList id
     * @param clazz  clazz
     * @return List<T>
     */
    List<T> getByIdList(String index, List<String> idList, Class<T> clazz);

    /**
     * 分页查询
     *
     * @param index    index
     * @param pageNo   pageNo
     * @param pageSize pageSize
     * @param clazz    clazz
     * @return 查询结果
     */
    List<T> searchByPages(String index, Integer pageNo, Integer pageSize, Class<T> clazz);

    /**
     * 分页条件查询
     *
     * @param index    index
     * @param pageNo   当前页
     * @param pageSize 每页多少条数据
     * @param clazz    clazz  封装的实现
     * @return 查询结果
     */
    List<T> searchByQuery(String index, String queryString, Integer pageNo, Integer pageSize, Class<T> clazz);

    /**
     * 分页条件高亮查询
     *
     * @param index    index
     * @param pageNo   当前页
     * @param pageSize 每页多少条数据
     * @param clazz    clazz  封装的实现
     * @return 查询结果
     */
    List<T> searchByQueryHighlight(String index, String queryString, Integer pageNo, Integer pageSize, Class<T> clazz);
}
