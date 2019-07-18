package com.example.esdemo.utils;

import com.example.esdemo.dto.PageDto;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于 RestHighLevelClient,ElasticSearch 6.6.2 简易工具类
 * create by zhu
 */
@Component
public class EsUtil {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Autowired
    RestHighLevelClient esClient;
    /**
     * 分页查询 default return 10 rows
     * @param <T>           返回实体映射
     * @param indexName     索引名称
     * @param clazz         类
     * @param queryConditionBuilder 条件构造器
     * @return
     * @throws IOException
     */
    public <T> PageDto<T> findPage(String indexName, Class<T> clazz, QueryConditionBuilder queryConditionBuilder) throws IOException {
        return findPage(indexName, indexName, clazz,queryConditionBuilder);
    }
    /**
     * 分页查询
     * @param <T>                   返回实体映射
     * @param indexName             索引名称
     * @param clazz                 类
     * @param queryConditionBuilder 条件构造器
     * @param page                  从1开始
     * @param pageSize              默认10
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T> PageDto<T> findPage(String indexName, Class<T> clazz, QueryConditionBuilder queryConditionBuilder, int page, int pageSize) throws IOException {
        return findPage(indexName, indexName, clazz,queryConditionBuilder,page,pageSize);
    }
    /**
     * 分页查询 default return 10 rows
     * @param <T>           返回实体映射
     * @param indexName     索引名称
     * @param type          类型
     * @param clazz         类
     * @param queryConditionBuilder 条件构造器
     * @return
     * @throws IOException
     */
    public <T> PageDto<T> findPage(String indexName, String type, Class<T> clazz, QueryConditionBuilder queryConditionBuilder) throws IOException {
        return findPage( indexName,  type, clazz,  queryConditionBuilder,1,10);
    }
    /**
     * 按条件查询单个数据
     * @param <T>           返回实体映射
     * @param indexName     索引名称
     * @param clazz         类
     * @param queryConditionBuilder 条件构造器
     * @return
     * @throws IOException
     */
    public <T> T findOne(String indexName,   Class<T> clazz, QueryConditionBuilder queryConditionBuilder) throws IOException {
        return findOne( indexName,  indexName, clazz,  queryConditionBuilder);
    }
    public <T> T findOne(String indexName, String type, Class<T> clazz, QueryConditionBuilder queryConditionBuilder) throws IOException {
        PageDto<T> pageDto = findPage( indexName,  type, clazz,  queryConditionBuilder,1,1);
        if(pageDto.getList() != null && pageDto.getList().size()>0){
            return pageDto.getList().get(0);
        }
        return null;
    }
    /**
     * 分页查询 default return 10 rows
     * @param <T>                   返回实体映射
     * @param indexName             索引名称
     * @param type                  类型
     * @param clazz                 类
     * @param queryConditionBuilder 条件构造器
     * @param page                  从1开始
     * @param pageSize              默认10
     * @param <T>
     * @return
     * @throws IOException
     */
    private <T> PageDto<T> findPage(String indexName, String type, Class<T> clazz, QueryConditionBuilder queryConditionBuilder, int page, int pageSize) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        queryConditionBuilder.doQueryCondition(searchSourceBuilder);
        //分页查询，设置起始下标，
        if(page >= 1){page -= 1;}else {page=0;}
        if(pageSize < 0){page = 10;}
        //下标从0开始
        searchSourceBuilder.from(page * pageSize);
        //每页显示个数
        searchSourceBuilder.size(pageSize);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
        /**
         * took：本次操作花费的时间，单位为毫秒。
         * timed_out：请求是否超时
         * _shards：说明本次操作共搜索了哪些分片
         * hits：搜索命中的记录
         * hits.total ： 符合条件的文档总数 hits.hits ：匹配度较高的前N个文档
         * hits.max_score：文档匹配得分，这里为最高分
         * _score：每个文档都有一个匹配度得分，按照降序排列。
         * _source：显示了文档的原始内容。
         */
        PageDto<T> pageDto = new PageDto<T>();
        SearchHits hits = response.getHits();
        //全部记录
        long allRecord = hits.getTotalHits();
        pageDto.setAllRecord(Integer.parseInt(String.valueOf(allRecord)));
        //最大页数
        long maxPage = allRecord/pageSize + (allRecord%pageSize > 0 ? 1 : 0);
        pageDto.setTotalPage(Integer.parseInt(String.valueOf(maxPage)));
        //当前页数 实际使用 从第一页开始 查询时-1 回传时 +1
        int currentPage = page >= 0 ? page+1 : page;
        pageDto.setPage(Integer.parseInt(String.valueOf(currentPage)));
        //分页长度
        int currentPageSize = pageSize;
        pageDto.setPageSize(Integer.parseInt(String.valueOf(currentPageSize)));

        SearchHit[] searchHits = hits.getHits();
        List<T> resList = new ArrayList<>();
        for (SearchHit hit : searchHits) {
//            String index = hit.getIndex();
//            String type = hit.getType();
//            String id = hit.getId();
//            float score = hit.getScore();
//            String sourceAsString = hit.getSourceAsString();
            T t = JsonUtils.toObject(hit.getSourceAsString(), clazz);
            resList.add(t);
        }
        pageDto.setList(resList);
        return pageDto;
    }
    /**
     * 获取索引byId
     * @param indexName 索引库名称
     * @param id        id标识
     * @param clazz     对象类型
     * @param <T>       返回类型
     * @return
     * @throws IOException
     */
    public <T> T getById(String indexName,String id,Class<T> clazz) throws IOException {
        return getById( indexName, indexName, id, clazz);
    }
    /**
     * 获取索引byId
     * @param indexName 索引库名称
     * @param id        id标识
     * @param type      类型 即将废弃
     * @param clazz     对象类型
     * @param <T>       返回类型
     * @return
     * @throws IOException
     */
    public <T> T getById(String indexName,String type,String id,Class<T> clazz) throws IOException {
        GetRequest request = new GetRequest(indexName).type(type).id(id);
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
        if(response.isExists()){
            return JsonUtils.toObject(response.getSourceAsString(), clazz);
        }
        return null;
    }
    /**
     * 删除索引
     * @param indexName 索引库名称
     * @param id        id标识
     * @return
     * @throws IOException
     */
    public boolean deleteById(String indexName,String id) throws IOException {
        DeleteRequest request = new DeleteRequest(indexName, indexName, id);
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        return DeleteResponse.Result.DELETED.equals(response.getResult());
    }
    /**
     * 删除索引
     * @param indexName 索引库名称
     * @param type      类型 即将废弃
     * @param id        id标识
     * @return
     * @throws IOException
     */
    public boolean deleteById(String indexName,String type,String id) throws IOException {
        DeleteRequest request = new DeleteRequest(indexName, type, id);
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        return DeleteResponse.Result.DELETED.equals(response.getResult());
    }
    /**
     * 修改索引 即修改数据
     * @param indexName 索引库名称
     * @param dto       保存或修改对象
     * @param id        id标识
     * @return
     * @throws IOException
     */
    public boolean updateById(String indexName,Object dto,String id) throws IOException {
        return updateById( indexName, indexName, dto, id);
    }
    /**
     * 修改索引 即修改数据
     * @param indexName 索引库名称
     * @param type      类型 即将废弃
     * @param dto       保存或修改对象
     * @param id        id标识
     * @return
     * @throws IOException
     */
    public boolean updateById(String indexName,String type,Object dto,String id) throws IOException {
        Assert.notNull(dto,"es 修改 失败 ! dto 不能为空");
        //索引请求对象
        UpdateRequest updateRequest = new UpdateRequest(indexName, type,
                id);
        //指定索引文档内容
        updateRequest.doc(JsonUtils.toJson(dto), XContentType.JSON);
        //索引修改响应对象
        UpdateResponse update = esClient.update(updateRequest,RequestOptions.DEFAULT);
        //获取响应结果
        RestStatus status = update.status();
        return RestStatus.OK.equals(status);
    }
    /**
     * 添加索引 即保存数据
     * @param indexName 索引库名称
     * @return
     */
    public boolean addById(String indexName,Object dto,String id) throws IOException {
        return addById( indexName, indexName, dto, id);
    }
    /**
     * 添加索引 即保存数据
     * @param indexName 索引库名称
     * @return
     */
    public boolean addById(String indexName,String type,Object dto,String id) throws IOException {
        Assert.notNull(dto,"es 保存失败 ! dto 不能为空");
        //索引请求对象
        IndexRequest indexRequest = new IndexRequest(indexName,type,id);
        //指定索引文档内容
        indexRequest.source(JsonUtils.toJson(dto), XContentType.JSON);
        //索引响应对象
        IndexResponse indexResponse = esClient.index(indexRequest,RequestOptions.DEFAULT);
        //获取响应结果
        DocWriteResponse.Result result = indexResponse.getResult();
        return DocWriteResponse.Result.CREATED.equals(result);
    }
    /**
     * 删除索引库
     * @param indexName 索引库名称
     * @return
     */
    public boolean deleteAll(String indexName){
        //删除索引请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        //删除索引
        AcknowledgedResponse deleteIndexResponse = null;
        try {
            deleteIndexResponse = esClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引删除失败 : {0}",e.getMessage());
            return false;
        }
        //删除索引响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        return acknowledged;
    }
    /**
     * 创建索引库
     * @param indexName 索引库名称
     * @param shard     分片数
     * @param replicas  副本数
     * @param builder   XContentBuilder or Json 设置字段以及分词器  如:
     *                 //设置映射
     *                 XContentBuilder builder = JsonXContent.contentBuilder()
     *                 .startObject()
     *                     .startObject("properties")
     *                         .startObject("name")
     *                             .field("type", "text")
     *                             .field("analyzer", "ik_max_word")
     *                             .field("search_analyzer", "ik_smart")
     *                         .endObject()
     *                         .startObject("pics")
     *                             .field("type", "text")
     *                             .field("index", "false")
     *                         .endObject()
     *                     .endObject()
     *                 .endObject();
     * @return
     */
    public boolean createIndex(String indexName,int shard,int replicas, XContentBuilder builder){
         return createIndex(indexName,indexName,shard,replicas,builder);
    }
    public boolean createIndex(String indexName,int shard,int replicas, String builder){
         return createIndex(indexName,indexName,shard,replicas,builder);
    }
    /**
     * 创建索引库
     * @param indexName 索引库名称
     * @param type      类型 官方计划7版本弱化 8版本完全删除
     * @param shard     分片数
     * @param replicas  副本数
     * @param builder   XContentBuilder or Json 设置字段以及分词器
     * @return
     */
    private boolean createIndex(String indexName, String type, int shard, int replicas, Object builder){
        Assert.notNull(builder,"builders 不能为空! ");
        //创建索引请求对象，并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        //设置索引参数
        createIndexRequest.settings(Settings.builder()
                .put("number_of_shards", shard)
                .put("number_of_replicas", replicas));
        if(builder instanceof XContentBuilder){
            createIndexRequest.mapping(type, (XContentBuilder)builder);
        }else if(builder instanceof String){
            createIndexRequest.mapping(type, (String)builder);
        }
        //创建索引操作客户端
        IndicesClient indices = esClient.indices();
        //创建响应对象
        CreateIndexResponse createIndexResponse = null;
        try {
            createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引创建失败 : {0}",e.getMessage());
            return false;
        }
        //得到响应结果
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
        return acknowledged;
    }
    /**
     * 条件构造器
     * @param <T>
     */
    public interface QueryConditionBuilder<T>{
        /**
         * //source源字段过虑,指定结果中所包括的字段有哪些
         *             searchSourceBuilder.fetchSource(new String[]{"name","price"}, new String[]{});
         *             searchSourceBuilder.query(QueryBuilders.termQuery("name",name));
         *
         *             MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(text,
         *                     "name", "describe")
         *                     .minimumShouldMatch("50%");
         *             multiMatchQueryBuilder.field("name",10);//提升boost
         *             searchSourceBuilder.query(multiMatchQueryBuilder);
         *
         * //            布尔查询
         *             BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
         *             //排序
         *             searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.DESC))
         *                     .sort(new FieldSortBuilder("price").order(SortOrder.ASC));
         * @param searchSourceBuilder
         * @throws IOException
         */
        void doQueryCondition(SearchSourceBuilder searchSourceBuilder) throws IOException;
    }
}
