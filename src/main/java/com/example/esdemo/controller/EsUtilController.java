package com.example.esdemo.controller;

import com.example.esdemo.dto.Product;
import com.example.esdemo.utils.EsUtil;
import com.example.esdemo.utils.JsonUtils;
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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * esUtil 测试控制器
 */
@RestController
@RequestMapping("util")
public class EsUtilController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Autowired
    EsUtil esUtil;
    /**
     * 创建索引库
     *
     * @return
     */
    @GetMapping("createIndex")
    public Object doIndex() throws IOException {
        //设置映射
        XContentBuilder builder = JsonXContent.contentBuilder()
        .startObject()
            .startObject("properties")
                .startObject("name")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_smart")
                .endObject()
                .startObject("pics")
                    .field("type", "text")
                    .field("index", "false")
                .endObject()
            .endObject()
        .endObject();
        return esUtil.createIndex("product",1,1,builder);
    }

    /**
     * 删除索引库以及所有数据
     *
     * @return
     */
    @GetMapping("deleteIndex")
    public Object deleteIndex() throws IOException {
        return esUtil.deleteIndex("product");
    }

    /**
     * 商品(索引)保存
     * @return
     * @throws IOException
     */
    @GetMapping("/save")
    public Object save() throws IOException {
        String nameStr = "荣耀20 PRO,HUAWEI nova 5,荣耀8X,HUAWEI Mate 20 Pro,小米MIX 3  6GB+128GB,Redmi Note 7 Pro,小米Play 4GB+64GB,小米8 屏幕指纹版 6GB+128GB,小米8青春版6GB+64GB,Redmi Note 7 6GB+64G,Redmi K20 Pro 6GB+128GB,小米9 6GB+128GB";
        String descStr = "DXO全球第二高分 4800万全焦段AI四摄 双光学防抖 麒麟980全网通版8GB+128GB 幻夜星河,HUAWEI nova 5 8GB+128GB 全网通版（仲夏紫）订金50抵100,千元屏霸 高屏占比 2000万AI双摄 全网通 4GB+64GB（魅海蓝）最高优惠300,麒麟980 AI芯片 超广角徕卡三摄 OLED曲面屏 屏内指纹 8GB+128GB 全网通版（翡冷翠）直降400|赠好礼,磁动力滑盖全面屏 / 四曲面陶瓷机身,6GB+128GB大存储，索尼4800万拍照,八核高性能处理器，后置1200万 AI 双摄,全球首款压感屏幕指纹 骁龙845处理器,潮流镜面渐变色，2400万自拍旗舰,4800万拍照，4000mAh大电量,骁龙855旗舰处理器，索尼4800万超广角三摄,骁龙855，索尼4800万超广角微距三摄";
        String priceStr = "3199,2799,1199,5099,3299,1599,749,1999,1499,1299,2599,2799";
        int length = nameStr.split(",").length;
        List<Boolean> resultList = new ArrayList<>();
        for (int i =0 ; i < length;i++){
            Product product = new Product();
            product.setId(Long.valueOf(String.valueOf(i+1)));
            product.setName(nameStr.split(",")[i]);
            product.setSubTitle(descStr.split(",")[i]);
            product.setPrice(new BigDecimal(priceStr.split(",")[i]));
            resultList.add(esUtil.addIndex("goods",product,String.valueOf(product.getId())));
        }
        //异步保存
//        esClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
//            @Override
//            public void onResponse(IndexResponse indexResponse) {
//                log.info("total: " + indexResponse.getShardInfo().getTotal());
//                log.info("success: " + indexResponse.getShardInfo().getSuccessful());
//                log.info("failed: " + indexResponse.getShardInfo().getFailed());
//            }
//            @Override
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        });
        return resultList;
    }

    /**
     *  商品(索引)更新
     *  注:ES更新文档的顺序是：先检索到文档、将原来的文档标记为删除、创建新文档、删除旧文档，创建新文档就会重建索引
     * @return
     * @throws IOException
     */
    @GetMapping("/update")
    public Object update() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("product", "product",
                "1");
        Product product = new Product();
        product.setId(1L);
        product.setName("华为手机P30");
        product.setSubTitle("音乐手机拍照手机高端智能机超长待机旗舰系列手机");
        product.setPrice(new BigDecimal("5699"));
        return esUtil.updateIndex("product", product, "1");
    }

    /**
     * 删除文档对象
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping("/del/{id}")
    public Object del(@PathVariable("id") String id) throws IOException {
        return esUtil.deleteIndex("product",id);
    }

    /**
     * 根据id获取单个文档对象
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping("/get/{id}")
    public Object getOne(@PathVariable("id") String id) throws IOException {
        return esUtil.getIndex("product",id,Product.class);
    }

    /**
     * 分页查询
     * @return
     * @throws IOException
     */
    @GetMapping("/findPage")
    public Object findPage(int page,int pageSize) throws IOException {
        return esUtil.findPage("product", Product.class, searchSourceBuilder -> {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            //source源字段过虑,指定结果中所包括的字段有哪些
//            searchSourceBuilder.fetchSource(new String[]{"name","price"}, new String[]{});
            //单字段查询
//            searchSourceBuilder.query(QueryBuilders.termQuery("name",name));
            //idList 多id查询
//        searchSourceBuilder.query(QueryBuilders.termQuery("_id",idList));
            //匹配关键字 Operator.OR 表示 或 条件  即任意分词匹配即可
//        searchSourceBuilder.query(QueryBuilders.matchQuery("describe", "音乐拍照手机").operator(Operator.OR));
            //也可以设置匹配比 .minimumShouldMatch("80%"); 表示，三个词在文档的匹配占比为80%，即3*0.8=2.4，向上取整得2，表示至少有两个词在文档中要匹配成功
//        searchSourceBuilder.query(QueryBuilders.matchQuery("describe", "音乐拍照手机").minimumShouldMatch("80%"));

            //多字段查询
//            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(text,
//                    "name", "describe")
//                    .minimumShouldMatch("50%");
//            multiMatchQueryBuilder.field("name",10);//提升boost
//            searchSourceBuilder.query(multiMatchQueryBuilder);
            //布尔查询
            // must：表示必须，多个查询条件必须都满足。（通常使用must）
            // should：表示或者，多个查询条件只要有一个满足即可。
            // must_not：表示非。
//            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            //范围过虑，保留大于等于60 并且小于等于100的记录。
//            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));
//            searchSourceBuilder.query(boolQueryBuilder);
           //排序
            searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.DESC));
        },page,pageSize);
    }
    /**
     * 高亮查询
     * @return
     * @throws IOException
     */
    @GetMapping("/highlightQuery")
    public Object highlightQuery(String text) throws IOException {
        SearchRequest searchRequest = new SearchRequest("product");
        searchRequest.types("product");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //排序
        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.ASC));
        searchSourceBuilder.query(boolQueryBuilder);
        //高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>");//设置前缀
        highlightBuilder.postTags("</tag>");//设置后缀
        // 设置高亮字段
        highlightBuilder.fields().add(new HighlightBuilder.Field("describe"));
        // highlightBuilder.fields().add(new HighlightBuilder.Field("description"));
        searchSourceBuilder.highlighter(highlightBuilder);
        //source源字段过虑,指定结果中所包括的字段有哪些
        searchSourceBuilder.fetchSource(new String[]{"name","describe","price"}, new String[]{});
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = getSearchResponse(searchRequest);

        for (SearchHit hit : response.getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //名称
            String name = (String) sourceAsMap.get("name");
            //取出高亮字段内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(highlightFields!=null){
                HighlightField nameField = highlightFields.get("name");
                if(nameField!=null){
                    Text[] fragments = nameField.getFragments();
                    StringBuffer stringBuffer = new StringBuffer();
                    for (Text str : fragments) {
                        stringBuffer.append(str.string());
                    }
                    name = stringBuffer.toString();
                }
                System.out.println("高亮 = " +name);
            }
        }
        return response;
    }

    private SearchResponse getSearchResponse(SearchRequest searchRequest) throws IOException {
//        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchResponse response = null;
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
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();
            String sourceAsString = hit.getSourceAsString();
//            Product product = JsonUtils.toObject(hit.getSourceAsString(),Product.class);
            System.out.println(hit.getSourceAsString());
        }
        return response;
    }

}
