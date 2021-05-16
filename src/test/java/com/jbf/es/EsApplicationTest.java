package com.jbf.es;

import com.alibaba.fastjson.JSON;
import com.jbf.es.model.Item;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.json.GsonJsonParser;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EsApplication.class)
public class EsApplicationTest {
    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;
    @Resource
    private RestHighLevelClient client;

    /**
     * @Description:创建索引，会根据Item类的@Document注解信息来创建
     */
    @Test
    public void testCreateIndex() {
        elasticsearchRestTemplate.indexOps(Item.class).create();
    }

    /**
     * @Description:删除索引
     */
    @Test
    public void testDeleteIndex() {
        elasticsearchRestTemplate.indexOps(Item.class).delete();
    }
    /**
     * @Description:判断索引是否存在
     */
    @Test
    public void testExistIndex() {

        System.out.println(elasticsearchRestTemplate.indexOps(Item.class).exists());
    }
    /**
     * @Description:删除
     */
    @Test
    public void delete() {
        elasticsearchRestTemplate.delete(String.valueOf(1L), Item.class);
    }
    /**
     * 查询文档数据
     */
    @Test
    public void getDoc() {
        Item item = elasticsearchRestTemplate.get(String.valueOf(1L), Item.class);
        System.out.println(item);
    }
    /**
     * 新增文档数据
     */
    @Test
    public void addDoc() throws IOException {
        Item po = new Item();
        po.setTitle("测试新增1");
        po.setCategory("品牌");
        po.setBrand("中国");
        po.setPrice(221.0);
        IndexRequest request = new IndexRequest("item");
//        // 指定id，如果不指定，则自动生成
//        request.id("1");
        // 请求超时
        request.timeout(TimeValue.timeValueSeconds(3));
        //request.timeout("1s");
        //  数据放入请求
        request.source(JSON.toJSONString(po), XContentType.JSON);
        // 客户端发送请求
        IndexResponse indices = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indices.toString());

    }
    /**
     * 修改文档数据
     */
    @Test
    public void updateDoc() {

        Map<String, Object> map = new HashMap<>();
        map.put("title", "aaa");
        map.put("category", "品牌");
        map.put("brand", "中国");
        map.put("price", 21.0);
        Document doc = Document.from(map);
        UpdateQuery updateQuery = UpdateQuery
                .builder(String.valueOf(2))
                .withDocument(doc)
                .build();
        IndexCoordinates indexCoordinates = IndexCoordinates.of("item");
        elasticsearchRestTemplate.update(updateQuery, indexCoordinates);
    }

    /**
     * 分页搜索数据
     * 使用QueryBuilder
     * termQuery("key", obj) 完全匹配
     * termsQuery("key", obj1, obj2..)   一次匹配多个值
     * matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
     * multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
     * matchAllQuery();         匹配所有文件
     * idsQuery();         只查询一个id的
     * fuzzyQuery();          模糊查询 不能用通配符, 找到相似的
     */
    @Test
    public void search() {
        Pageable pageable = PageRequest.of(0, 10);

        SortBuilder<FieldSortBuilder> sortBuilder = new FieldSortBuilder("price")
                .order(SortOrder.DESC);

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.boolQuery().should(QueryBuilders.fuzzyQuery("title", "abc")))
                .withPageable(pageable)
                .withSort(sortBuilder)
                .build();
        SearchHits<Item> search = elasticsearchRestTemplate.search(query, Item.class);
        System.out.println(search.getSearchHits());
    }

    /**
     * 高亮搜索
     */
    @Test
    public void highlight() {
        String preTag = "<font color='red'>";
        String postTag = "</font>";

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchQuery("title", "abc"))
                .withHighlightFields(new HighlightBuilder.Field("title")
                        .preTags(preTag)
                        .postTags(postTag))
                .build();
        SearchHits<Item> searchHits = elasticsearchRestTemplate.search(query, Item.class);

        List<SearchHit<Item>> searchHitList = searchHits.getSearchHits();
        List<Map<String, Object>> hlList = new ArrayList<>();
        for (SearchHit h : searchHitList) {

            List<String> highlightField = h.getHighlightField("title");
            String nameValue = highlightField.get(0);
            String originalJson = JSON.toJSONString(h.getContent());
            JsonParser jj = new GsonJsonParser();
            Map<String, Object> myHighLight = jj.parseMap(originalJson);
            // 用高亮的搜索结果覆盖原字段值
            myHighLight.put("title", nameValue);
            System.out.println(myHighLight);

            hlList.add(myHighLight);
        }
        System.out.println(hlList);
    }
    /**
     * 高亮搜索 排序加分页
     */
    @Test
    public void highlight1() {
        String preTag = "<font color='red'>";
        String postTag = "</font>";
        Pageable pageable = PageRequest.of(0, 10);
        SortBuilder<FieldSortBuilder> sortBuilder = new FieldSortBuilder("price")
                .order(SortOrder.DESC);
        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.fuzzyQuery("title", "abc"))
                .withPageable(pageable)
                .withSort(sortBuilder)
                .withHighlightFields(new HighlightBuilder.Field("title")
                        .preTags(preTag)
                        .postTags(postTag))
                .build();
        SearchHits<Item> searchHits = elasticsearchRestTemplate.search(query, Item.class);

        List<SearchHit<Item>> searchHitList = searchHits.getSearchHits();
        List<Map<String, Object>> hlList = new ArrayList<>();
        for (SearchHit h : searchHitList) {

            List<String> highlightField = h.getHighlightField("title");
            String nameValue = highlightField.get(0);
            String originalJson = JSON.toJSONString(h.getContent());
            JsonParser jj = new GsonJsonParser();
            Map<String, Object> myHighLight = jj.parseMap(originalJson);
            // 用高亮的搜索结果覆盖原字段值
            myHighLight.put("title", nameValue);
            hlList.add(myHighLight);
        }
        System.out.println(hlList);
    }
}
