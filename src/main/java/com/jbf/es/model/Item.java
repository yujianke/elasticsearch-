package com.jbf.es.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "item", shards = 1, replicas = 0,createIndex = false)
@Data
public class Item {
    @Id
    private Long id;
    //文章使用分词器
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String title; //标题
    @Field(type = FieldType.Keyword)
    private String category;// 分类
    @Field(type = FieldType.Keyword)
    private String brand; // 品牌
    @Field(type = FieldType.Double)
    private Double price; // 价格

}

