package com.jbf.es.dao;

import com.jbf.es.model.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.Collection;
import java.util.List;

public interface ItemRepository extends ElasticsearchRepository<Item,Long> {
    /**
     * @Description:根据价格区间查询  自定义查询
     * @Param price1
     * @Param price2
     */
    List<Item> findByPriceBetween(double price1, double price2);

    List<Item> findByTitle(String title1);

    List<Item> findByTitleIn(Collection<String> ss);
}

