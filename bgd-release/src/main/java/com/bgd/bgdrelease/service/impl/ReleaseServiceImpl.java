package com.bgd.bgdrelease.service.impl;

import com.bgd.bgdrelease.service.ReleaseService;
import com.bgd.common.constant.MoveConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ReleaseServiceImpl implements ReleaseService {

    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        String query="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \"2019-06-03\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        //将上边的query用代码的格式封装起来
        //1.首先构造SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //3.在构造bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //4.filter，TermQueryBuilder
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        //2.根据上边的第一层结构为query，所以构造query
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());
        //5.将整个构造好的格式传入new Search.Builder中
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(MoveConstant.ES_INDEX_DAU).addType("_doc").build();
        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
              total = searchResult.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }
    //这个也是同样的如此
    @Override
    public Map getDauHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(MoveConstant.ES_INDEX_DAU).addType("_doc").build();

        Map dauHourMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Long count = bucket.getCount();
                dauHourMap.put(key,count);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return dauHourMap;
    }

}
