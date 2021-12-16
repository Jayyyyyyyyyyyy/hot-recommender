package com.td.recommend.recall.hotvideo.datasource;

import com.td.recommend.recall.hotvideo.utils.ESClientSingleton;
import lombok.Getter;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TagidReverseIndexes {
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    @Getter
    public static Map<String, List<String>> titleResearch = new ConcurrentHashMap<>();
    @Getter
    public static Map<String, List<String>> operatorHot = new ConcurrentHashMap<>();
    @Getter
    public static Map<String, List<String>> operatorInterest = new ConcurrentHashMap<>();
    @Getter
    public static Map<String, List<String>> recome = new ConcurrentHashMap<>();
    private static final Logger log = LoggerFactory.getLogger(TagidReverseIndexes.class);
    private static final String titleKey = "titleresearch:vid";
    private static final String hotKey = "operator_hot:vid";
    private static final String operatorInterestKey = "operator_interest:vid";
    private static final String recomeKey = "recome_vids";
    private static final String[] source = {"firstcat.tagid", "secondcat.tagid", "content_tag.tagid", "content_phrase.tagid"};
    private static final RedisClientSingleton redis = RedisClientSingleton.general;

    static {
        scheduledExecutorService.scheduleAtFixedRate(TagidReverseIndexes::loadData, 0, 1, TimeUnit.MINUTES);
    }

    private static void loadData() {
        //redis中的元素可以是一个vid或者vid:score格式
        buildReverseIndex(redis.smembers(titleKey), titleResearch);//title research
        buildReverseIndex(redis.lrange(hotKey, 0, -1), operatorHot);//运营hot
        buildReverseIndex(redis.lrange(operatorInterestKey, 0, -1), operatorInterest);//阿米巴
        buildReverseIndex(redis.lrange(recomeKey, 0, -1), recome);//看了vid第二天还来
    }

    private static void buildReverseIndex(Collection<String> items, Map<String, List<String>> reverseIndex) {
        SearchResponse search;
        try {
            List<String> vids = items.stream().map(i -> i.split(":")[0]).collect(Collectors.toList());
            search = ESClientSingleton.getInstance().getClient()
                    .search(buildQuery(vids), RequestOptions.DEFAULT);
            Map<String, Map<String, Object>> sourceMap = Arrays.stream(search.getHits().getHits()).collect(Collectors.toMap(SearchHit::getId, SearchHit::getSourceAsMap));

            reverseIndex.clear();

            for (String item : items) {
                String vid = item.split(":")[0];
                Map<String, Object> source = sourceMap.get(vid);
                if (source != null) {
                    buildFlatTagId(item, source, reverseIndex, "firstcat");
                    buildFlatTagId(item, source, reverseIndex, "secondcat");
                    buildNestedTagId(item, source, reverseIndex, "content_tag");
                    buildNestedTagId(item, source, reverseIndex, "content_phrase");
                }
            }
        } catch (IOException e) {
            log.info("build reverse index failed", e);
        }
    }

    private static void buildFlatTagId(String item, Map<String, Object> source, Map<String, List<String>> reverseIndex, String tagName) {
        if (source.containsKey(tagName)){
            Map<String, String> tag = (Map<String, String>) source.get(tagName);
            String tagId = tag.get("tagid");
            reverseIndex.putIfAbsent(tagId, new ArrayList<>());
            reverseIndex.get(tagId).add(item);
        }
    }

    private static void buildNestedTagId(String item, Map<String, Object> source, Map<String, List<String>> reverseIndex, String tagName) {
        if (source.containsKey(tagName)) {
            List<HashMap<String, String>> tagList = (List<HashMap<String, String>>) source.get(tagName);
            for (HashMap<String, String> stringStringHashMap : tagList) {
                String tagId = stringStringHashMap.get("tagid");
                reverseIndex.putIfAbsent(tagId, new ArrayList<>());
                reverseIndex.get(tagId).add(item);
            }
        }
    }

    public static SearchRequest buildQuery(Collection<String> vidSet) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.termsQuery("_id", vidSet));
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.fetchSource(source, null);
        searchSourceBuilder.size(vidSet.size());
        SearchRequest searchRequest = new SearchRequest("portrait_video").types("video");
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(10000);
        System.out.println(TagidReverseIndexes.getOperatorHot());
    }
}
