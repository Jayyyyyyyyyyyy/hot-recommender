package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zjl on 2019/6/14.
 */
@Repository
public class SearchRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(SearchRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("search-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("search-video.request.latency").time();
        List<VideoDoc> docs = new ArrayList<>();
        try {
            RedisClientSingleton instance = RedisClientSingleton.search;
//            String key = "searchrecall||" + diu;
            String key = "ufr_search_rec||" + recommendContext.getKey();
            List<String> topItems = instance.lrange(key, 0, recommendContext.getNum());
            for (String id : topItems) {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(id);
                docs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("search-video recall failed with id={}", recommendContext.getKey(), e);
        }

        time.stop();

        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("search-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("search-video.recall.failrate").update(100);
        }
        return docs;
    }
}