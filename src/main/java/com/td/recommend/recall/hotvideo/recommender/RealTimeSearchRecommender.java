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
@Repository
public class RealTimeSearchRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(RealTimeSearchRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("realtimesearch-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("realtimesearch-video.request.latency").time();
        List<VideoDoc> docs = new ArrayList<>();
        String query = "";
        try {
            query = recommendContext.getKey();
            int num = recommendContext.getNum();
            RedisClientSingleton instance = RedisClientSingleton.search;
            String key = "realtimesearch||" + query;
            List<String> topItems = instance.lrange(key, 0, num);
            for (String id : topItems) {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(id);
                docs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("realtimesearch-video recall failed with id={}", query, e);
        }

        time.stop();

        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("realtimesearch-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("realtimesearch-video.recall.failrate").update(100);
        }
        return docs;
    }
}
