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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by zjl on 2019/6/14.
 */
@Repository
public class OriginRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(OriginRecommender.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("origin-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("origin-video.request.latency").time();
        List<VideoDoc> docs = new ArrayList<>();

        try {
            RedisClientSingleton instance = RedisClientSingleton.origin;
            String id = instance.get(recommendContext.getKey());
            if(id != null) {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(id);
                docs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("origin recall failed with id={}", recommendContext.getKey(), e);
        }
        time.stop();
        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("origin-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("origin-video.recall.failrate").update(100);
        }
        return docs;
    }
}