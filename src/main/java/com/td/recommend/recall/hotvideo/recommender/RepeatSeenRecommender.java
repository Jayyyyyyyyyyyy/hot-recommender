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
import java.util.Collections;
import java.util.List;

/**
 * create by zjl at 2020/05/26
 */

@Repository
public class RepeatSeenRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(RepeatSeenRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("repeatwatch-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("repeatwatch-video.request.latency").time();
        List<VideoDoc> docs = new ArrayList<>();
        String REPEATWATCH_KEY;
        if (recommendContext.getType().equals("vrepeatv1_seen")) {
            REPEATWATCH_KEY = "repeat_watch||";
        } else if (recommendContext.getType().equals("vrepeatv2_seen")) {
            REPEATWATCH_KEY = "repeatwatchv2_vid||";
        } else {
            REPEATWATCH_KEY = "repeatwatch_vid||";
        }
        String diu = recommendContext.getKey();
        try {
            RedisClientSingleton instance = RedisClientSingleton.search;
            String key = REPEATWATCH_KEY + diu;
            List<String> list = instance.lrange(key, 0, recommendContext.getNum());
            if (REPEATWATCH_KEY.equals("repeatwatch_vid||")) {
                Collections.shuffle(list);
            }
            for (String id : list) {
                VideoDoc videoDoc = new VideoDoc();
                double score = 1.0;
                videoDoc.setId(id);
                videoDoc.setScore(score);
                docs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("repeatwatch-video recall failed with id={}", diu, e);
        }

        time.stop();

        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("repeatwatch-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("repeatwatch-video.recall.failrate").update(100);
        }
        return docs;
    }

}
