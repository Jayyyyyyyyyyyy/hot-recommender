package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.ResDoc;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.HttpClientSingleton;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class FollowWorksRecommender implements IRecommender {


    private TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance().getTaggedMetricRegistry();

    private static final Logger LOG = LoggerFactory.getLogger(FollowWorksRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        taggedMetricRegistry.meter("followwatch.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("followwatch.request.latency").time();
        List<VideoDoc> docs = new ArrayList<>();
        String uid = recommendContext.getKey();
        String prefix = "tfollowworks_";

        try {
            RedisClientSingleton instance = RedisClientSingleton.cf;
            String key = prefix + uid;
            List<String> topItems = instance.lrange(key, 0, recommendContext.getNum());
            for (String id : topItems) {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(id);
                docs.add(videoDoc);
            }
        } catch (Exception e) {
                LOG.error("tfollowworks recall failed with diu={}", uid, e);
            }

        time.stop();
        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("followworks-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("followworks-video.recall.failrate").update(100);
        }
        LOG.info("return docs size is {}",docs.size());
        return docs;
    }
}
