package com.td.recommend.recall.hotvideo.recommender;

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
public class SimUidsRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(SimUidsRecommender.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        List<VideoDoc> docs = new ArrayList<>();

        String uid = recommendContext.getKey();
        String prefix = "vuser_";
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
            LOG.error("recall failed with id={}", uid, e);
        }
        return docs;
    }

}