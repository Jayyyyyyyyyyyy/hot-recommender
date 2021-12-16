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
 * Created by liujikun on 2019/7/20.
 */
@Repository
public class TopRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(TopRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("hot-top.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("hot-top.request.latency").time();

        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            String key = "top:" + recommendContext.getKey();
            RedisClientSingleton redis = RedisClientSingleton.general;
            List<String> vids = redis.lrange(key, 0, recommendContext.getNum());
            vids.forEach(vid -> {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(vid);
                videoDocs.add(videoDoc);
            });
        } catch (Exception e) {
            LOG.error("group top recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("hot-top.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("hot-top.recall.failrate").update(100);
        }
        return videoDocs;
    }


    public static void main(String[] args) {
        RecommendContext recommendContext = new RecommendContext();
        recommendContext.setKey("1007");
        recommendContext.setNum(50);
        List<VideoDoc> recommend = new TopRecommender().recommend(recommendContext);
        recommend.forEach(doc -> System.out.println(doc.getId()));
    }
}
