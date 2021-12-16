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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zjl on 2019/8/8.
 */
@Repository
public class TeachingRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(TeachingRecommender.class);

    private Map<String,List<VideoDoc>> teachList = new HashMap<>();

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("teaching-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("teaching-video.request.latency").time();

        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            RedisClientSingleton redis = RedisClientSingleton.popular;
            String key = "r_zero_quality";//舞蹈教学视频
            if (teachList.containsKey(key)) {
                return teachList.get(key);
            }
            List<String> vids = redis.lrange(key, 0, recommendContext.getNum()-1);
            vids.forEach(vid -> {
                        VideoDoc videoDoc = new VideoDoc();
                        videoDoc.setId(vid);
                        videoDocs.add(videoDoc);
                    });
            teachList.put(key,videoDocs);
        } catch (Exception e) {
            LOG.error("teaching recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("teaching-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("teaching-video.recall.failrate").update(100);
        }
        return videoDocs;
    }


    public static void main(String[] args) {
        RecommendContext recommendContext = new RecommendContext();
        recommendContext.setBucket("80000");
        recommendContext.setNum(50);
        List<VideoDoc> recommend = new TeachingRecommender().recommend(recommendContext);
        recommend.forEach(doc -> System.out.println(doc.getId()));
    }
}
