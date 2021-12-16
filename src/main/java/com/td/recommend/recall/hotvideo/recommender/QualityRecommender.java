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
public class QualityRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(TeachingRecommender.class);
    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter(recommendContext.getType()+"-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer(recommendContext.getType()+"-video.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        String type = recommendContext.getType();
        String key = "quality_fitness_vid";
        if("vquality_classic".equals(type)){
            key = "quality_classical_vid";
        }
        try {
            RedisClientSingleton instance = RedisClientSingleton.search;
            List<String> topItems = instance.lrange(key, 0, recommendContext.getNum());
            for (String id : topItems) {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(id);
                videoDocs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error(recommendContext.getType()+" recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram(recommendContext.getType()+"-video.recall.no-result.rate").update(0);
        } else {
            taggedMetricRegistry.histogram(recommendContext.getType()+"-video.recall.no-result.rate").update(100);
        }
        return videoDocs;
    }

}
