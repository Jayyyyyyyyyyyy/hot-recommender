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
 * create by pansm at 2019/08/30
 */

@Repository
public class NewHotMp3Recommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(NewHotMp3Recommender.class);

    private static final String NEW_HOT_MP3_KEY="hotmp3top20vid";

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("newhotmp3.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("newhotmp3.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();

        try {
            RedisClientSingleton rediscli = RedisClientSingleton.general;
            List<String> valuelist = rediscli.lrange(NEW_HOT_MP3_KEY, 0, -1);

            if (valuelist==null || valuelist.isEmpty()) {
                LOG.error("key:{} ,list is error.",NEW_HOT_MP3_KEY);
                return new ArrayList<>();
            }
            if (valuelist.size()<2) {
                return new ArrayList<>();
            }
            for (int i=0; i<Math.min((valuelist.size()),recommendContext.getNum()); i++) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = valuelist.get(i);
                double score = 1.0;
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }

        } catch (Exception e) {
            LOG.error("new_hot_mp3 recall failed "+e.toString(), e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("new_hot_mp3-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("newhotmp3.recall.failrate").update(100);
        }
        return videoDocs;
    }
}
