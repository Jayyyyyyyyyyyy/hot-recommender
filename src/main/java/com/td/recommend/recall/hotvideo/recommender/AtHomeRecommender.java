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
 * create by Liujikun at 2019/02/03
 */

@Repository
public class AtHomeRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(AtHomeRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("athome.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("athome.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();

        try {
            List<String> valueList = RedisClientSingleton.general.lrange("athome_vids", 0, recommendContext.getNum());
            for (int i = 0; i < Math.min((valueList.size()), recommendContext.getNum()); i++) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = valueList.get(i);
                double score = 1.0;
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }

        } catch (Exception e) {
            LOG.error("at home recall failed " + e.toString(), e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("athome-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("athome.recall.failrate").update(100);
        }
        return videoDocs;
    }


}
