package com.td.recommend.recall.hotvideo.recommender;

import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import com.td.recommend.recall.hotvideo.utils.Shuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * create by zjl at 2019/02/06
 */

@Repository
public class BlastRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(BlastRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("blast.request.qps").mark();
        List<VideoDoc> videoDocs = new ArrayList<>();
        String key = "blast_vids";
        if (recommendContext.getType().endsWith("_rlvt")) {
            key = "rlvt_blast_vids";
        }
        try {
            List<String> list = RedisClientSingleton.general.lrange(key, 0, recommendContext.getNum());
            List<String> valueList = Shuffle.listShuffle(list);
            int size = Math.min((valueList.size()), recommendContext.getNum());
            for (int i = 0; i < size; i++) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = valueList.get(i);
                double score = 1.0;
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("blast recall failed ", e);
        }
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("blast-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("blast.recall.failrate").update(100);
        }
        return videoDocs;
    }
}
