package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.TagidReverseIndexes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OperatorInterestRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorInterestRecommender.class);

    @Override
    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("operator_interest.recall.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("operator_interest.recall.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            List<String> vidList = TagidReverseIndexes.getOperatorInterest().getOrDefault(recommendContext.getKey(), Collections.emptyList());
            Collections.shuffle(vidList);
            for (int i=0; i<Math.min((vidList.size()),recommendContext.getNum()); i++) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = vidList.get(i);
                double score = 1.0;
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("operator_interest recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("operator_interest-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("operator_interest.recall.failrate").update(100);
        }
        return videoDocs;
    }
}
