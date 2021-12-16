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

public class RecomeRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(RecomeRecommender.class);

    @Override
    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("recome.recall.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("recome.recall.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            List<String> itemList = TagidReverseIndexes.getRecome().getOrDefault(recommendContext.getKey(), Collections.emptyList());

            for (String item : itemList) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = item.split(":")[0];
                double score = Double.parseDouble(item.split(":")[1]);
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("recome recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("recome.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("recome.recall.failrate").update(100);
        }
        return videoDocs;
    }
}
