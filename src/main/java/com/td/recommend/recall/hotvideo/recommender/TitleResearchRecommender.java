package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.TagidReverseIndexes;
import com.td.recommend.recall.hotvideo.utils.Shuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * create by zjl at 2020/08/05
 */

@Repository
public class TitleResearchRecommender implements IRecommender{
    private static final Logger LOG = LoggerFactory.getLogger(TitleResearchRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("titleresearch.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("titleresearch.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            List<String> list = TagidReverseIndexes.getTitleResearch().get(recommendContext.getKey());
            if(list==null || list.size()<1){
                return Collections.emptyList();
            }
            List<String> valueList = Shuffle.listShuffle(list);
            int size  = Math.min((valueList.size()), recommendContext.getNum());
            for (int i = 0; i < size; i++) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = valueList.get(i);
                double score = 1.0;
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }

        } catch (Exception e) {
            LOG.error("titleresearch recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("titleresearch.recall.no-result.rate").update(0);
        } else {
            taggedMetricRegistry.histogram("titleresearch.recall.no-result.rate").update(100);
        }
        return videoDocs;
    }

}
