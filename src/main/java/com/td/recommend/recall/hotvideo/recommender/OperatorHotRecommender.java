package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.KeyIdListOp;
import com.td.recommend.recall.hotvideo.datasource.TagidReverseIndexes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OperatorHotRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorHotRecommender.class);

    private static final String NEW_USER_KEY="operator_hot:vid";


    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("operator_hot.recall.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("operator_hot.recall.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        String type = recommendContext.getType();
        try {

            List<String> valuelist;
            if (type.equals("voperator_hot")) {
                valuelist = TagidReverseIndexes.getOperatorHot().get(recommendContext.getKey());
            } else {
                valuelist = KeyIdListOp.getKeyValuesList().get(type);
                Collections.shuffle(valuelist);
            }

            if (valuelist==null || valuelist.isEmpty()) {
                LOG.error("operator_hot recall key:{} ,list is error, type:{}",NEW_USER_KEY,type);
                return new ArrayList<>();
            }
            if (valuelist.size() > recommendContext.getNum()){
                Collections.shuffle(valuelist);
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
            LOG.error("operator_hot recall failed "+e.toString(), e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("operator_hot-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("operator_hot.recall.failrate").update(100);
        }
        return videoDocs;
    }
}
