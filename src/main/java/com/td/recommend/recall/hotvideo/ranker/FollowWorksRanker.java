package com.td.recommend.recall.hotvideo.ranker;

import com.td.recommend.commons.item.PredictItem;
import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.commons.profile.DocProfileUtils;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.recommender.UserCfRecommender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowWorksRanker implements RecallRanker {
    //private static final Logger LOG = LoggerFactory.getLogger(FollowWorksRanker.class);

    @Override
    public void rank(PredictItems<DocItem> predictItems, RecommendContext recommendContext) {
        for (PredictItem<DocItem> item : predictItems) {
            double ctr = DocProfileUtils.getFeedDcCtr(item.getItem());
            double view = DocProfileUtils.getFeedDcView(item.getItem());
            double score = 0.0;
            if (view>= 10.0){
                 score = ctr;
            }
            item.setScore(score);
        }
        predictItems.sort();
        //String abc = predictItems.getItems().stream().map(item -> item.getId()).toArray().toString();
    }
}
