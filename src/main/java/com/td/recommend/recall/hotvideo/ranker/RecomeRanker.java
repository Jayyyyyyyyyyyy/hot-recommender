package com.td.recommend.recall.hotvideo.ranker;

import com.td.recommend.commons.item.PredictItem;
import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.commons.profile.DocProfileUtils;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;

public class RecomeRanker implements RecallRanker {
    @Override
    public void rank(PredictItems<DocItem> predictItems, RecommendContext recommendContext) {
        for (PredictItem<DocItem> item : predictItems) {
            double ctr = DocProfileUtils.getFeedDcCtr(item.getItem());
            double score = item.getScore() * ctr;
            item.setScore(score);
        }
        predictItems.sort();
    }
}
