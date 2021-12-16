package com.td.recommend.recall.hotvideo.ranker;

import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;

/**
 * Created by:liujikun
 * Date: 2019/8/18
 */
public interface RecallRanker {
    void rank(PredictItems<DocItem> predictItems, RecommendContext recommendContext);
}