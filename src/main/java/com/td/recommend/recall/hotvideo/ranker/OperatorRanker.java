package com.td.recommend.recall.hotvideo.ranker;

import com.td.data.profile.TDynamicDocDataNew;
import com.td.data.profile.TDynamicDocRaw;
import com.td.data.profile.client.ItemProfileClient;
import com.td.recommend.commons.item.PredictItem;
import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;

public class OperatorRanker implements RecallRanker {
    @Override
    public void rank(PredictItems<DocItem> predictItems, RecommendContext recommendContext) {
        double click, view, dcCtr, score;
        for (PredictItem<DocItem> item : predictItems) {
            try {
                TDynamicDocDataNew doc = item.getItem().getNewsDocumentData().get().getDynamicDocumentDataNew().get();
                TDynamicDocRaw dynamicDocumentData = doc.rawMap.get(ItemProfileClient.ScopeEnum.RECOM_FEED.getName());
                click = dynamicDocumentData.getDcClickNum();
                view = dynamicDocumentData.getDcShowNum();
            } catch (Exception e) {
                click = 10;
                view = 100;
            }
            try {
                dcCtr = click/view;
            }catch (Exception e){
                dcCtr = 0.1;
            }
            if(click > view){
                dcCtr = 0.1;
            }
            item.setScore(dcCtr);
        }
        predictItems.sort();
    }
}
