package com.td.recommend.recall.hotvideo.ranker;

import com.td.data.profile.TDynamicDocDataNew;
import com.td.data.profile.TDynamicDocRaw;
import com.td.data.profile.client.ItemProfileClient;
import com.td.featurestore.feature.Feature;
import com.td.featurestore.feature.IFeatures;
import com.td.featurestore.feature.KeySortedFeatures;
import com.td.recommend.commons.item.PredictItem;
import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Liujikun on 2019/08/23.
 * Thompson Sampling Beta Distribution based ranker
 */

public class RecallBetaRanker implements RecallRanker {
    private static final Logger logger = LoggerFactory.getLogger(RecallBetaRanker.class);

    @Override
    public void rank(PredictItems<DocItem> predictItems, RecommendContext recommendContext) {
        for (PredictItem<DocItem> item : predictItems) {
            double retrieveClick, retrieveView, click, view, miss;
            String retrieverName = recommendContext.getType() + "_" + recommendContext.getKey();

            try {//召回点的曝光点击
                retrieveClick = click = item.getItem().getFeatures("retrieveClick").get().get(retrieverName).get().getValue();
                retrieveView = view = item.getItem().getFeatures("retrieveView").get().get(retrieverName).get().getValue();

            } catch (Exception e) {
                retrieveClick = click = 0;
                retrieveView = view = 0;
            }

            try {//feed decay的曝光点击
                if (click < 10) {
                    TDynamicDocDataNew doc = item.getItem().getNewsDocumentData().get().getDynamicDocumentDataNew().get();
                    TDynamicDocRaw dynamicDocumentData = doc.rawMap.get(ItemProfileClient.ScopeEnum.RECOM_FEED.getName());
                    click = dynamicDocumentData.getDcClickNum();
                    view = dynamicDocumentData.getDcShowNum();
                }
            } catch (Exception ignored) {
                click = view = 0;
            }

            miss = view - click < 0 ? 0 : view - click;

            BetaDistribution betaDistribution = new BetaDistribution(click + 1, miss + 1);
            double betaScore = betaDistribution.sample();
            item.getItem().addFeatures("rank", rankFeatures(retrieveClick, retrieveView, betaScore));
            item.setScore(betaScore);
        }
        predictItems.sort();
    }

    private IFeatures rankFeatures(double click, double view, double betaScore) {
        KeySortedFeatures rankFeatures = new KeySortedFeatures("rank");
        rankFeatures.add(new Feature("retrieveClick", click));
        rankFeatures.add(new Feature("retrieveView", view));
        rankFeatures.add(new Feature("retrieveCtr", (click / (view + 1))));
        rankFeatures.add(new Feature("score", betaScore));
        return rankFeatures;
    }
}
