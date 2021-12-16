package com.td.recommend.recall.hotvideo.recommender;

import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.ResDoc;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.HttpClientSingleton;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

@Repository
public class SplitFlowRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(SplitFlowRecommender.class);
    static String url;

    static {
        Config server = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        url = server.getString("splitflow.u2i.url");
    }

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        ResDoc resDoc = null;

        String key = recommendContext.getKey();
        int num = recommendContext.getNum();
        String url = SplitFlowRecommender.url + "diu=" + key + "&num=" + num;

        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            resDoc = httpClient.request(url, ResDoc.class);
        } catch (Exception e) {
            LOG.error("recall failed with context={}", recommendContext, e);
        }

        if (resDoc != null && resDoc.getStatus() == 0 && resDoc.getData() != null) {
            return resDoc.getData();
        } else {
            return Collections.emptyList();
        }
    }

}
