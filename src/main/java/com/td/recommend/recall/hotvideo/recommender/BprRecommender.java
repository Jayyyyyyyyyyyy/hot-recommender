package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
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

/**
 * Created by zjl on 2019/7/4.
 */
@Repository
public class BprRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(BprRecommender.class);
    static String brpUrl;

    static {
        Config bprServer = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        brpUrl = bprServer.getString("bpr.u2i.url");
    }

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("bpr-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("bpr-video.request.latency").time();
        ResDoc resDoc = null;

        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String url = brpUrl + "key=" + recommendContext.getKey() + "&num=" + recommendContext.getNum();
            resDoc = httpClient.request(url, ResDoc.class);
        } catch (Exception e) {
            LOG.error("BprRecommender recall failed with id={}", recommendContext.getKey(), e);
        }

        time.stop();

        if (resDoc != null && resDoc.getStatus() == 0 && resDoc.getData() != null) {
            taggedMetricRegistry.histogram("bpr-video.recall.failrate").update(0);
            return resDoc.getData();
        } else {
            taggedMetricRegistry.histogram("bpr-video.recall.failrate").update(100);
            return Collections.emptyList();
        }
    }

}
