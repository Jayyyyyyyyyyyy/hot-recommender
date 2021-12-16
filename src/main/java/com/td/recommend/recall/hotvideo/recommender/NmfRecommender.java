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
public class NmfRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(NmfRecommender.class);
    static String url;

    static {
        Config conf = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        url = conf.getString("nmfv2.dnn.url");
    }

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("nmf-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("nmf.request.latency").time();
        ResDoc resDoc = null;
        String key = recommendContext.getKey();

        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String requestUrl = url + "key=" + key + "&num=" + recommendContext.getNum();
            resDoc = httpClient.request(requestUrl, ResDoc.class);
        } catch (Exception e) {
            LOG.error(" recall failed with id={}", key, e);
        }

        time.stop();

        if (resDoc != null && resDoc.getStatus() == 0 && resDoc.getData() != null) {
            taggedMetricRegistry.histogram("nmfv.recall.failrate").update(0);
            return resDoc.getData();
        } else {
            taggedMetricRegistry.histogram("nmfv.recall.failrate").update(100);
            return Collections.emptyList();
        }
    }
}
