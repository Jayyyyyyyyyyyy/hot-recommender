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
 * Created by zjl on 2019/6/14.
 */
@Repository
public class BertRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(BertRecommender.class);

    Config bertServer = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
    String bertUrl = bertServer.getString("bert.i2i.url.32");

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("bert-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("bert-video.request.latency").time();
        ResDoc resDoc = null;

        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String url = bertUrl + "key=" + recommendContext.getKey() + "&num=" + recommendContext.getNum();
            resDoc = httpClient.request(url, ResDoc.class);
        } catch (Exception e) {
            LOG.error("BertRecommender recall failed with id={}", recommendContext.getKey(), e);
        }

        time.stop();

        if (resDoc != null && resDoc.getStatus() == 0 && resDoc.getData() != null) {
            taggedMetricRegistry.histogram("bert-video.recall.failrate").update(0);
            return resDoc.getData();
        } else {
            taggedMetricRegistry.histogram("bert-video.recall.failrate").update(100);
            return Collections.emptyList();
        }
    }
}