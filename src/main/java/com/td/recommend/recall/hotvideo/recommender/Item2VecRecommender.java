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

import java.util.Collections;
import java.util.List;

public class Item2VecRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(GemRecommender.class);
    static String item2vecUrl;

    static {
        Config graphEmbeddingServer = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        item2vecUrl = graphEmbeddingServer.getString("item2vec.i2i.url");
    }

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("item2vec.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("item2vec.request.latency").time();
        ResDoc resDoc = null;

        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String url = item2vecUrl + "key=" + recommendContext.getKey() + "&num=" + recommendContext.getNum();
            resDoc = httpClient.request(url, ResDoc.class);
        } catch (Exception e) {
            LOG.error("GemRecommender recall failed with id={}", recommendContext.getKey(), e);
        }

        time.stop();

        if (resDoc != null && resDoc.getStatus() == 0 && resDoc.getData() != null) {
            taggedMetricRegistry.histogram("item2vec.recall.failrate").update(0);
            return resDoc.getData();
        } else {
            taggedMetricRegistry.histogram("item2vec.recall.failrate").update(100);
            return Collections.emptyList();
        }
    }

}