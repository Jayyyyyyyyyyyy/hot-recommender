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

@Repository
public class ClusterRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRecommender.class);
    static String url;

    static {
        Config server = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        url = server.getString("cluster.u2i.url");
    }

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("cluster-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("cluster-video.request.latency").time();
        ResDoc resDoc = null;

        String key = recommendContext.getKey();
        String bucket = recommendContext.getBucket();
        String url;
        if (bucket.equals("exp")) {
            url = ClusterRecommender.url + "diu=" + key + "&num=100&leader=pivot&history=50&probe=7";
        } else {
            url = ClusterRecommender.url + "diu=" + key + "&num=50&leader=pivot";
        }
        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            resDoc = httpClient.request(url, ResDoc.class);
        } catch (Exception e) {
            LOG.error("cluster recall failed with id={}", recommendContext.getKey(), e);
        }

        time.stop();

        if (resDoc != null && resDoc.getStatus() == 0 && resDoc.getData() != null) {
            taggedMetricRegistry.histogram("cluster-video.recall.failrate").update(0);
            return resDoc.getData();
        } else {
            taggedMetricRegistry.histogram("cluster-video.recall.failrate").update(100);
            return Collections.emptyList();
        }
    }

}
