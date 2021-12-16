package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Created by liujikun on 2019/7/20.
 */
@Repository
public class HeadpoolVideoRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(HeadpoolVideoRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("headpool-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("headpool-video.request.latency").time();

        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            RedisClientSingleton redis = RedisClientSingleton.search;
            String key = "static:index:1";//每日精选
            Set<Tuple> idsWithSore = redis.zrange(key, 0, recommendContext.getNum());
            idsWithSore.stream()
                    .sorted(Comparator.comparing(Tuple::getScore).reversed())
                    .forEach(t -> {
                        VideoDoc videoDoc = new VideoDoc();
                        videoDoc.setId(t.getElement());
                        videoDoc.setScore(t.getScore());
                        videoDocs.add(videoDoc);
                    });
        } catch (Exception e) {
            LOG.error("headpool recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("headpool-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("headpool-video.recall.failrate").update(100);
        }
        return videoDocs;
    }


    public static void main(String[] args) {
        RecommendContext recommendContext = new RecommendContext();
        recommendContext.setBucket("80000");
        recommendContext.setNum(50);
        List<VideoDoc> recommend = new HeadpoolVideoRecommender().recommend(recommendContext);
        recommend.forEach(doc -> System.out.println(doc.getId()));
    }
}
