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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * create by pansm at 2019/08/30
 */

@Repository
public class PopularRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(PopularRecommender.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private static final String POPULAR_KEY="shortterm_popular_vid";
    private static final String separator="&";

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("popular-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("popular-video.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();

        try {
            RedisClientSingleton hotRedis = RedisClientSingleton.popular;
            String value = hotRedis.get(POPULAR_KEY);
            //String value = "1500672816241&1500672301623&1500672737063&1500672787812&1500672712985&1500672663154&1500672609531&1500672791465&1500672462090&1500672674404&1500672714262&1500672793566&1500672466103&1500672772704&1500672717180&1500672388520&1500672559942&1500672494631&1500672793016&1500672689691&1500672462925&1500672738943";

            if (value==null || value.isEmpty()) {
                LOG.error("key:{} ,value is error.",POPULAR_KEY);
                return new ArrayList<>();
            }

            String[] valuelist = value.split(separator);
            if (valuelist.length<2) {
                return new ArrayList<>();
            }
            for (int i=0; i<Math.min((valuelist.length-1),recommendContext.getNum()); i++) {
                VideoDoc videoDoc = new VideoDoc();
                String docId = valuelist[i];
                double score = 1.0;
                videoDoc.setId(docId);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }

        } catch (Exception e) {
            LOG.error("popular recall failed "+e.toString(), e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("popular-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("popular-video.recall.failrate").update(100);
        }
        return videoDocs;
    }

    public static void main(String[] argv) {
        RedisClientSingleton hotRedis = RedisClientSingleton.popular;
        String value = hotRedis.get(POPULAR_KEY);
        System.out.println(value);
    }


}
