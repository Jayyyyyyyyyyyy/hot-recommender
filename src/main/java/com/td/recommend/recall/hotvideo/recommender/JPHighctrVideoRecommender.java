package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.cache.JPHighctrVideoGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * create by pansm at 2019/08/29
 */

@Repository
public class JPHighctrVideoRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(HighctrVideoRecommender.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("jphighctr-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("jphighctr-video.request.latency").time();

        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            List<VideoDoc> resultDocs = JPHighctrVideoGetter.getInstance().getVideoDocs(JPHighctrVideoGetter.JPHIGHCTR_KEY);

            int realnum = Math.min(recommendContext.getNum(),resultDocs.size());
            videoDocs = resultDocs.subList(0,realnum);
        } catch (Exception e) {
            LOG.error("highctr recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("highctr-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("highctr-video.recall.failrate").update(100);
        }
        return videoDocs;
    }


    public static void main(String[] args) {
        RecommendContext recommendContext = new RecommendContext();
        recommendContext.setBucket("80000");
        recommendContext.setNum(50);
        List<VideoDoc> recommend = new HighctrVideoRecommender().recommend(recommendContext);
        recommend.forEach(doc -> System.out.println(doc.getId()));
    }
}
