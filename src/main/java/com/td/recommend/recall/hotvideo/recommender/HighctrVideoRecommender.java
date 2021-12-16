package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import com.td.recommend.streaming.common.MatrixStoreItemNew;
import com.td.recommend.streaming.utils.SerializeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by liujikun on 2019/7/20.
 */
@Repository
public class HighctrVideoRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(HighctrVideoRecommender.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("highctr-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("highctr-video.request.latency").time();

        List<VideoDoc> videoDocs = new ArrayList<>();
        try {
            RedisClientSingleton hotRedis = RedisClientSingleton.highctr;
            String key = "80000_highctr";
            byte[] ds = hotRedis.get(key.getBytes());
            Object object = SerializeUtil.deserialize(ds);

            if (object != null) {
                MatrixStoreItemNew mi = (MatrixStoreItemNew) object;
                int hotSize = mi.getDocMatrics().size();
                System.out.println(hotSize);
                for (int i = 0; i < Math.min(recommendContext.getNum(), hotSize); i++) {
                    VideoDoc videoDoc = new VideoDoc();
                    String docId = mi.getDocMatrics().get(i).docId;
                    double score = mi.getDocMatrics().get(i).ctr;
                    videoDoc.setId(docId);
                    videoDoc.setScore(score);
                    videoDocs.add(videoDoc);
                }
            }
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
