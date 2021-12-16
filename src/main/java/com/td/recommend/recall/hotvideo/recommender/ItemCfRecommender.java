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
 * Created by zjl on 2019/6/14.
 */
@Repository
public class ItemCfRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(ItemCfRecommender.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("itemcf-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("itemcf-video.request.latency").time();
        List<VideoDoc> docs = new ArrayList<>();

        String vid = recommendContext.getKey();
        String type = recommendContext.getType();
        // String bucket = recommendContext.getBucket()
        String prefix = "vitemcf_";
        if(type!=null && type.equals("vitemcfv2")){
            prefix = "vitemcf_v2_";
        }else if (type!=null && type.equals("vitemcftrend")){
            prefix = "itemcf_trend_";
        }else{

        }
        try {
            RedisClientSingleton instance = RedisClientSingleton.cf;
            String key = prefix + vid;
            List<String> topItems = instance.lrange(key, 0, recommendContext.getNum()); // 从redis中获取数据
            for (String id : topItems) {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(id);
                docs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("itemcf recall failed with id={}", vid, e);
        }
        time.stop();
        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("itemcf-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("itemcf-video.recall.failrate").update(100);
        }
        return docs;
    }

}