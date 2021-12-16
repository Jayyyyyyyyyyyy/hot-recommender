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

import java.util.*;

@Repository
public class LookLikeRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(LookLikeRecommender.class);
    private static final String separator1 = "\\s";
    private static final String separator2 = ":";
    public LookLikeRecommender() { }
    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("looklike-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("looklike-video.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        String key = "look:" + recommendContext.getKey();
        try {
//            RedisClientSingleton instance = RedisClientSingleton.looklike;//对应redis 已下线
            String videoInfos = null;//instance.get(key);
            videoDocs = getVideoDocs(videoInfos);
            videoDocs.subList(0, Math.min(recommendContext.getNum(), videoDocs.size()));
        } catch (Exception e) {
            LOG.error("looklike-video recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("looklike-video.recall.no-result.rate").update(0);
        } else {
            taggedMetricRegistry.histogram("looklike-video.recall.no-result.rate").update(100);
        }
        return videoDocs;
    }

    private static List<VideoDoc> getVideoDocs(String videoInfos) {
        List<VideoDoc> videoDocs = new ArrayList<>();
        if (videoInfos != null && !videoInfos.isEmpty()) {
            String[] vid_scores = videoInfos.split(separator1);
            for (int i = 0; i< vid_scores.length;i++) {
                String[] split = vid_scores[i].split(separator2);
                if(split != null && split.length==2){
                    VideoDoc videoDoc = new VideoDoc();
                    try {
                        videoDoc.setScore(Double.valueOf(split[1]));
                    }catch (Exception e){
                        LOG.error("parse score failed");
                        videoDoc.setScore(0);
                    }
                    videoDoc.setId(split[0]);
                    videoDocs.add(videoDoc);
                }
            }
            videoDocs.sort(Comparator.comparing(VideoDoc::getScore).reversed());
        }
        return videoDocs;
    }
}