package com.td.recommend.recall.hotvideo.recommender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Repository
public class RealTimeRepeatSeenRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(RealTimeRepeatSeenRecommender.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    public RealTimeRepeatSeenRecommender() { }
    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("realtime quality-video.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("realtime quality-video.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>();
        String key = "userPlay:" + recommendContext.getKey();
        try {
            RedisClientSingleton instance = RedisClientSingleton.hot_mp3;
            String videoInfos = instance.get(key);
            videoDocs = getVideoDocs(videoInfos);
            videoDocs.sort(Comparator.comparing(VideoDoc::getScore).reversed());
            videoDocs.subList(0, Math.min(recommendContext.getNum(), videoDocs.size()));
        } catch (Exception e) {
            LOG.error("realtime quality recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("realtime quality-video.recall.no-result.rate").update(0);
        } else {
            taggedMetricRegistry.histogram("realtime quality-video.recall.no-result.rate").update(100);
        }
        return videoDocs;
    }

    private static List<VideoDoc> getVideoDocs(String videoInfos) {
        List<VideoDoc> videoDocs = new ArrayList<>();
        if (videoInfos != null && !videoInfos.isEmpty()) {
            Map<String, RealTimeRepeatSeenRecommender.VideoInfo> videoMap = JSON.parseObject(videoInfos, Map.class);
            for (Map.Entry entry : videoMap.entrySet()) {
                String vid = String.valueOf(entry.getKey());
                JSONObject value = (JSONObject) entry.getValue();
                RealTimeRepeatSeenRecommender.VideoInfo videoInfo = value.toJavaObject(RealTimeRepeatSeenRecommender.VideoInfo.class);
                Long timeDiff = timeDistance(videoInfo.recent_time);
                int click = videoInfo.click > 3 ? 3 : videoInfo.click;
                double score = 0.5 * click / 3 + 0.25 * timeDiff / 14 + 0.25 * videoInfo.usefulPlay;
                double score1 = Math.pow(0.98,14-timeDiff)*(0.5 * click / 3 + 0.5 * videoInfo.usefulPlay);
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setScore(score);
                videoDoc.setId(vid);
                videoDocs.add(videoDoc);
            }
        }
        return videoDocs;
    }

    @Getter
    @Setter
    static class VideoInfo {
        public int click;
        public int usefulPlay;
        public String recent_time;
    }

    public static Long timeDistance(String recentTime) {
        String t2 = LocalDateTime.now().minusDays(14).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        try {
            String t1 = sdf.format(new Date(Long.valueOf(recentTime)));
            Date startDate = sdf.parse(t1);
            Date endDate = sdf.parse(t2);
            long betweenDate = (startDate.getTime() - endDate.getTime()) / (60 * 60 * 24 * 1000);
            return betweenDate;
        } catch (Exception e) {
            LOG.error("realtime quality recommender parse time Exception");
        }
        return 14L;
    }
}