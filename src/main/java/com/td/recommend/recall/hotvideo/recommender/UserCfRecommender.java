package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.ResDoc;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.HttpClientSingleton;
import com.td.recommend.recall.hotvideo.history.ClickedDurationHistory;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

/**
 * Created by zjl on 2019/6/14.
 */
@Repository
public class UserCfRecommender implements IRecommender {
    private ForkJoinPool forkJoinPool = new ForkJoinPool(256);
    private ClickedDurationHistory history = ClickedDurationHistory.getInstance();
    private static String u2uUrl;
    private static final int userNum = 200;
    private static int timeout;
    private TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance().getTaggedMetricRegistry();

    static {
        Config bprServer = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        u2uUrl = bprServer.getString("nmfv3.u2u.url");
        timeout = HotVideoConfig.getInstance().getConfig().getInt("usercf.clicks.timeout");
    }

    private static final Logger LOG = LoggerFactory.getLogger(UserCfRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        taggedMetricRegistry.meter("usercf.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("usercf.request.latency").time();
        List<VideoDoc> docs = Collections.emptyList();
        ResDoc similarUsers;
        String diu = recommendContext.getKey();
        String appid = recommendContext.getAppid();

        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String annUrl = u2uUrl;
            String url = annUrl + "key=" + diu + "&num=" + userNum + "&appid=" + appid;
            similarUsers = httpClient.request(url, ResDoc.class);
            if (similarUsers.getData() != null) {
                List<ImmutablePair<String, Double>> usersClicks = getAllClicks(similarUsers.getData());
                docs = sortOccurDocs(usersClicks, recommendContext.getNum());
            } else {
                LOG.error("usercf request u2u failed with diu={}", diu);
            }
        } catch (Exception e) {
            LOG.error("usercf recall failed with diu={}", diu, e);
        }
        time.stop();
        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("usercf.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("usercf.recall.failrate").update(100);
        }
        return docs;
    }

    private List<ImmutablePair<String, Double>> getAllClicks(List<VideoDoc> resDocs) {
        List<ImmutablePair<String, Double>> videos;
        Timer.Context time = taggedMetricRegistry.timer("usercf.getclicks.latency").time();
        ForkJoinTask<List<ImmutablePair<String, Double>>> task = forkJoinPool.submit(() ->
                resDocs.parallelStream()
                        .flatMap(resDoc -> history.clickedDuration(resDoc.getId()).stream())
                        .collect(toList())
        );
        try {
            videos = task.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            task.cancel(true);
            videos = Collections.emptyList();
            LOG.error("usercf.getclicks failed!", e);
        }

        time.stop();

        if (videos.size() > 0) {
            taggedMetricRegistry.histogram("usercf.sim.clicks.count").update(videos.size() / userNum);
        }
        return videos;
    }

    private List<VideoDoc> sortOccurDocs(List<ImmutablePair<String, Double>> allClicks, int num) {
        Map<String, Double> occurMap = new HashMap<>();
        allClicks.forEach(doc -> occurMap.merge(doc.getKey(), Math.atan(doc.getValue() / (60 * 3)), Double::sum));

        return occurMap.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry<String, Double>::getValue).reversed())
                .filter(occur -> occur.getValue() > 1)
                .limit(num)
                .map(kv -> {
                    VideoDoc videoDoc = new VideoDoc();
                    videoDoc.setId(kv.getKey());
                    videoDoc.setScore(kv.getValue());
                    return videoDoc;
                }).collect(toList());
    }
}
