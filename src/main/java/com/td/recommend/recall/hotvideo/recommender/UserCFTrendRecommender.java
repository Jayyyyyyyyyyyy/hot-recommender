package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.commons.profile.DocProfileUtils;
import com.td.recommend.docstore.dao.DocItemDao;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.ResDoc;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.HttpClientSingleton;
import com.td.recommend.recall.hotvideo.history.ClickHistory;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class UserCFTrendRecommender implements IRecommender{
    private ForkJoinPool forkJoinPool = new ForkJoinPool(128);
    private ClickHistory history = ClickHistory.getInstance();
    private static String nmfv3_u2uUrl;
    private static final int userNum = 200;
    private static int timeout;
    private TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance().getTaggedMetricRegistry();

    static {
        Config bprServer = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        nmfv3_u2uUrl = bprServer.getString("nmfv3.u2u.url");
        timeout = HotVideoConfig.getInstance().getConfig().getInt("usercf.clicks.timeout");
    }

    private static final Logger LOG = LoggerFactory.getLogger(UserCfRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        taggedMetricRegistry.meter("usercftrend.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("usercftrend.request.latency").time();
        List<VideoDoc> docs = Collections.emptyList();
        ResDoc similarUsers;
        String diu = recommendContext.getKey();
        String appid = recommendContext.getAppid();
        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String annUrl = nmfv3_u2uUrl;
            String url = annUrl + "key=" + diu + "&num=" + userNum + "&appid=" + appid;
            similarUsers = httpClient.request(url, ResDoc.class);
            if (similarUsers.getData() != null) {
                List<String> usersClicks = getAllClicks(similarUsers.getData()); //获取点击历史记录
                docs = sortOccurDocs(usersClicks, recommendContext.getNum());
            } else {
                LOG.error("usercftrend request u2u failed with diu={}", diu);
            }
        } catch (Exception e) {
            LOG.error("usercftrend recall failed with diu={}", diu, e);
        }
        time.stop();
        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("usercftrend.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("usercftrend.recall.failrate").update(100);
        }
        return docs;
    }

    private List<String> getAllClicks(List<VideoDoc> resDocs) {
        List<String> videos;
        Timer.Context time = taggedMetricRegistry.timer("usercftrend.getclicks.latency").time();

        ForkJoinTask<List<String>> task = forkJoinPool.submit(() ->
                resDocs.parallelStream()
                        .flatMap(resDoc -> history.clicked(resDoc.getId()).stream())
                        .collect(toList())
        );
        try {
            videos = task.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            task.cancel(true);
            videos = Collections.emptyList();
            LOG.error("usercftrend.getclicks failed!", e);
        }

        time.stop();

        if(videos.size()>0) {
            taggedMetricRegistry.histogram("usercftrend.sim.clicks.count").update(videos.size() / userNum);
        }
        return videos;
    }

    private List<VideoDoc> sortOccurDocs(List<String> allClicks, int num) {
        int[] a = {103,503,107,105}; //过滤ctype
        Map<String, Integer> occurMap = new HashMap<>();
        allClicks.forEach(docId -> occurMap.merge(docId, 1, Integer::sum));

        List<Map.Entry<String, Integer>> occurList = new ArrayList<>(occurMap.entrySet());
        LOG.info("occurList size: {}", occurList.size());
        return occurList.stream()
                .sorted(Comparator.comparing(Map.Entry<String, Integer>::getValue).reversed())
                .filter(occur -> occur.getValue() > 1 ) //过滤个数大于1
                .filter(occur -> IntStream.of(a).anyMatch(x -> x == DocProfileUtils.getCtype(new DocItemDao().get(occur.getKey()).get()))) //过滤ctype
                .limit(num) //100
                .map(kv -> {
                    VideoDoc videoDoc = new VideoDoc();
                    videoDoc.setId(kv.getKey());
                    videoDoc.setScore(kv.getValue());
                    return videoDoc;
                }).collect(toList());
    }
}
