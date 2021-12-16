package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.commons.profile.DocProfileUtils;
import com.td.recommend.docstore.dao.DocItemDao;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.ResDoc;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.HttpClientSingleton;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import com.td.recommend.recall.hotvideo.history.ClickHistory;
import com.td.recommend.recall.hotvideo.history.ClickedDurationHistory;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.td.recommend.userstore.dao.UserItemDao;
import com.td.recommend.userstore.data.UserItem;
import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class FollowWatchRecommender implements IRecommender {

    private static String u2uUrl;
    private static final int userNum = 100;
    private static int timeout;

    private TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance().getTaggedMetricRegistry();


    static {
        Config bprServer = HotVideoConfig.getInstance().getConfig().getConfig("vector-server");
        u2uUrl = bprServer.getString("nmfv3.u2u.url");
        timeout = HotVideoConfig.getInstance().getConfig().getInt("usercf.clicks.timeout");
    }

    private static final Logger LOG = LoggerFactory.getLogger(UserCfRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        taggedMetricRegistry.meter("followwatch.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("followwatch.request.latency").time();

        List<VideoDoc> docs = new ArrayList<>();

        //List<String> similardius = new ArrayList<>();

        //String uid = recommendContext.getKey();
        String diu = recommendContext.getKey();
        String appid = recommendContext.getAppid();
        String prefix = "tfollowwatch_";

        List<String> similarDius = new ArrayList<>();
        similarDius.add(diu);
        try {
            HttpClientSingleton httpClient = HttpClientSingleton.getInstance();
            String annUrl = u2uUrl;
            String url = annUrl + "key=" + diu + "&num=" + userNum + "&appid=" + appid;
            ResDoc similarUsers = httpClient.request(url, ResDoc.class); //获取100个相似用户
            if (similarUsers.getData() != null) {
                similarDius.addAll(similarUsers.getData().stream().map(VideoDoc::getId).collect(toList()));
                //LOG.info("similar dius={}", similarDius);
            }

            RedisClientSingleton instance = RedisClientSingleton.cf;

            similarDius.stream().map(one ->
                    instance.lrange(prefix+one, 0, recommendContext.getNum())
            ).flatMap(Collection::stream).distinct().forEach(one ->{
                docs.add(new VideoDoc(one));
            });

            } catch (Exception e) {
                LOG.error("usercf recall failed with diu={}", diu, e);
            }

//        try{
//            List<String> allDius = new ArrayList<>();
//            RedisClientSingleton instance = RedisClientSingleton.cf;
//
//            for (String my_diu: similarDius) {
//                String key = prefix + my_diu;
//                if (instance.exists(key)){
//                    List<String> topItems = instance.lrange(key, 0, recommendContext.getNum()); // 获取watchfollow 观看的vids
//                    allDius.addAll(topItems);
//                }else{
//                    continue;
//                }
//            }
//            //去重
//            allDius = new ArrayList<String>(new LinkedHashSet<String>(allDius));
//            for (String id : allDius) {
//                VideoDoc videoDoc = new VideoDoc();
//                videoDoc.setId(id);
//                docs.add(videoDoc);
//            }
//
//        } catch (Exception e) {
//            LOG.error("followwatch can't find in redis with diu={}", diu, e);
//        }

        time.stop();
        if (docs.size() > 0) {
            taggedMetricRegistry.histogram("followwatch-video.recall.failrate").update(0);
        } else {
            taggedMetricRegistry.histogram("followwatch-video.recall.failrate").update(100);
        }
        LOG.info("return docs size is {}",docs.size());
        return docs;
    }
}
//        UserItem userItem = new UserItemDao().getUserFollow(uid).get();
//
//        List<VideoDoc> UidDocs = new ArrayList<>();
//        userItem.getFacets().get("vfollow_diu").forEach(item -> {
//            VideoDoc uidDoc = new VideoDoc();
//            uidDoc.setId(item.getName()); //uid
//            UidDocs.add(uidDoc);
//        });
//        LOG.info("followwatch: uid {}: the number of follows is {}", uid, UidDocs.size());
////        System.out.println(videoDocs);
//        if (UidDocs.size() > 0) {
//            List<String> usersClicks = getAllClicks(UidDocs); // 获取所有用户历史点击视频的vid
//            LOG.info("followwatch: uid {}: the number of total videos is {}", uid, usersClicks.size());
//            docs = sortOccurDocs(usersClicks, recommendContext.getNum());
//            LOG.info("followwatch: uid {}: after fileting, the number of total videos is {}", uid, docs.size());
//
//        } else {
//            LOG.error("followwatch get 0 follow with uid={}", uid);
//        }
//
//        time.stop();
//        if (docs.size() > 0) {
//            taggedMetricRegistry.histogram("followwatch.recall.failrate").update(0);
//        } else {
//            taggedMetricRegistry.histogram("followwatch.recall.failrate").update(100);
//        }
//        return docs;
//    }
//
//    private List<String> getAllClicks(List<VideoDoc> resDocs) {
//        List<String> videos;
//        Timer.Context time = taggedMetricRegistry.timer("followwatch.getclicks.latency").time();
//
//        ForkJoinTask<List<String>> task = forkJoinPool.submit(() ->
//                resDocs.parallelStream()
//                        .flatMap(resDoc -> history.clicked(resDoc.getId()).stream())
//                        .collect(toList())
//        );
//
//        ForkJoinTask<List<String>> task2 = forkJoinPool.submit(() ->
//                resDocs.parallelStream()
//                        .flatMap(resDoc -> history.clicked(resDoc.getId()).stream())
//                        .collect(toList())
//        );
//        try {
//            videos = task.get(timeout, TimeUnit.MILLISECONDS);
//        } catch (Exception e) {
//            task.cancel(true);
//            videos = Collections.emptyList();
//            LOG.error("followwatch.getclicks failed!", e);
//        }
//
//        time.stop();
//
//        if(videos.size()>0) {
//            taggedMetricRegistry.histogram("followwatch.sim.clicks.count").update(videos.size() / userNum);
//        }
//
//        return videos;
//
//    }
//
//    private List<VideoDoc> sortOccurDocs(List<String> allClicks, int num) {
//        Map<String, Integer> occurMap = new HashMap<>();
//        allClicks.forEach(docId -> occurMap.merge(docId, 1, Integer::sum));
//
//        List<Map.Entry<String, Integer>> occurList = new ArrayList<>(occurMap.entrySet());
//
//        return occurList.stream()
//                .sorted(Comparator.comparing(Map.Entry<String, Integer>::getValue).reversed())
//                .filter(occur -> occur.getValue() >= 1)// 召回频次大于40%用户数
//                //.filter(occur -> IntStream.of(ctypes).anyMatch(x -> x == DocProfileUtils.getCtype(new DocItemDao().get(occur.getKey()).get()))) //过滤ctype
//                .limit(num)
//                .map(kv -> {
//                    VideoDoc videoDoc = new VideoDoc();
//                    videoDoc.setId(kv.getKey());
//                    videoDoc.setScore(kv.getValue());
//                    return videoDoc;
//                }).collect(toList());
//    }
//    public static void main(String[] args) {
//        RecommendContext recommendContext = new RecommendContext();
//        recommendContext.setNum(100);
//        recommendContext.setKey("2572699");
//        List<VideoDoc> recommend = new FollowWatchRecommender().recommend(recommendContext);
//        recommend.forEach(doc -> System.out.println( doc.getId() + ":" + doc.getScore()));
//    }
//}
