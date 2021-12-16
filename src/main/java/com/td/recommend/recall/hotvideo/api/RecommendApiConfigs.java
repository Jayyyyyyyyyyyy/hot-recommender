package com.td.recommend.recall.hotvideo.api;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.td.recommend.recall.hotvideo.filter.ExerciseFilter;
import com.td.recommend.recall.hotvideo.filter.RecallFilter;
import com.td.recommend.recall.hotvideo.filter.TrInviewFilter;
import com.td.recommend.recall.hotvideo.ranker.FollowWorksRanker;
import com.td.recommend.recall.hotvideo.ranker.OperatorRanker;
import com.td.recommend.recall.hotvideo.ranker.RecallRanker;
import com.td.recommend.recall.hotvideo.ranker.RecomeRanker;
import com.td.recommend.recall.hotvideo.recommender.*;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RecommendApiConfigs {
    private static Cache<String, Map<String, Object>> cache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(200000).build();
    private static Cache<String, Map<String, Object>> fastCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES).maximumSize(100000).build();

    private static final Map<String, ApiConfig> configMap = ImmutableMap.<String, ApiConfig>builder()
            //global
            .put("highctr", new ApiConfig(new HighctrVideoRecommender(), null, null, cache))         // 无引用
            .put("jphighctr", new ApiConfig(new JPHighctrVideoRecommender(), null, null, cache))     //在video_recommender中没有对应的retrievekeybuild
            .put("headpool", new ApiConfig(new HeadpoolVideoRecommender(), null, null, cache))       //无引用
            .put("teaching", new ApiConfig(new TeachingRecommender(), null, null, cache))            //无引用
            .put("hotmp3", new ApiConfig(new HotMp3Recommender(), null, null, cache))                //无引用
            .put("athome", new ApiConfig(new AtHomeRecommender(), null, null, cache))                //无引用
            .put("newhotmp3", new ApiConfig(new NewHotMp3Recommender(), null, null, cache))          //在video_recommender中没有对应的retrievekeybuild
            .put("blast", new ApiConfig(new BlastRecommender(), null, null, cache))                  //*
            .put("basic", new ApiConfig(new BasicRecommender(), null, null, cache))                  //无引用
            .put("popular", new ApiConfig(new PopularRecommender(), null, new ExerciseFilter(), cache))    //*
            .put("quality", new ApiConfig(new QualityRecommender(), null, null, cache))              //无引用
            //i2i
            .put("itemcf", new ApiConfig(new ItemCfRecommender(), null, new ExerciseFilter(), cache))      //*
            .put("simuids", new ApiConfig(new SimUidsRecommender(), null, null, cache))              //提供洪涛使用
            .put("origin", new ApiConfig(new OriginRecommender(), null, null, cache))                //*
            .put("gem", new ApiConfig(new GemRecommender(), null, new ExerciseFilter(), cache))           //*
            .put("bert", new ApiConfig(new BertRecommender(), null, new ExerciseFilter(), cache))         //*
            //u2i
            .put("usercf", new ApiConfig(new UserCfRecommender(), null, new ExerciseFilter(), null))                    //*
            .put("usercfv2", new ApiConfig(new UserCFV2Recommender(), null, new ExerciseFilter(), null))                // 无请求
            .put("bpr", new ApiConfig(new BprRecommender(), null, new ExerciseFilter(), null))
            .put("search", new ApiConfig(new SearchRecommender(), null, new ExerciseFilter(), null))                    //*
            .put("nmf", new ApiConfig(new NmfRecommender(), null, new ExerciseFilter(), null))                          // 无请求
            .put("nmfv2", new ApiConfig(new NmfV2Recommender(), null, new ExerciseFilter(), null))                      // 无请求
            .put("repeat", new ApiConfig(new RepeatSeenRecommender(), null, null, null))                           //*
            .put("realtimesearch", new ApiConfig(new RealTimeSearchRecommender(), null, new ExerciseFilter(), null))    // *
            .put("cluster", new ApiConfig(new ClusterRecommender(), null, null, null))
            .put("minet", new ApiConfig(new MinetRecommender(), null, null, null))                                //无流量 下掉
            .put("splitflow", new ApiConfig(new SplitFlowRecommender(), null, null, null))                        //无流量 下掉
            .put("followwatch", new ApiConfig(new FollowWatchRecommender(), null, null, null))
            .put("followworks", new ApiConfig(new FollowWorksRecommender(), new FollowWorksRanker(), null, null))


            //others
            .put("topuids", new ApiConfig(new TopUidsRecommender(), null, null, cache))                                     //在video_recommender中没有对应的retrievekeybuild
            .put("horse", new ApiConfig(new HorseRecommender(), null, null, fastCache))                                     //*
            .put("top", new ApiConfig(new TopRecommender(), null, null, fastCache))                                         // 无流量
            .put("teachingresearch", new ApiConfig(new TeachingResearchRecommender(), null, new TrInviewFilter(), null))   //*
            .put("titleresearch", new ApiConfig(new TitleResearchRecommender(), null, new TrInviewFilter(), null))         //*
            .put("city", new ApiConfig(new CityRecommender(), null, null, cache))                                           // 无引用
            .put("district", new ApiConfig(new DistrictRecommender(), null, null, cache))                                   // 无引用
            .put("operatorhot", new ApiConfig(new OperatorHotRecommender(), new OperatorRanker(), null, cache))                    //无引用
            .put("interest_op", new ApiConfig(new OperatorInterestRecommender(), null, null, cache))                        // 无流量
            .put("usercfv3", new ApiConfig(new UserCFV3Recommender(), null, new ExerciseFilter(), null))                   //*
            .put("nmfv3", new ApiConfig(new NmfV3Recommender(), null, new ExerciseFilter(), null))                         // *
            .put("item2vec", new ApiConfig(new Item2VecRecommender(), null, new ExerciseFilter(), cache))                        //无引用
            .put("recome", new ApiConfig(new RecomeRecommender(), new RecomeRanker(), null, cache))                                //*
            .build();

    public static ApiConfig get(String api) {
        ApiConfig apiConfig = configMap.get(api);
        if (apiConfig == null) {
            throw new UnsupportedOperationException("unsupported api: " + api);
        }
        return apiConfig;
    }

    public static Map<String, ApiConfig> getMap() {
        return configMap;
    }

    @Getter
    @AllArgsConstructor
    public static class ApiConfig {
        IRecommender recommender;
        RecallRanker ranker;
        RecallFilter filter;
        Cache<String, Map<String, Object>> cache;
    }
}
