package com.td.recommend.recall.hotvideo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONType;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HorseConfigs {
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private static volatile Map<String, Horse> horses = Collections.emptyMap();
    private static final Logger log = LoggerFactory.getLogger(HorseConfigs.class);

    static {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            RedisClientSingleton redis = RedisClientSingleton.general;
            Map<String, Horse> tmpHorses = new HashMap<>();
            for (int i = 1; i <= 10; i++) {
                String horseName = "v" + i + "_horse";
                Horse horse = parseHorseConfig(redis, horseName);
                if (horse != null) {
                    tmpHorses.put(horseName, horse);
                }
            }
            horses = tmpHorses;
        }, 0, 1, TimeUnit.MINUTES);
    }

    private static Horse parseHorseConfig(RedisClientSingleton redis, String horse) {
        Map<String, String> confStats = redis.hgetAll(horse);
        try {
            Config config = JSON.parseObject(confStats.get("config"), Config.class);
            List<Stat> stats = JSON.parseArray(confStats.get("stats"), Stat.class);
            if (config == null) {
                return null;
            }
            if (stats == null) {
                stats = new ArrayList<>();
            }
            return new Horse(config, stats);
        } catch (Exception e) {
            log.warn("horse {} parse failed ", horse, e);
            return null;
        }
    }

    public static Map<String, Horse> get() {
        return horses;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @JSONType(orders = {"type", "key", "max_horse_view", "max_vid_view", "levels", "vids"})
    public static class Config {
        String type;
        String key;
        double max_horse_view;
        double max_vid_view;
        List<Level> levels;
        List<String> vids;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @JSONType(orders = {"vid", "click", "view", "total_click", "total_view", "weight"})
    public static class Stat {
        String vid;
        double click;
        double view;
        double total_click;
        double total_view;
        double weight;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @JSONType(orders = {"lctr", "lview", "lweight"})
    public static class Level {
        double lctr;
        double lview;
        double lweight;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @JSONType(orders = {"config", "stats"})
    public static class Horse {
        Config config;
        List<Stat> stats;
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(10000);
        Map<String, Horse> stringHorseMap = HorseConfigs.get();
        System.out.println(JSON.toJSONString(stringHorseMap));
    }
}

