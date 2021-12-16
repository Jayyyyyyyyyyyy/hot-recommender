package com.td.recommend.recall.hotvideo.datasource;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TeachingResearchVids {
    private static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    @Getter
    private static volatile Map<String, Map<String, List<String>>> interest = new HashMap<>();
    @Getter
    private static volatile Map<String, Map<String, List<String>>> virtualInterest = new HashMap<>();
    private static final Logger log = LoggerFactory.getLogger(TeachingResearchVids.class);

    static {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                RedisClientSingleton redis = RedisClientSingleton.general;
                List<String> feedList = redis.lrange("feed_test_video:groups:list", 0, -1);
                //分组实验
                Map<String, Map<String, List<String>>> backupMap = new HashMap<>();
                Map<String, Map<String, List<String>>> vbackupMap = new HashMap<>();
                for (String value : feedList) {
                    String[] s = value.split("_");
                    if (s.length < 4) {
                        continue;
                    }
                    String vid = s[0];
                    String tagid = s[1];
                    String virTagid = s[2];
                    String group = s[3];
                    loadInterestVids(vid, tagid, group, backupMap);
                    loadInterestVids(vid, virTagid, group, vbackupMap);
                }
                interest = backupMap;
                virtualInterest = vbackupMap;
            } catch (Exception e) {
                log.error("load teaching-research video and weight failed", e);
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    private static void loadInterestVids(String vid, String tagid, String group, Map<String, Map<String, List<String>>> interest) {
        Map<String, List<String>> groupVids = interest.get(tagid);
        if (groupVids != null) {
            List<String> vids = groupVids.get(group);
            if (vids != null) {
                vids.add(vid);
            } else {
                vids = new ArrayList<>();
                vids.add(vid);
                groupVids.put(group, vids);
            }
        } else {
            groupVids = new HashMap<>();
            List<String> vids = new ArrayList<>();
            vids.add(vid);
            groupVids.put(group, vids);
            interest.put(tagid, groupVids);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(10000);
        System.out.println(interest);
    }
}
