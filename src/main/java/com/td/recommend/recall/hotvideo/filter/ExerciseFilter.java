package com.td.recommend.recall.hotvideo.filter;

import com.google.common.collect.ImmutableMap;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * create by pansm at 2019/09/06
 */
public class ExerciseFilter implements RecallFilter {
    private static final Logger LOG = LoggerFactory.getLogger(ExerciseFilter.class);
    private static ImmutableMap<String, Double> uerMap = ImmutableMap.of(
            "1007", 0.05,
            "268", 0.05
    );

    public void filter(List<VideoDoc> list, RecommendContext context) {
        Double uer;
        if (context.getBucket().endsWith("exercise_filter-yes")) {
            uer = uerMap.getOrDefault(context.getBucket(), 0.05);
        } else {
            return;
        }

        if (list.isEmpty()) {
            return;
        }
        RedisClientSingleton client = RedisClientSingleton.headpoolFilter;
//        List<String> idList = list.stream().map(VideoDoc::getId).collect(Collectors.toList());
        List<String> idList = list.stream().map(i -> "fwur_" + i.getId()).collect(Collectors.toList());

        try {
            String[] ids = idList.toArray(new String[0]);
            List<String> result = client.mget(ids);
            Set<String> inValidSet = IntStream.range(0, ids.length)
                    .mapToObj(i -> new Pair<>(list.get(i), result.get(i)))
                    .filter(i -> i.getValue() == null || Double.parseDouble(i.getValue()) < uer)
                    .map(i -> i.getKey().getId())
                    .collect(Collectors.toSet());
            list.removeIf(i -> inValidSet.contains(i.getId()));

        } catch (Exception ex) {
            LOG.error("get idlist failed," + ex.toString(), ex);
        }
    }


    public static void main(String[] argv) {
        List<VideoDoc> idlist = new ArrayList<>();

        idlist.add(new VideoDoc("1500672391837"));
        idlist.add(new VideoDoc("1500676032819"));
        idlist.add(new VideoDoc("1500676399474"));
        idlist.add(new VideoDoc("111"));
        RecommendContext recommendContext = new RecommendContext();
        recommendContext.setType("gs");
        new ExerciseFilter().filter(idlist, recommendContext);
        System.out.println("LJK" + idlist.stream().map(i -> i.getId()).collect(Collectors.joining(",")));
        System.out.println("LJK" + RedisClientSingleton.headpoolFilter.get("1500676399474"));
    }


}
