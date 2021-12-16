package com.td.recommend.recall.hotvideo.filter;

import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TrInviewFilter implements RecallFilter{

    private static Logger LOG = LoggerFactory.getLogger(TrInviewFilter.class);

    private static int inview_threshold = 20000;

    private String buildKey(String id, String type, String day) {
        return day+"_"+type+"_"+id;
    }

    private String getCurDay() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date t = new Date();
        return df.format(t);
    }

    public void filter(List<VideoDoc> docs, RecommendContext recommendContext) {
        String day=getCurDay();

        RedisClientSingleton client = RedisClientSingleton.tr_day_inview;
        List<String> keyList = docs.stream().map(i -> buildKey(i.getId(), recommendContext.getType(), day)).collect(Collectors.toList());
        try {
            String[] ids = keyList.toArray(new String[0]);
            List<String> result = client.mget(ids);
            Set<String> inValidSet = IntStream.range(0, ids.length)
                    .mapToObj(i -> new Pair<>(docs.get(i), result.get(i)))
                    .filter(i -> i.getValue() != null && Integer.parseInt(i.getValue()) > inview_threshold)
                    .map(i -> i.getKey().getId())
                    .collect(Collectors.toSet());
            docs.removeIf(i -> inValidSet.contains(i.getId()));

            if (inValidSet.size()>0) {
                LOG.info("type:{},invalidset:{}",recommendContext.getType(),String.join(",",inValidSet));
            }


        } catch (Exception ex) {
            LOG.error("get idlist failed," + ex.toString(), ex);
        }
    }


    public static void main(String[] argv) {

        TrInviewFilter trInviewFilter = new TrInviewFilter();
        List<VideoDoc> docs = new ArrayList<>();
        VideoDoc doc1 = new VideoDoc(); doc1.setId("1500677797751");
        VideoDoc doc2 = new VideoDoc(); doc2.setId("1500675344886");
        VideoDoc doc3 = new VideoDoc(); doc3.setId("1500677622534");
        VideoDoc doc4 = new VideoDoc(); doc4.setId("1500674835360");
        VideoDoc doc5 = new VideoDoc(); doc5.setId("1500678017688");
        docs.add(doc1);
        docs.add(doc2);
        docs.add(doc3);
        docs.add(doc4);
        docs.add(doc5);
        RecommendContext recommendContext = new RecommendContext();
        recommendContext.setType("vsubcat_tr");
        trInviewFilter.filter(docs, recommendContext);
        docs.forEach(x->System.out.println(x.getId()));


        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date t = new Date();
        System.out.println(df.format(t));
    }

}
