package com.td.recommend.recall.hotvideo.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import com.td.recommend.streaming.common.MatrixStoreItemNew;
import com.td.recommend.streaming.common.MutiMatrixStoreItem;
import com.td.recommend.streaming.utils.SerializeUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * create by pansm at 2019/08/29
 */
public class JPHighctrVideoGetter {
    private static Logger LOG = LoggerFactory.getLogger(JPHighctrVideoGetter.class);

    public static final String JPHIGHCTR_KEY = "action_highctr#tangdou";
    private LoadingCache<String, List<VideoDoc>> cacheDocList;
    private static final int expireTime = 600;

    private static JPHighctrVideoGetter instance = new JPHighctrVideoGetter();


    private JPHighctrVideoGetter() {

        cacheDocList = CacheBuilder.newBuilder().refreshAfterWrite(expireTime, TimeUnit.SECONDS)
                .build(new CacheLoader<String,  List<VideoDoc>>() {
                    @Override
                    public  List<VideoDoc> load(String key) throws Exception {
                        LOG.info("start refresh jphighctr key:{}",key);
                        return getItemListByKey(key);
                    }
                });
        if (StringUtils.isNotBlank(JPHIGHCTR_KEY)) {
            cacheDocList.put(JPHIGHCTR_KEY, getItemListByKey(JPHIGHCTR_KEY));
        }
    }


    public static JPHighctrVideoGetter getInstance() {
        return instance;
    }


    private List<VideoDoc> getItemListByKey(String key) {
        try {
            RedisClientSingleton hotRedis = RedisClientSingleton.highctr;
            byte[] ds = hotRedis.get(key.getBytes());
            Object object = SerializeUtil.deserialize(ds);
            List<VideoDoc> resultList = new ArrayList<>();

            if (object != null) {
                MutiMatrixStoreItem mi = (MutiMatrixStoreItem) object;
                int hotSize = mi.getDocMatrics().size();
                System.out.println(hotSize);
                for (int i=0; i<hotSize; i++) {
                    VideoDoc videoDoc = new VideoDoc();
                    String docId = mi.getDocMatrics().get(i).docId;
                    double score = mi.getDocMatrics().get(i).totalCtr;
                    videoDoc.setId(docId);
                    videoDoc.setScore(score);
                    resultList.add(videoDoc);
                }
                return resultList;
            }
        } catch (Exception e) {
            LOG.error("jphighctr recall failed"+e.toString(), e);
        }
        return new ArrayList<>();

    }


    public List<VideoDoc> getVideoDocs(String key) {
        try {
            return cacheDocList.get(key);
        }catch (Exception ex) {
            LOG.error("cacheDocList get err:"+ex.toString(),ex);
        }
        return new ArrayList<>();
    }

    public static void main(String[] argv) {
        List<VideoDoc> list = JPHighctrVideoGetter.getInstance().getItemListByKey(JPHIGHCTR_KEY);
        for (VideoDoc doc:list) {
            System.out.println("id:"+doc.getId()+" score:"+doc.getScore());
        }
    }
}
