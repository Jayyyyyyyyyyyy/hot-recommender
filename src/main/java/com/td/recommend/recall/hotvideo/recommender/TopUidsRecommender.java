package com.td.recommend.recall.hotvideo.recommender;

import com.google.common.collect.Lists;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * create by Liujikun at 2021/06/16
 */

@Repository
public class TopUidsRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(TopUidsRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        List<VideoDoc> videoDocs = new ArrayList<>();

        try {
            List<String> secondcatUidNameVids = RedisClientSingleton.general.lrange("secondid_puid_vid", 0, -1);

            String lastKey = "";
            VideoDoc lastDoc = null;
            for (String secondcatUidVid : secondcatUidNameVids) {
                String[] split = secondcatUidVid.split(":");
                String secondCat = split[0];
                String uid = split[1];
                String vid = split[2];
                String v2secid = split[3];
                String v2secname = split[4];

                String key = secondCat + ":" + uid;
                if (!key.equals(lastKey)) {
                    VideoDoc videoDoc = new VideoDoc();
                    videoDoc.setId(uid);
                    videoDoc.setSubcat(secondCat);
                    videoDoc.setSubcatId(v2secid);
                    videoDoc.setSubcatName(v2secname);
                    videoDoc.setAttach(Lists.newArrayList(vid));
                    videoDocs.add(videoDoc);
                    lastDoc = videoDoc;
                    lastKey = key;
                } else {
                    lastDoc.getAttach().add(vid);
                }
            }
        } catch (Exception e) {
            LOG.error("recall failed ", e);
        }
        return videoDocs;
    }

    public static void main(String[] args) {
        List<VideoDoc> recommend = new TopUidsRecommender().recommend(new RecommendContext());
        System.out.println(recommend);
    }
}
