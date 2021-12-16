package com.td.recommend.recall.hotvideo.recommender;

import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.utils.HorseConfigs;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

/**
 * create by Liujikun at 2021/06/16
 */

@Repository
public class HorseRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(HorseRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        List<VideoDoc> videoDocs = new ArrayList<>();

        try {
            HorseConfigs.Horse horse = HorseConfigs.get().get(recommendContext.getType());
            HorseConfigs.Config config = horse.getConfig();
            List<String> vids = config.getVids();
            Map<String, HorseConfigs.Stat> statMap = horse.getStats().stream().collect(Collectors.toMap(HorseConfigs.Stat::getVid, i -> i));
            Set<String> invalidVids = horse.getStats().stream().filter(i -> i.getView() > config.getMax_vid_view() || i.getWeight() <= 0.0).map(HorseConfigs.Stat::getVid).collect(Collectors.toSet());
            for (String vid : vids) {
                if (invalidVids.contains(vid)) {
                    continue;
                }
                HorseConfigs.Stat stat = statMap.get(vid);
                double click = 0;
                double view = 0;
                if (stat != null) {
                    click = stat.getTotal_click();
                    view = stat.getTotal_view();
                }
                BetaDistribution betaDistribution = new BetaDistribution(click + 1, view - click + 1);
                double score = betaDistribution.sample();
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setId(vid);
                videoDoc.setScore(score);
                videoDocs.add(videoDoc);
            }
        } catch (Exception e) {
            LOG.error("recall failed ", e);
        }
        videoDocs.sort(Comparator.comparing(VideoDoc::getScore).reversed());
        return videoDocs;
    }


}
