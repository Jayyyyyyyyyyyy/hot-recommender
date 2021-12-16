package com.td.recommend.recall.hotvideo.recommender;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.google.common.collect.Lists;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.TeachingResearchVids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Created by zjl on 2019/8/8.
 */
@Repository
public class TeachingResearchRecommender implements IRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(TeachingResearchRecommender.class);

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance()
                .getTaggedMetricRegistry();
        taggedMetricRegistry.meter("teachingresearch.request.qps").mark();
        Timer.Context time = taggedMetricRegistry.timer("teachingresearch.request.latency").time();
        List<VideoDoc> videoDocs = new ArrayList<>(1);
        try {
            String tagid = recommendContext.getKey();
            String type = recommendContext.getType();
            feedGroupExp(videoDocs, tagid, type);

        } catch (Exception e) {
            LOG.error("teachingresearch recall failed ", e);
        }
        time.stop();
        if (videoDocs.size() > 0) {
            taggedMetricRegistry.histogram("teachingresearch.recall.no-result.rate").update(0);
        } else {
            taggedMetricRegistry.histogram("teachingresearch.recall.no-result.rate").update(100);
        }
        return videoDocs;
    }

    private void feedGroupExp(List<VideoDoc> videoDocs, String tagid, String type) {
        Map<String, Map<String, List<String>>> tagGroupVids;
        if (type.startsWith("vx")) {
            tagGroupVids = TeachingResearchVids.getVirtualInterest();
        } else {
            tagGroupVids = TeachingResearchVids.getInterest();
        }
        Map<String, List<String>> groupVids = tagGroupVids.get(tagid);
        if (groupVids != null) {

            HashMap<String, Iterator<String>> groupVidIter = new HashMap<>();

            groupVids.forEach((k, v) -> {
                ArrayList<String> shuffledList = Lists.newArrayList(v);
                Collections.shuffle(shuffledList);
                groupVidIter.put(k, shuffledList.iterator());
            });

            ArrayList<String> groups = new ArrayList<>(groupVids.keySet());
            Collections.shuffle(groups);
            while (groups.size() > 0) {
                Iterator<String> groupIter = groups.iterator();
                while (groupIter.hasNext()) {
                    Iterator<String> vidIter = groupVidIter.get(groupIter.next());
                    if (vidIter.hasNext()) {
                        VideoDoc videoDoc = new VideoDoc();
                        videoDoc.setId(vidIter.next());
                        videoDocs.add(videoDoc);
                    } else {
                        groupIter.remove();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        ArrayList<String> strings = new ArrayList<>();
        strings.add("a");
        strings.add("b");
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();

        }
        System.out.println(strings);

    }
}
