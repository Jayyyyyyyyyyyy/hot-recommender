package com.td.recommend.recall.hotvideo.index;

import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Frang on 2017/8/12.
 */
public class InterestIndex {
    private static final Logger LOG = LoggerFactory.getLogger(InterestIndex.class);

    private Map<String, List<VideoDoc>> catHotVideoMap = new HashMap<>();
    private Map<String, List<VideoDoc>> subcatHotVideoMap = new HashMap<>();
    private Map<String, List<VideoDoc>> tagHotVideoMap = new HashMap<>();

    public void buildIndex(List<VideoDoc> videoDocs) {
        Map<String, List<VideoDoc>> catHotVideoMap = new HashMap<>();
        Map<String, List<VideoDoc>> subcatHotVideoMap = new HashMap<>();
        Map<String, List<VideoDoc>> tagHotVideoMap = new HashMap<>();

        for (VideoDoc videoDoc : videoDocs) {
            String cat = videoDoc.getCat();
            if (cat != null) {
                putDoc(catHotVideoMap, videoDoc, cat);
            }

            String subcat = videoDoc.getSubcat();
            if (subcat != null) {
                putDoc(subcatHotVideoMap, videoDoc, subcat);
            }

            for (String tag : videoDoc.getAttach()) {
                putDoc(tagHotVideoMap, videoDoc, tag);
            }
        }

        this.catHotVideoMap = catHotVideoMap;
        this.subcatHotVideoMap = subcatHotVideoMap;
        this.tagHotVideoMap = tagHotVideoMap;
    }

    private void putDoc(Map<String, List<VideoDoc>> hotVideoMap, VideoDoc videoDoc, String interest) {
        List<VideoDoc> hotVideo = hotVideoMap.get(interest);
        if (hotVideo == null) {
            hotVideo = new ArrayList<>();
            hotVideoMap.put(interest, hotVideo);
        }
        hotVideo.add(videoDoc);
    }

    public List<VideoDoc> getHotVideo(String type, String interest) {
        List<VideoDoc> videoDocs = null;
        switch (type) {
            case "cat":
                videoDocs = this.catHotVideoMap.get(interest);
                break;
            case "subcat":
                videoDocs = this.subcatHotVideoMap.get(interest);
                break;
            case "tag":
                videoDocs = this.tagHotVideoMap.get(interest);
                break;
        }

        if (videoDocs == null) {
            LOG.warn("type={} interest={} has empty hotvideo", type, interest);
            return Collections.emptyList();
        }

        return videoDocs;
    }
}
