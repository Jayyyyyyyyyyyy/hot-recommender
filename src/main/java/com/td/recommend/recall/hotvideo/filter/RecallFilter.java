package com.td.recommend.recall.hotvideo.filter;

import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;

import java.util.List;

/**
 * Created by:liujikun
 * Date: 2017/8/18
 */
public interface RecallFilter {
    void filter(List<VideoDoc> docs, RecommendContext recommendContext);
}
