package com.td.recommend.recall.hotvideo.recommender;

import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;

import java.util.List;

public interface IRecommender {
    List<VideoDoc> recommend(RecommendContext recommendContext);
}
