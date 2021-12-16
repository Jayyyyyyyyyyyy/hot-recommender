package com.td.recommend.recall.hotvideo.api;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.filter.RecallFilter;
import com.td.recommend.recall.hotvideo.ranker.RecallRanker;
import com.td.recommend.recall.hotvideo.recommender.IRecommender;
import com.td.recommend.recall.hotvideo.utils.PredictItemsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@Repository
public class RecommenderService {
    private static final Logger LOG = LoggerFactory.getLogger(RecommenderService.class);
    private TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance().getTaggedMetricRegistry();

    @Autowired
    private PredictItemsBuilder predictItemsBuilder;

    public List<VideoDoc> recommend(RecommendContext recommendContext) {
        String api = recommendContext.getApi(); // RecommendApiConfigs中定义好的名字：例如，'itemcf'
        IRecommender recommender = RecommendApiConfigs.get(recommendContext.getApi()).getRecommender();

        List<VideoDoc> result = recommender.recommend(recommendContext); // 执行recommender class 里的 recommend， 并且返回结果
        if (result == null || result.size() == 0) {
            return Collections.emptyList();
        }
        RecallRanker ranker = RecommendApiConfigs.get(api).getRanker();
        if (ranker != null) {
            Timer.Context timer = taggedMetricRegistry.timer("hot-" + api + ".ranker.latency").time();
            PredictItems<DocItem> predictItems = predictItemsBuilder.buildPredictItems(result);
            ranker.rank(predictItems, recommendContext);
            result = predictItems.getItems().stream().map(i -> {
                VideoDoc videoDoc = new VideoDoc();
                videoDoc.setScore(i.getScore());
                videoDoc.setEscore(i.getPredictScore());
                videoDoc.setId(i.getId());
                return videoDoc;
            }).collect(Collectors.toList());
            timer.stop();
        }
        RecallFilter filter = RecommendApiConfigs.get(api).getFilter();
        if (filter != null) {
            filter.filter(result, recommendContext);
        }

        int resultSize = Math.min(result.size(), recommendContext.getNum());
        return result.subList(0, resultSize);
    }
}
