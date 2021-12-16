package com.td.recommend.recall.hotvideo.utils;

import com.td.recommend.commons.item.PredictItem;
import com.td.recommend.commons.item.PredictItems;
import com.td.recommend.docstore.data.DocItem;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
public class PredictItemsBuilder {
    @Autowired
    private HotDataSource hotDataSource;
    private static final Logger LOG = LoggerFactory.getLogger(PredictItemsBuilder.class);

    public PredictItems<DocItem> buildPredictItems(List<VideoDoc> videos) {
        List<PredictItem<DocItem>> predictItems = new ArrayList<>();
        List<String> itemIds = videos.stream().map(VideoDoc::getId).collect(Collectors.toList());
        List<Optional<DocItem>> itemOpts = hotDataSource.getCandidateDAO().parallelGet(itemIds);
        Map<String, Double> idScores = videos.stream().collect(Collectors.toMap(VideoDoc::getId, VideoDoc::getScore,(oldValue,newValue)->newValue));
        for (Optional<DocItem> itemOpt : itemOpts) {
            if (itemOpt.isPresent()) {
                PredictItem<DocItem> predictItem = new PredictItem<>();
                Double score = idScores.getOrDefault(itemOpt.get().getId(), 0.0);
                predictItem.setScore(score);
                predictItem.setPredictScore(score);
                predictItem.setId(itemOpt.get().getId());
                predictItem.setItem(itemOpt.get());
                predictItems.add(predictItem);
            }
        }

        int failSize = itemIds.size() - predictItems.size();
        if (failSize > 0) {
            LOG.warn("buildPredictItems failed size={}", failSize);
        }

        PredictItems<DocItem> result = new PredictItems<>();
        result.setItems(predictItems);
        return result;
    }
}
