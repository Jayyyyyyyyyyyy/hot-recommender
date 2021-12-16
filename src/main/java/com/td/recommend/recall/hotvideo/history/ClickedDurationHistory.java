package com.td.recommend.recall.hotvideo.history;

import com.td.recommend.FilterDurationServer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class ClickedDurationHistory {
    private static final Logger LOG = LoggerFactory.getLogger(ClickedDurationHistory.class);

    public static ClickedDurationHistory instance = new ClickedDurationHistory();
    private final FilterDurationServer filterDurationServer;


    public static ClickedDurationHistory getInstance() {
        return instance;
    }

    public ClickedDurationHistory() {
        filterDurationServer = new FilterDurationServer();
    }

    public List<ImmutablePair<String, Double>> clickedDuration(String userId) {
        List<ImmutablePair<String, Double>> loadedList = filterDurationServer.getDurationPairs(userId);
        if(loadedList == null){
            return Collections.emptyList();
        }
        return loadedList;
    }
    public static void main(String[] args) {
        System.out.println("zjl"+ClickedDurationHistory.getInstance().clickedDuration("860529030531005"));

    }
}
