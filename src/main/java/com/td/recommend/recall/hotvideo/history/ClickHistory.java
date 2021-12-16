package com.td.recommend.recall.hotvideo.history;

import com.td.recommend.FilterClickServer;
import com.td.recommend.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by admin on 2017/6/22.
 */
public class ClickHistory {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHistory.class);

    public static ClickHistory instance = new ClickHistory();
    private final FilterClickServer clickServer;

    public static ClickHistory getInstance() {
        return instance;
    }

    public ClickHistory() {
        clickServer = new FilterClickServer(false);
    }

    public List<String> clicked(String userId) {
        FilterList loadedList = clickServer.getLoadedList(userId);
        if(loadedList == null){
            return Collections.emptyList();
        }
        return loadedList.getList();
    }
    public static void main(String[] args) {
        System.out.println("ljk"+ClickHistory.getInstance().clicked("860529030531005"));

    }
}
