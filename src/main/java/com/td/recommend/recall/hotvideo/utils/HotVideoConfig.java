package com.td.recommend.recall.hotvideo.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by fuliangliang on 2017/6/20.
 */
public class HotVideoConfig {
    private volatile static HotVideoConfig instance = new HotVideoConfig();

    private Config config;
    private Config rootConfig;

    public static HotVideoConfig getInstance() {
        return instance;
    }

    private HotVideoConfig() {
        rootConfig = ConfigFactory.load();
        config = rootConfig.getConfig("hotvideo");
    }

    public Config getConfig() {
        return config;
    }


    public Config getRootConfig() {
        return rootConfig;
    }
}
