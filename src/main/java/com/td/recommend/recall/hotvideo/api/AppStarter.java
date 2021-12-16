package com.td.recommend.recall.hotvideo.api;

import com.github.sps.metrics.OpenTsdbReporter;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.github.sps.metrics.opentsdb.OpenTsdb;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.td.recommend.recall.hotvideo.utils.LogCleaner;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.NetworkInterface;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by liujikun on 2019/6/26.
 */
@SpringBootApplication

public class AppStarter {
    private static final Logger LOG = LoggerFactory.getLogger(AppStarter.class);
    private static final String DEFAULT_LOG_PATH = "../logs";
    public static void main(String[] args) {
        initMetrics();
        LogCleaner.getInstance().cleanLogs(DEFAULT_LOG_PATH);
        SpringApplication.run(AppStarter.class, args);
    }
    private static void initMetrics() {
        try {
            Map<String, String> tags = new HashMap<>();
            tags.put("component", "hot-recommender");
            String hostName;
            try {
                NetworkInterface eth0 = NetworkInterface.getByName("eth0");
                hostName = eth0.getInetAddresses().nextElement().getHostAddress();
            } catch (Exception e) {
                hostName = "127.0.0.1";
            }
            tags.put("host", hostName);

            Config userNewsConfig = HotVideoConfig.getInstance().getConfig();
            if (userNewsConfig.hasPath("opentsdb-address")) {
                String openTsdbAddress = userNewsConfig.getString("opentsdb-address");
                OpenTsdb openTsdb = OpenTsdb.forService(openTsdbAddress)
                        .withGzipEnabled(true)
                        .create();
                TaggedMetricRegistry metricRegistry = new TaggedMetricRegistry();
                OpenTsdbReporter.forRegistry(metricRegistry)
                        .withTags(tags)
                        .withBatchSize(20)
                        .build(openTsdb)
                        .start(10L, TimeUnit.SECONDS);
                TaggedMetricRegisterSingleton.getInstance().init(metricRegistry);
            } else {
                LOG.error("Missing opentsdb-address config for metrics");
            }
        } catch (Exception e) {
            LOG.error("init metrics failed!", e);
        }
    }
}
