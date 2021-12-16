package com.td.recommend.recall.hotvideo.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2017/6/10.
 */
public class ApplicationSharedExecutorService {
    private static volatile ApplicationSharedExecutorService instance = null;
    private ExecutorService executorService;

    public static final String THREAD_POOL_MAX_SIZE_KEY = "app-threadpool-maxsize";
    public static final String THREAD_POOL_QUEUE_SIZE_KEY = "app-threadpool-queuesize";

    public static ApplicationSharedExecutorService getInstance() {
        if (instance == null) {
            synchronized (ApplicationSharedExecutorService.class) {
                if (instance == null) {
                    instance = new ApplicationSharedExecutorService();
                }
            }
        }
        return instance;
    }

    private ApplicationSharedExecutorService() {
        Config threadConfig = HotVideoConfig.getInstance().getConfig().getConfig("threadpool-param");

        int maxPoolSize = 30;
        if (threadConfig.hasPath(THREAD_POOL_MAX_SIZE_KEY)) {
            maxPoolSize = threadConfig.getInt(THREAD_POOL_MAX_SIZE_KEY);
            System.out.println("maxPoolSize:"+ threadConfig.getInt(THREAD_POOL_MAX_SIZE_KEY));
        }

        int queueSize = 30;
        if (threadConfig.hasPath(THREAD_POOL_QUEUE_SIZE_KEY)) {
            queueSize = threadConfig.getInt(THREAD_POOL_QUEUE_SIZE_KEY);
            System.out.println("queueSize:"+ threadConfig.getInt(THREAD_POOL_MAX_SIZE_KEY));
        }

        executorService = new ThreadPoolExecutor(maxPoolSize, maxPoolSize,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new ThreadFactoryBuilder().setNameFormat("uservideo-shared-%d").build());
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
