package com.td.recommend.recall.hotvideo.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2017/6/19.
 */
public class LogCleaner {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    private static final int DEFAULT_LOG_RETENTION_DAYS = 3;
    private static LogCleaner instance = new LogCleaner();

    public static LogCleaner getInstance() {
        return instance;
    }

    public void cleanLogs(String logPath) {

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        LocalDateTime dateTime = LocalDateTime.now().minusDays(DEFAULT_LOG_RETENTION_DAYS);

                        List<String> dateFormats = Arrays.asList(
                                dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                                dateTime.format(DateTimeFormatter.ofPattern("yyyy_MM_dd"))
                        );

                        String glob = "glob:*{" + StringUtils.join(dateFormats, ",") + "}*";
                        LOG.info("start to do log clean job");
                        cleanLogs(logPath, glob);
                    } catch (IOException e) {
                        LOG.error("clean log failed...");
                    }
                }, 0, 1, TimeUnit.DAYS);

    }

    private void cleanLogs(String logPath, String glob) throws IOException {
        final PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(glob);
        Files.walkFileTree(Paths.get(logPath), new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (pathMatcher.matches(file.getFileName())) {
                    LOG.info("log_retention, delete file:" + file.toAbsolutePath());
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
