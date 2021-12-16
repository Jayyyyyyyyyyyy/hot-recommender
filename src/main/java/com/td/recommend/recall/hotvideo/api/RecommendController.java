package com.td.recommend.recall.hotvideo.api;

import com.codahale.metrics.Timer;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.google.common.cache.Cache;
import com.td.recommend.commons.api.ApiConstants;
import com.td.recommend.commons.api.ApiResultBuilder;
import com.td.recommend.commons.metrics.TaggedMetricRegisterSingleton;
import com.td.recommend.recall.hotvideo.bean.RecommendContext;
import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@RestController
@EnableAutoConfiguration
@ComponentScan("com.td.recommend.recall.hotvideo")
@RequestMapping("/recall")
public class RecommendController {
    private static final Logger LOG = LoggerFactory.getLogger(RecommendController.class);

    @Autowired
    private RecommenderService recommendService;

    @RequestMapping("*")
    public Map<String, Object> recommend(HttpServletRequest request) {
        TaggedMetricRegistry taggedMetricRegistry = TaggedMetricRegisterSingleton.getInstance().getTaggedMetricRegistry(); // ？
        RecommendContext recommendContext = RecommendContext.build(request); //解析请求，获取全部参数
        String api = recommendContext.getApi();
        try {
            taggedMetricRegistry.meter("hot-" + api + ".request.qps").mark();
            Timer.Context time = taggedMetricRegistry.timer("hot-" + api + ".request.latency").time();
            long startTime = System.currentTimeMillis();

            String cacheKey = recommendContext.getCacheKey();
            Cache<String, Map<String, Object>> realCache = RecommendApiConfigs.get(api).getCache(); //获取cache
            Map<String, Object> resultMap = Optional.ofNullable(realCache).map(cache -> cache.getIfPresent(cacheKey)).orElse(null);
            if (resultMap != null) {  //如果有cache，直接获取
                List<?> resultList = (List<?>) resultMap.get(ApiConstants.DATA);
                taggedMetricRegistry.histogram("hot-" + api + ".result.size").update(resultList.size());
                taggedMetricRegistry.histogram("hot-" + api + ".cache.hitrate").update(100);
                LOG.info("{} result from cache, size {}", recommendContext, resultList.size());
                return resultMap;
            }

            taggedMetricRegistry.histogram("hot-" + api + ".cache.hitrate").update(0);
            List<VideoDoc> resultList = recommendService.recommend(recommendContext); //执行，并且返回VideoDoc

            if (resultList.size() > 0) {
                resultMap = ApiResultBuilder.create().success(resultList).build(); // 变成json 返回
                taggedMetricRegistry.histogram("hot-" + api + ".recall.failrate").update(0);
            } else {
                resultMap = ApiResultBuilder.empty();
                LOG.warn("{} no result", recommendContext);
                taggedMetricRegistry.histogram("hot-" + api + ".recall.failrate").update(100);
            }
            if (realCache != null && resultList.size()>0) {
                realCache.put(cacheKey, resultMap);
            }
            time.stop();
            long endTime = System.currentTimeMillis() - startTime;
            LOG.info("{} result size: {}, cost {} ms", recommendContext, resultList.size(), endTime);
            taggedMetricRegistry.histogram("hot-" + api + ".result.size").update(resultList.size());
            return resultMap;
        } catch (Exception e) {
            LOG.error("{} result failed!", recommendContext, e);
            taggedMetricRegistry.histogram("hot-" + api + ".result.size").update(0);
            return ApiResultBuilder.create().failure(e.getMessage()).build();
        }
    }

}
