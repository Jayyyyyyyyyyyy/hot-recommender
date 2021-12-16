package com.td.recommend.recall.hotvideo.utils;

import com.typesafe.config.Config;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Liujikun on 2019/6/20.
 */
public class ESClientSingleton {
    private static Logger LOG = LoggerFactory.getLogger(ESClientSingleton.class);

    private static ESClientSingleton instance = new ESClientSingleton();

    private static RestHighLevelClient client;

    public static ESClientSingleton getInstance() {
        return instance;
    }

    private ESClientSingleton() {
        Config config = HotVideoConfig.getInstance().getConfig();
        try {
            Config esConf = config.getObject("es-config").toConfig();
            String host = esConf.getString("host");
            int port = esConf.getInt("port");
            String schema = esConf.getString("schema");
            RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, port, schema))
                    .setMaxRetryTimeoutMillis(500)
                    .setHttpClientConfigCallback(httpClientBuilder ->
                            httpClientBuilder.setDefaultIOReactorConfig(
                                    IOReactorConfig.custom()
                                            .setConnectTimeout(50)
                                            .setSoKeepAlive(true)
                                            .setSoTimeout(300)
                                            .build())
                                    .setKeepAliveStrategy((r, c) -> 2 * 60 * 1000));

            client = new RestHighLevelClient(restClientBuilder);
        } catch (Exception e) {
            LOG.error("build es client failed!", e);
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public static void main(String[] args) {
        long freshDay = System.currentTimeMillis() - 7 * 24 * 3600 * 1000L;


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort(new FieldSortBuilder("ctime").order(SortOrder.DESC));
        searchSourceBuilder.query(QueryBuilders.boolQuery()
//                .filter(QueryBuilders.termQuery("firstcat.tagid","264"))
//                .filter(QueryBuilders.termQuery("cstatus","0"))
                        .filter(QueryBuilders.termQuery("cstage", "7"))
                        .filter(QueryBuilders.termsQuery("talentstar", Arrays.asList(4, 5, 6)))
                        .filter(QueryBuilders.rangeQuery("ctime").gt(freshDay))
        );
        //searchSourceBuilder.fetchSource(ESRequestBuilder.fetchSource, null);
        SearchRequest searchRequest = new SearchRequest("portrait_video").types("video");
        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest);

        try {
            SearchResponse search = ESClientSingleton.getInstance().getClient()
                    .search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(search);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
}
