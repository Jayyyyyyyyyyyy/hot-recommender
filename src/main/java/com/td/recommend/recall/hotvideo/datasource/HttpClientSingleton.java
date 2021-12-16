package com.td.recommend.recall.hotvideo.datasource;

import com.td.recommend.recall.hotvideo.bean.ResDoc;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import com.typesafe.config.Config;
import org.apache.http.HttpHeaders;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class HttpClientSingleton {
    private static final Logger log = LoggerFactory.getLogger(HttpClientSingleton.class);
    public static HttpClientSingleton instance = new HttpClientSingleton();
    private RestTemplate restTemplate;

    public static HttpClientSingleton getInstance() {
        return instance;
    }

    public HttpClientSingleton() {
        Config httpServerConf = HotVideoConfig.getInstance().getConfig().getConfig("http-param");
        int maxperout = httpServerConf.getInt("http.maxperroute");
        int readtimeout = httpServerConf.getInt("http.readtimeout");
        int connesttimeout = httpServerConf.getInt("http.connecttimeout");

        PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();
        connMgr.setDefaultMaxPerRoute(maxperout);
        BasicHeader header = new BasicHeader(HttpHeaders.CONNECTION, "close");
        CloseableHttpClient httpClient = HttpClientBuilder.create()
                .setConnectionManager(connMgr)
                .setDefaultHeaders(Collections.singleton(header))
                .build();

        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(readtimeout);
        requestFactory.setConnectTimeout(connesttimeout);

        restTemplate = new RestTemplate(requestFactory);
    }

    public <T> T request(String url, Class<T> entity) {
        return restTemplate.getForObject(url, entity);

    }

    public static void main(String[] args) {

//        for (int i = 0; i < 300; i++)
//            try {
//                ResDoc x = HttpClientSingleton.getInstance().request("http://10.42.36.120:8083/search?key=867226030133089&num=10", ResDoc.class);
////                ResDoc y = HttpClientSingleton.getInstance().request("http://10.42.167.180:8080/recall/highctr?num=100&cid=80000", ResDoc.class);
//                Integer size = x.getData().size();
//                System.out.println(size);
//
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//    }
//        IntStream.range(0, 100).parallel().forEach(i -> {
//            try {
//                ResDoc x = HttpClientSingleton.getInstance().request("http://10.42.36.120:8083/search?key=867226030133089&num=10", ResDoc.class);
//                Integer size = x.getData().size();
//                System.out.println(size);
//
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//        });

        ExecutorService executorService = Executors.newFixedThreadPool(100);
        IntStream range = IntStream.range(0, 100);
        long l = System.currentTimeMillis();
        range.forEach(k -> {
            executorService.execute(() -> {
                try {
                    ResDoc x = HttpClientSingleton.getInstance().request("http://10.42.52.68:8088/vector/cluster?diu=A100004C4BD4DF&leader=random&num=100", ResDoc.class);
                    System.out.println(x.getData());
                } catch (Exception e) {
                    e.printStackTrace();

                }
            });
        });
        System.out.println("ljk"+(System.currentTimeMillis()-l));

    }
}
