package com.td.recommend.recall.hotvideo.datasource;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.typesafe.config.Config;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liujikun on 2017/12/5.
 */
public class MongoClientSingleton {
    private static Logger LOG = LoggerFactory.getLogger(MongoClientSingleton.class);
    private String DATABASE;



    private MongoClient mongoClient = null;
    private MongoCollection<Document>  collection;

    private static MongoClientSingleton instance = new MongoClientSingleton();

    public static MongoClientSingleton getInstance() {
        return instance;
    }

    private MongoClientSingleton() {
        try {
            Config hotVideoConfig = HotVideoConfig.getInstance().getConfig();
            Config clientConfig = hotVideoConfig.getConfig("mongo-config");
            String connectionsPerHost = clientConfig.getString("mongodb.connectionsPerHost");
            String maxWaitTime = clientConfig.getString("mongodb.maxWaitTime");
            String socketTimeout = clientConfig.getString("mongodb.socketTimeout");
            String connectTimeout = clientConfig.getString("mongodb.connectTimeout");
            String maxConnectionLifeTime = clientConfig.getString("mongodb.maxConnectionLifeTime");
            DATABASE= clientConfig.getString("mongodb.db");
            String collectionName = clientConfig.getString("mongodb.collection");
            MongoClientOptions options = MongoClientOptions.builder()
                    .connectionsPerHost(Integer.parseInt(connectionsPerHost))
                    .maxWaitTime(Integer.parseInt(maxWaitTime))
                    .socketTimeout(Integer.parseInt(socketTimeout))
                    .socketKeepAlive(true)
                    .maxConnectionLifeTime(Integer.parseInt(maxConnectionLifeTime))
                    .connectTimeout(Integer.parseInt(connectTimeout))
                    .readPreference(ReadPreference.secondaryPreferred())
                    .build();
            String server_list_str = clientConfig.getString("mongodb.serverlist");
            String[] serverList = server_list_str.split(",");

            List<ServerAddress> hosts = new ArrayList<>();
            for (int i = 0; i < serverList.length; i++) {
                String host = serverList[i].split(":")[0];
                int port = Integer.parseInt(serverList[i].split(":")[1]);
                ServerAddress address = new ServerAddress(host, port);
                hosts.add(address);
            }

//            mongoClient = new MongoClient(hosts, options);
//            collection =  mongoClient.getDatabase(DATABASE).getCollection(collectionName);


        } catch (Exception ex) {
            LOG.error("StaticNewsClient failed", ex);
        }
    }

    public MongoCollection<Document> getChannelHotCollection() {
        return collection;
    }

}
