package com.td.recommend.recall.hotvideo.datasource;

import com.typesafe.config.Config;
import com.td.recommend.recall.hotvideo.utils.HotVideoConfig;
import org.apache.solr.client.solrj.SolrClient;

import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;


public class SolrClientSingleton {
    private static Logger LOG = LoggerFactory.getLogger(SolrClientSingleton.class);

    private static SolrClientSingleton instance = new SolrClientSingleton();

    private SolrClient solrClient;

    public static SolrClientSingleton getInstance() {
        return instance;
    }

    private SolrClientSingleton() {
//        Config config = HotVideoConfig.getInstance().getConfig();
//        try {
//            List<String> solrURLs = config.getStringList("solr-url");
//            String[] solrURLArray = new String[solrURLs.size()];
//            solrClient = new LBHttpSolrClient(solrURLs.toArray(solrURLArray));
//        } catch (MalformedURLException e) {
//            LOG.error("build solrclient failed!", e);
//        }
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }
}
