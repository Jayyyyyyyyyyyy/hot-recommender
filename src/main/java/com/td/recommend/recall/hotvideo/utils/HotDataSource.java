package com.td.recommend.recall.hotvideo.utils;

import com.td.data.profile.client.ItemProfileClient.ScopeEnum;
import com.td.featurestore.datasource.ItemDAO;
import com.td.featurestore.datasource.ItemDataSource;
import com.td.featurestore.item.IItem;
import com.td.featurestore.item.ItemKey;
import com.td.recommend.docstore.conf.DocItemDaoConfig;
import com.td.recommend.docstore.dao.DocItemDao;
import com.td.recommend.docstore.data.DocItem;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Liujikun on 2019/7/28.
 */
@Service
public class HotDataSource implements ItemDataSource<DocItem> {
    private DocItemDao docItemDao;

    private HotDataSource() {
        DocItemDaoConfig docItemDaoConfig = new DocItemDaoConfig();
        docItemDaoConfig.setStaticCache(true);
        docItemDaoConfig.setDynamicCache(true);
        docItemDaoConfig.setDynamicMaxsize(100000);
        docItemDaoConfig.setStaticMaxsize(100000);
        docItemDaoConfig.setDynamicInitialcapacity(100000);
        docItemDaoConfig.setStaticInitialcapacity(100000);
        docItemDao = new DocItemDao(docItemDaoConfig, (ScopeEnum[]) null);
    }

    @Override
    public Map<String, ItemDAO<? extends IItem>> getQueryDAOs() {
        Map<String, ItemDAO<? extends IItem>> queryDAOs = new HashMap<>();
        queryDAOs.put(ItemKey.doc.name(), docItemDao);
        return queryDAOs;
    }

    @Override
    public ItemDAO<DocItem> getCandidateDAO() {
        return docItemDao;
    }
}
