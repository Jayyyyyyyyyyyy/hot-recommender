package com.td.recommend.recall.hotvideo.filter;

import com.td.recommend.recall.hotvideo.bean.VideoDoc;
import com.td.recommend.recall.hotvideo.datasource.RedisClientSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * create by pansm at 2019/09/06
 */
public class HeadPoolFilter {
    private static final Logger LOG = LoggerFactory.getLogger(HeadPoolFilter.class);

    private static HeadPoolFilter instance  = new HeadPoolFilter();

    private HeadPoolFilter() {

    }

    public static HeadPoolFilter getInstance() {
        return instance;
    }


    public Set<String> filter(List<String> idlist) {
        RedisClientSingleton client = RedisClientSingleton.headpoolFilter;
        Set<String> resultSet = new HashSet<>();
        try {
            String[] idarr = new String[idlist.size()];
            idlist.toArray(idarr);
            List<String> resultlist = client.mget(idarr);
            if (resultlist!=null) {
                for (int i=0;i<resultlist.size();i++) {
                    String value = resultlist.get(i);
                    if (value!=null) {
                        resultSet.add(idlist.get(i));
                    }
                }
               return resultSet;
            }
        }catch(Exception ex){
            LOG.error("get idlist failed,"+ex.toString(),ex);
        }
        return resultSet;
    }

    public List<VideoDoc> filterex(List<VideoDoc> beforeFilterList,int num,String context) {
        long starttime = System.currentTimeMillis();
        RedisClientSingleton client = RedisClientSingleton.headpoolFilter;
        List<String> idlist = beforeFilterList.stream().map(t->t.getId()).collect(Collectors.toList());
        List<VideoDoc> resultVideoDocs = new ArrayList<>();
        int count = 0;
        try {
            String[] idarr = new String[idlist.size()];
            idlist.toArray(idarr);
            List<String> resultlist = client.mget(idarr);
            if (resultlist!=null) {
                for (int i=0; i<resultlist.size();i++) {
                    String value = resultlist.get(i);
                    if (value!=null) {
                        resultVideoDocs.add(beforeFilterList.get(i));
                        count++;
                    }
                    count++;
                    if (count>=num) {
                        break;
                    }
                }
            }

        }catch (Exception ex) {
            LOG.error("get idlist failed,"+ex.toString(),ex);
        }
        LOG.info("context:{},before size:{},after filter size:{},filter time:{}",context,beforeFilterList.size(),resultVideoDocs.size(),System.currentTimeMillis()-starttime);
        return resultVideoDocs;
    }


    public static void main(String[] argv) {
        List<String> idlist = new ArrayList<>();
//        idlist.add("1500670147704");
//        idlist.add("1500668333547");
//        idlist.add("1500670149859");
//        idlist.add("111");

        Set<String> resultSet = HeadPoolFilter.getInstance().filter(idlist);

        for (String key : resultSet) {
            System.out.println(key);
        }

    }


}
