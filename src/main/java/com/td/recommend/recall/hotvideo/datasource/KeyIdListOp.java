package com.td.recommend.recall.hotvideo.datasource;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KeyIdListOp {
    private static final Logger LOG = LoggerFactory.getLogger(KeyIdListOp.class);

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Getter
    public static Map<String, List<String>>  keyValuesList = new ConcurrentHashMap<>();
    private static final RedisClientSingleton redis = RedisClientSingleton.general;
    static {
        loadData();
        scheduledExecutorService.scheduleAtFixedRate(KeyIdListOp::loadData, 0, 10, TimeUnit.MINUTES);
    }

    //vshape_eu  vyoga_eu
    private static void loadData() {
        addData(redis.lrange("op:vyoga_eu",0,-1), keyValuesList, "vyoga_eu");
        addData(redis.lrange("op:vshape_eu",0,-1), keyValuesList, "vshape_eu");
        addData(redis.lrange("op:vshuffle_eu",0,-1), keyValuesList, "vshuffle_eu");
        addData(redis.lrange("op:vjittebug_eu",0,-1), keyValuesList, "vjittebug_eu");
        addData(redis.lrange("op:vshuffle_eu",0,-1), keyValuesList, "vshuffle_eu_rlvt");
        addData(redis.lrange("op:vjittebug_eu",0,-1), keyValuesList, "vjittebug_eu_rlvt");
    }

    private static void addData(List<String> idlist, Map<String, List<String>> keyValuesList, String key) {
        keyValuesList.put(key, idlist);
        LOG.info("add data key:{}, size:{}",key, idlist.size());
    }



    public static void main(String[] argv) {
        List<String> vshape_eu_list = Arrays.asList("1500678497736","1500678697094","1500678416492","1500678554006","1500678697301","1500674186922","1500678467281","1500678423595","1500674453555","1500675192951","1500674567555","1500678485071","1500674559089","1500678424287","1500678423599","1500678413726","1500675321191","1500678036827","1500675820646","1500678036683","1500678416305","1500678553276","1500678225236","1500678468727","1500678612242","1500678036831","1500675041736","1500678423596","1500678416491","1500678045328","1500678036672","1500678416303","1500678497858","1500677997439","1500678416489","1500678416307","1500677965954","1500678431834","1500678045412","1500678437935");

 //       redis.expire("op:vshape_eu", 1);
//        redis.expire("op:vjittebug_eu", 1);
        //redis.expire("op:vyoga_eu", 1);

        List<String> vyoga_eu_list =  Arrays.asList("1470712189","1500672894405","1485788085","1500670140637","8444743","1500673437720","1500670818542","1500677985955","1500673411545","1500676757665","1500672680067","3823072","1500674764842","1500678131315","1500677985950","1500678466397","1500668849217","1027751","9482682");


        //List<String> vshuffle_eu_list =  Arrays.asList("8901258","9331822","9372127","9109557","9443238","9340112","9171027","7606124","1500661085000","9464535","9150278","1500661748222","9155146","1500663345312","9288867","1500660522475","9438556","1500668296301","9162273","7596087","1500673158054");
        //List<String> vshuffle_eu_list =  Arrays.asList("1500678324215","1500678471441","1500678461029","1500661751212","1500677601641","1500668170976","9256072","1500665724516","1500678152214","1500676573874","1500665457068","1500675205936","1500672587008","1500668409020","1500677667178","1500677933725","1500675507363","1500677806301","1500677223452","1500676998960","1500676980108","1500670929387","1500671853819","1500677903035","1500678463719","1500678068347","1500668324870","1500677642560","1500675337462","1500678365920","1500674066783","1500678166622");
        //List<String> vjittebug_eu_list = Arrays.asList("1500678220210","1500678254334","1500678124271","1500678103854","1500678469469","1500678381172","1500678483465","1500678479423","1500678453337","1500678116373","1500678162476","1500678153740","1500678400405","1500678445966","1500678391277","1500678337286","1500678197939","1500678185003","1500678263991","1500678169336","1500678141249","1500678218535","1500678231263","1500678458504","1500678441198","1500678185543","1500678205848","1500678192980");

        try {
            Thread.sleep(2000);
        }catch (Exception ex) {

        }
    //redis.rpush("op:vshape_eu", vshape_eu_list);

        try {
            Thread.sleep(15000);
        }catch (Exception ex) {

        }
        List<String> result =  keyValuesList.get("vshape_eu");
        //List<String> result1 = keyValuesList.get("vjittebug_eu");

        System.out.println(String.join(",",result));
        //System.out.println(String.join(",", result1));
        //System.out.println(String.join(",",result1));

    }
}
