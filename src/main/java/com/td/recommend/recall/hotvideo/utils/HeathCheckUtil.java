package com.td.recommend.recall.hotvideo.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HeathCheckUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HeathCheckUtil.class);

    public static String execCurl(String[] cmds){
        ProcessBuilder process = new ProcessBuilder(cmds);
        Process p;
        try {
            p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            return builder.toString();

        } catch (IOException e) {
            System.out.print("error");
            e.printStackTrace();
        }
        return null;
    }

    public static Status heachCheck(String[] cmd) {
        String result = execCurl(cmd);
        try {
            JSONObject obj = JSONObject.parseObject(result);
            int status = obj.getIntValue("status");

            Status estatus = status==0 ? Status.OK : Status.FAIL;
            return estatus;
        }catch (Exception ex) {
            LOG.error("heachCheck failed:{}",ex);
            return Status.FAIL;
        }
    }

    @Getter
    @AllArgsConstructor
    public enum Status {
        OK(0),
        FAIL(1);

        int status;
    }

}
