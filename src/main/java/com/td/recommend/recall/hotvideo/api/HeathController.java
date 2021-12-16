package com.td.recommend.recall.hotvideo.api;


import com.alibaba.fastjson.JSONObject;
import com.td.recommend.commons.api.ApiResultBuilder;
import com.td.recommend.recall.hotvideo.utils.HeathCheckUtil;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

@RestController
@EnableAutoConfiguration
@ComponentScan("com.td.recommend.recall.hotvideo")
public class HeathController {


    @RequestMapping("/health")
    public Map<String, Object> heath(HttpServletRequest request, HttpServletResponse resp)throws IOException {
        String[] cmds = {"curl", "http://127.0.0.1:8080/recall/popular?bucket=&num=300&type=80000&key=80000"};

        HeathCheckUtil.Status status = HeathCheckUtil.heachCheck(cmds);

        if (status == HeathCheckUtil.Status.OK) {
            resp.setStatus(200);
            resp.getWriter().print("ok");
        }else {
            resp.setStatus(500);
            resp.getWriter().print("sick");
        }
        return null;
    }
}
