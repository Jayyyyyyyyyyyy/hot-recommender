package com.td.recommend.recall.hotvideo.bean;

import com.td.recommend.commons.request.RequestParamHelper;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by liujikun on 2020/6/20.
 */
@Getter
@Setter
@ToString
public class RecommendContext {
    private String api = "";
    private String type = "";
    private String key = "";
    private String bucket = "";
    private String appid = "";
    private int num;

    public String getCacheKey() {
        return String.join("_", api, type, key, bucket, String.valueOf(num),appid);
    }

    public static RecommendContext build(HttpServletRequest request) {
        RecommendContext recommendContext = new RecommendContext();
        String[] paths = request.getServletPath().split("/");
        String api = request.getServletPath().split("/")[paths.length - 1];
        recommendContext.setApi(api);
        recommendContext.setType(RequestParamHelper.getString(request, "type", ""));
        recommendContext.setKey(RequestParamHelper.getString(request, "key", ""));
        recommendContext.setBucket(RequestParamHelper.getString(request, "bucket", ""));
        recommendContext.setAppid(RequestParamHelper.getString(request, "appid", ""));
        int num = RequestParamHelper.getInt(request, "num", 200);

        recommendContext.setNum(num);
        return recommendContext;
    }
}
