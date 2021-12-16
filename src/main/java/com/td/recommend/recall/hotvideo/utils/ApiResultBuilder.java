package com.td.recommend.recall.hotvideo.utils;

import java.util.HashMap;
import java.util.Map;

public class ApiResultBuilder {
    private Map<String, Object> resultMap = new HashMap();

    private ApiResultBuilder() {
    }

    public static ApiResultBuilder create() {
        return new ApiResultBuilder();
    }

    public ApiResultBuilder success(Object data) {
        this.resultMap.put("data", data);
        this.resultMap.put("status", Status.success.getCode());
        return this;
    }

    public ApiResultBuilder failure(String errorMsg) {
        this.resultMap.put("status", Status.failure.getCode());
        this.resultMap.put("errorMsg", errorMsg);
        return this;
    }

    public Map<String, Object> build() {
        return this.resultMap;
    }

    private enum Status {
        success(0),
        failure(-1);

        private int code;

        private Status(int code) {
            this.code = code;
        }

        public int getCode() {
            return this.code;
        }
    }
}