package com.td.recommend.recall.hotvideo.bean;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;


@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResDoc {
    private Integer status;
    //private Boolean cache_hit;
    //private String host;
    //private String msg;
    //private Integer num;
    //private Integer tid;
    //private Integer time_cost;
    private List<VideoDoc> data;
}
