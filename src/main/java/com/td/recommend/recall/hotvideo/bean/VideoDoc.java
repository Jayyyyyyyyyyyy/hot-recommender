package com.td.recommend.recall.hotvideo.bean;


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
public class VideoDoc {
    private String id;
    private String title;
    private double score;
    private double escore;
    private String publishTime;
    private String cat;
    private String subcat;
    private String subcatId;
    private String subcatName;
    private String channelId;

    private Map<String, String> tags;

    private Map<String, Double> features;
    private List<String> attach;

    public VideoDoc() { }

    public VideoDoc(String id) {
        this.id = id;
    }
}
