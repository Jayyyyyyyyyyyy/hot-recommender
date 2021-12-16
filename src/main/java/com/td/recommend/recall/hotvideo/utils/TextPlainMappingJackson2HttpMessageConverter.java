package com.td.recommend.recall.hotvideo.utils;

import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zjl on 2019/7/4.
 */

public class TextPlainMappingJackson2HttpMessageConverter extends MappingJackson2HttpMessageConverter {
    public TextPlainMappingJackson2HttpMessageConverter(){
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.TEXT_PLAIN);
        mediaTypes.add(MediaType.TEXT_HTML);
        setSupportedMediaTypes(mediaTypes);
    }
}
