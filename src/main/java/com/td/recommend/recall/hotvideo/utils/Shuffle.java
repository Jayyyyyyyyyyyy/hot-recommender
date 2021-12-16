package com.td.recommend.recall.hotvideo.utils;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Shuffle {
    public static List<String> listShuffle(List<String> list) {
        int size = list.size();
        Random random = new Random();
        for(int i = 0; i < size; i++) {
            int randomPos = random.nextInt(size);
            Collections.swap(list, i, randomPos);
        }
        return list;
    }
}
