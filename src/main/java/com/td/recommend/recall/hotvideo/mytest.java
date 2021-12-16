package com.td.recommend.recall.hotvideo;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;



import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.distribution.BetaDistribution;

public class mytest {
    public static void main(String[] args) {
        List<Integer> classTime = new ArrayList<Integer>();
        List<Integer> attendClassTime = new ArrayList<Integer>();
        classTime.add(2);
        classTime.add(1);
        classTime.add(3);
        attendClassTime.add(3);
        attendClassTime.add(1);
        attendClassTime.add(2);
        attendClassTime.add(2);
        attendClassTime.add(5);
        classTime.addAll(attendClassTime);
        classTime = new ArrayList<Integer>(new LinkedHashSet<Integer>(classTime));//去重保持当前排序
        System.out.println(classTime.toString());



    }
}

