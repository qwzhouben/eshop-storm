package com.zben.eshop;

import static org.junit.Assert.assertTrue;

import org.apache.storm.trident.util.LRUMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);

    @Test
    public void shouldAnswerWithTrue() {

        List<Map.Entry<Long, Long>> topnProductList = new ArrayList<>();
        int top = 3;

        productCountMap.put(1L, 4L);
        //productCountMap.put(2l, 5l);
   /*     productCountMap.put(3l, 8l);
        productCountMap.put(4l, 3l);
        productCountMap.put(5l, 7l);
        productCountMap.put(6l, 9l);*/

        for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
            topnProductList.add(productCountEntry);
        }

        for (int i = 0; i < topnProductList.size(); i++) {
            for (int j = 0; j < topnProductList.size()-i-1; j++) {
                if (topnProductList.get(j).getValue() < topnProductList.get(j+1).getValue()) {
                    Map.Entry<Long, Long> big = topnProductList.get(j+1);
                    Map.Entry<Long, Long> small = topnProductList.get(j);
                    topnProductList.set(j, big);
                    topnProductList.set(j+1, small);
                }
            }
        }
        topnProductList = topnProductList.subList(0, topnProductList.size() > top ? top : topnProductList.size());

        System.out.println(topnProductList);
    }
}
