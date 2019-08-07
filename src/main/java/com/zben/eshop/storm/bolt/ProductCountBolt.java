package com.zben.eshop.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @DESC:
 * @author: jhon.zhou
 * @date: 2019/8/6 0006 13:41
 */
public class ProductCountBolt extends BaseRichBolt {

    /**
     * 热点数据Map
     */
    private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);
    private static final int TOP = 3;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        new Thread(new ProductCountThread()).start();
    }

    private class ProductCountThread implements Runnable {

        @Override
        public void run() {
            List<Map.Entry<Long, Long>> topnProductList = new ArrayList<>();
            while (true) {
                topnProductList.clear();

                for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                    topnProductList.add(productCountEntry);
                }

                // 比较大小，冒泡排序，取出前3个  生成最热topn的算法有很多种
                for (int i = 0; i < topnProductList.size(); i++) {
                    for (int j = 0; j < topnProductList.size()-i-1; j++) {
                        if (topnProductList.get(j).getValue() > topnProductList.get(j+1).getValue()) {
                            Map.Entry<Long, Long> big = topnProductList.get(j);
                            Map.Entry<Long, Long> small = topnProductList.get(j+1);
                            topnProductList.set(j, big);
                            topnProductList.set(j+1, small);
                        }
                    }
                }
                topnProductList = topnProductList.subList(0, topnProductList.size() > TOP ? TOP : topnProductList.size());

                Utils.sleep(10000);
            }

            /*while(true) {
                topnProductList.clear();

                int topn = 3;

                for(Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                    if(topnProductList.size() == 0) {
                        topnProductList.add(productCountEntry);
                    } else {
                        // 比较大小，生成最热topn的算法有很多种
                        // 但是我这里为了简化起见，不想引入过多的数据结构和算法的的东西
                        // 很有可能还是会有漏洞，但是我已经反复推演了一下了，而且也画图分析过这个算法的运行流程了
                        boolean bigger = false;

                        for(int i = 0; i < topnProductList.size(); i++){
                            Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);

                            if(productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                int lastIndex = topnProductList.size() < topn ? topnProductList.size() - 1 : topn - 2;
                                for(int j = lastIndex; j >= i; j--) {
                                    topnProductList.set(j + 1, topnProductList.get(j));
                                }
                                topnProductList.set(i, productCountEntry);
                                bigger = true;
                                break;
                            }
                        }

                        if(!bigger) {
                            if(topnProductList.size() < topn) {
                                topnProductList.add(productCountEntry);
                            }
                        }
                    }
                }

                Utils.sleep(60000);
            }*/
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        Long count = productCountMap.get(productId);
        if (count == null) {
            count = 0L;
        }
        count++;
        productCountMap.put(productId, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
