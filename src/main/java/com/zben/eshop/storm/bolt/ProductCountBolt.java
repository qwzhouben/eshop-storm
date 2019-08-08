package com.zben.eshop.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.zben.eshop.storm.zk.ZookeeperSession;
import http.HttpClientUtils;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class ProductCountBolt extends BaseRichBolt {

    /**
     * 热点数据Map
     */
    private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);
    private static final int TOP = 3;
    private ZookeeperSession zkSession;
    private int taskid;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        zkSession = ZookeeperSession.getInstance();
        new Thread(new ProductCountThread()).start();
        new Thread(new HotProductThread()).start();
        taskid = context.getThisTaskId();
        log.info("【ProductCountBolt获取到taskid】taskid=" + taskid);
        /**
         * 1. 将自己的taskid写入一个zookeeper node中，形成taskid的列表
         * 2. 然后每次都将自己的热门商品列表，写入自己的taskid对应的zookeeper节点
         * 3. 这样的话，并行的预热程序才能从第一步中知道，有哪些taskid
         * 4. 然后哦并行预热程序根据每个taskid去获取一个锁，然后再从对应的znode中拿到热门商品
         */
        initTaskId(taskid);
    }

    private void initTaskId(int thisTaskId) {
        /**
         * ProductCountBolt所有的taskid启动的时候，都会将自己的taskid写到同一个node的值中
         * 格式就是逗号分隔，拼接成一个列表
         * 111,211,355
         */
        zkSession.acquireDistributedLock();

        zkSession.createNode("/taskid-list");
        String taskidList = zkSession.getNodeData("/taskid-list");
        log.info("【ProductCountBolt获取到node  taskid list】taskidList=" + taskidList);

        if (!"".equals(taskidList)) {
            taskidList += "," + thisTaskId;
        } else {
            taskidList += thisTaskId;
        }
        zkSession.setNodeData("/taskid-list", taskidList);
        log.info("【ProductCountBolt设置node taskid list】taskidList=" + taskidList);

        zkSession.releaseDistributedLock();
    }

    private class ProductCountThread implements Runnable {

        @Override
        public void run() {
            List<Map.Entry<Long, Long>> topnProductList = new ArrayList<>();
            List<Long> productList = new ArrayList<>();
            while (true) {
                try {
                    topnProductList.clear();
                    productList.clear();
                    if (productCountMap.size() == 0) {
                        Utils.sleep(200);
                        continue;
                    }
                    log.info("【ProductCountThread打印productCountMap的长度】size=" + productCountMap.size());
                    for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                        topnProductList.add(productCountEntry);
                    }
                    log.info("【统计前的topnProductList={}】", topnProductList);
                    // 比较大小，冒泡排序，取出前3个  生成最热topn的算法有很多种
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
                    topnProductList = topnProductList.subList(0, topnProductList.size() > TOP ? TOP : topnProductList.size());
                    log.info("【统计后的topnProductList={}】", topnProductList);
                    //获取到一个topn list
                    for (Map.Entry<Long, Long> longLongEntry : topnProductList) {
                        productList.add(longLongEntry.getKey());
                    }
                    log.info("【productList={}】", productList);
                    String topnProductListJSON = JSONArray.toJSONString(productList);

                    zkSession.createNode("/task-hot-product-list-" + taskid);
                    zkSession.setNodeData("/task-hot-product-list-" + taskid, topnProductListJSON);
                    log.info("【ProductCountThread计算出一份top3热门商品列表】zk path=" + ("/task-hot-product-list-" + taskid) + ", topnProductListJSON=" + topnProductListJSON);
                    Utils.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
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

    private class HotProductThread implements Runnable {
        List<Map.Entry<Long, Long>> topnProductList = new ArrayList<>();
        List<Long> hotProductIdList = Lists.newArrayList();
        List<Long> lastTimeHotProductIdList = Lists.newArrayList();

        @Override
        public void run() {
            while (true) {
                /**
                 * 1. 将LRUMap中的数据按照访问次数，进行全局排序
                 * 2. 计算95%的商品的访问次数平均值
                 * 3. 遍历排序后的商品访问次数，从最大的开始
                 * 4. 如果某个商品它的访问量是平均值的10倍，就认为是缓存热点
                 */
                try {
                    topnProductList.clear();
                    hotProductIdList.clear();

                    if (productCountMap.size() == 0) {
                        Utils.sleep(200);
                        continue;
                    }
                    log.info("【ProductCountThread打印productCountMap的长度】size=" + productCountMap.size());
                    for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                        topnProductList.add(productCountEntry);
                    }
                    log.info("【排序前的topnProductList={}】", topnProductList);
                    // 比较大小，冒泡排序
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
                    log.info("【排序后的topnProductList={}】", topnProductList);

                    //2.计算95%的商品的访问次数平均值
                    long calculateCount = Math.round(topnProductList.size() * 0.95);
                    long totalCount = 0l;
                    for (int i =topnProductList.size() - 1; i >= topnProductList.size() - calculateCount; i--) {
                        totalCount += topnProductList.get(i).getValue();
                    }
                    long avgCount = totalCount / calculateCount;
                    log.info("【95%商品的平均值】avgCount={}", avgCount);

                    //3. 从第一个元素开始遍历，判断是否是平均值得10倍
                    for (Map.Entry<Long, Long> entry : topnProductList) {
                        if (avgCount * 10 < entry.getValue()) {
                            log.info("【HotProductThread发现一个热点】entry={}", entry);
                            hotProductIdList.add(entry.getKey());

                            //将缓存热点反向推送到流量分发的nginx中
                            HttpClientUtils.sendGetRequest("http://192.168.2.99/hot?productId=" + entry.getKey());

                            //将缓存热点，那个商品对应的完整的缓存数据，发送请求到缓存服务去获取，反向推送到所有的后端应用nginx服务器上去
                            String resp = HttpClientUtils.sendGetRequest("http://192.168.2.217:8080/getProductInfo?productId=" + entry.getKey());

                            String[] appNginxURLs = new String[]{
                                "http://192.168.3.105/hot?productId=" + entry.getKey() + "&productInfo=" + resp,
                                    "http://192.168.3.82/hot?productId=" + entry.getKey() + "&productInfo=" + resp
                            };

                            for (String appNginxURL : appNginxURLs) {
                                HttpClientUtils.sendGetRequest(appNginxURL);
                            }
                        }
                    }

                    //4. 热点缓存消失的实时自动识别和感知
                    if (lastTimeHotProductIdList.size() == 0) {
                        if (hotProductIdList.size() > 0) {
                            lastTimeHotProductIdList.addAll(hotProductIdList);
                        }
                        log.info("【HotProductThread保存上次热点数据】lastTimeHotProductIdList=" + lastTimeHotProductIdList);
                    } else {
                        for (Long productId : lastTimeHotProductIdList) {
                            if (!hotProductIdList.contains(productId)) {
                                log.info("【HotProductThread发现一个热点消失了】productId=" + productId);
                                // 说明上次的那个商品id的热点，消失了
                                // 发送一个http请求给到流量分发的nginx中，取消热点缓存的标识
                                HttpClientUtils.sendGetRequest("http://192.168.2.99/cancel_hot?productId="+productId);
                            }
                        }
                        lastTimeHotProductIdList.clear();
                        if (hotProductIdList.size() > 0) {
                            lastTimeHotProductIdList.addAll(hotProductIdList);
                        }
                        log.info("【HotProductThread保存上次热点数据】lastTimeHotProductIdList=" + lastTimeHotProductIdList);
                    }

                    Utils.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");

        log.info("【ProductCountBolt接收到一个商品id】 productId=" + productId);
        Long count = productCountMap.get(productId);
        if (count == null) {
            count = 0L;
        }
        count++;
        productCountMap.put(productId, count);
        log.info("【ProductCountBolt完成商品访问次数统计】productId=" + productId + ", count=" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
