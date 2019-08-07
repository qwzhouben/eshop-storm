package com.zben.eshop.storm.spout;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @DESC: kafka消费数据的spout
 * spout，继承一个基类，实现接口，这个里面主要是负责从数据源获取数据
 * 我们这里作为一个简化，就不从外部的数据源去获取数据了，只是自己内部不断发射一些句子
 * @author: jhon.zhou
 * @date: 2019/8/6 0006 11:41
 */
@Slf4j
public class AccessLogKafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);

    /**
     * open方法，是对spout进行初始化的
     * 比如说，创建一个线程池，或者创建一个数据库连接池，或者构造一个httpclient
     */
    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        // 在open方法初始化的时候，会传入进来一个东西，叫做SpoutOutputCollector
        // 这个SpoutOutputCollector就是用来发射数据出去的
        this.collector = collector;
        startKafkaConsumer();
    }

    private void startKafkaConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.3.105:2181,192.168.3.82:2181,192.168.2.99:2181");
        properties.put("group.id", "eshop-cache-group");
        properties.put("zookeeper.session.timeout.ms", "40000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        String topic = "access-log";

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (KafkaStream stream : streams) {
            new Thread(new KafkaMessageProcessor(stream)).start();
        }
    }

    private class KafkaMessageProcessor implements Runnable {

        private KafkaStream kafkaStream;

        public KafkaMessageProcessor(KafkaStream kafkaStream) {
            this.kafkaStream = kafkaStream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
            while (it.hasNext()) {
                String message = new String(it.next().message());
                log.info("【AccessLogKafkaSpout中的Kafka消费者接收到一条日志】message=" + message);
                try {
                    queue.put(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * nextTuple方法
     * 这个spout类，之前说过，最终会运行在task中，某个worker进程的某个executor线程内部的某个task中
     * 那个task会负责去不断的无限循环调用nextTuple()方法
     * 只要的话呢，无限循环调用，可以不断发射最新的数据出去，形成一个数据流
     */
    @Override
    public void nextTuple() {
        if (queue.size() > 0) {
            try {
                String message = queue.take();
                collector.emit(new Values(message));
                log.info("【AccessLogKafkaSpout发射出去一条日志】message=" + message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }

    /**
     * declareOutputFields这个方法
     * 很重要，这个方法是定义一个你发射出去的每个tuple中的每个field的名称是什么
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
