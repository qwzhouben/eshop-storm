package com.zben.eshop.storm.bolt;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @DESC: 日志解析的bolt
 * 写一个bolt，直接继承一个BaseRichBolt基类
 * 实现里面的所有的方法即可，每个bolt代码，同样是发送到worker某个executor的task里面去运行
 *
 * @author: jhon.zhou
 * @date: 2019/8/6 0006 13:34
 */
public class LogParseBolt extends BaseRichBolt {

    private OutputCollector collector;

    /**
     * 对于bolt来说，第一个方法，就是prepare方法
     * OutputCollector，这个也是Bolt的这个tuple的发射器
     */
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * execute方法
     * 就是说，每次接收到一条数据后，就会交给这个executor方法来执行
     */
    @Override
    public void execute(Tuple tuple) {
        String message = tuple.getStringByField("message");
        JSONObject messageJson = JSONObject.parseObject(message);
        JSONObject uriArgsJson = messageJson.getJSONObject("uri_args");
        Long productId = uriArgsJson.getLong("productId");
        if(productId != null) {
            collector.emit(new Values(productId));
        }
    }

    /**
     * 定义发射出去的tuple，每个field的名称
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId"));
    }
}
