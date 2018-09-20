package com.alibaba.jstorm.hbase.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.sumpay.hbase.HBaseClient;
import com.alibaba.jstorm.hbase.config.HbaseConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * @author heyc
 * @description
 * @date 2018/9/13 16:42
 */
public abstract class AbstractHbaseBolt extends BaseRichBolt {

    protected static final String HBASE_CONFIG_PERFIX = "hbase.";

    protected OutputCollector collector;

    protected HBaseClient hbaseClient;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        hbaseClient = new HBaseClient(getHbaseConfiguration(stormConf));
    }

    /**
     * getHbaseConfiguration
     * @param stormConf
     * @return
     */
    protected Configuration getHbaseConfiguration(Map stormConf) {
        Configuration conf = new Configuration();
        Iterator<Map.Entry> iterator = stormConf.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = iterator.next();
            if (entry.getKey().toString().startsWith(HBASE_CONFIG_PERFIX)) {
                conf.set(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        if (conf.get(HbaseConfig.HBASE_ZOOKEEPER_PORT) == null) {
            conf.set(HbaseConfig.HBASE_ZOOKEEPER_PORT, "2181");
        }
        if (conf.get(HbaseConfig.HBASE_ZOOKEEPER_PARENT) == null) {
            conf.set(HbaseConfig.HBASE_ZOOKEEPER_PARENT, "/hbase");
        }
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        execute(input, collector, hbaseClient);
    }

    /**
     * execute
     * @param input
     * @param collector
     * @param hbaseClient
     */
    public abstract void execute(Tuple input, OutputCollector collector, HBaseClient hbaseClient);

}
