package com.alibaba.jstorm.hbase;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.hbase.bolt.AbstractHbaseBolt;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author heyc
 * @description
 * @date 2018/9/14 15:32
 */
public class TestHbaseBolt extends AbstractHbaseBolt {

    private static final Logger logger = LoggerFactory.getLogger(TestHbaseBolt.class);

    private static final String TABLE_NAME = "collect_payment_info";

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        try {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                desc.addFamily(new HColumnDescriptor("trade"));
                desc.addFamily(new HColumnDescriptor("payment"));
                admin.createTable(desc);
                logger.info("create table {} success", TABLE_NAME);
            } else {
                logger.info("hbase table [{}] exists", TABLE_NAME);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input, OutputCollector collector, Connection connection) {
        try {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            String rowKey = "";
            Put put = new Put(Bytes.toBytes(rowKey));

        } catch (Exception e) {
            throw new FailedException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
