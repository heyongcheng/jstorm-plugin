package com.alibaba.jstorm.hbase.config;

import org.apache.hadoop.conf.Configuration;

/**
 * @author heyc
 * @description
 * @date 2018/9/13 16:50
 */
public class HbaseConfig extends Configuration {

    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

    public static final String HBASE_ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";

    public static final String HBASE_ZOOKEEPER_PARENT = "zookeeper.znode.parent";




}
