package com.alibaba.jstorm.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import com.alibaba.jstorm.common.utils.StringUtils;
import com.alibaba.jstorm.redis.common.RedisClient;
import com.alibaba.jstorm.redis.common.RedisCommand;
import com.alibaba.jstorm.redis.common.RedisConfig;

import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/27 20:42
 */
public abstract class AbstractRedisBolt extends BaseRichBolt {

    private static final long serialVersionUID = -7294334656454951695L;

    private boolean forceReInit;

    private String configPath;

    private RedisConfig redisConfig;

    public AbstractRedisBolt(String configPath) {
        this.configPath = configPath;
    }

    public AbstractRedisBolt(String configPath, boolean forceReInit) {
        this.configPath = configPath;
        this.forceReInit = forceReInit;
    }

    public AbstractRedisBolt(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public AbstractRedisBolt(RedisConfig redisConfig, boolean forceReInit) {
        this.redisConfig = redisConfig;
        this.forceReInit = forceReInit;
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        if (this.redisConfig != null) {
            RedisClient.init(redisConfig, forceReInit);
        } else if (!StringUtils.isEmpty(this.configPath)){
            RedisClient.init(configPath, forceReInit);
        }
    }

    /**
     * doInRedis
     * @param command
     * @param <T>
     * @return
     */
    public static <T> T doInRedis(RedisCommand<T> command) {
        return RedisClient.execute(command);
    }

}
