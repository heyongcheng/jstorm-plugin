package com.alibaba.jstorm.redis.common;

import com.alibaba.jstorm.common.utils.PropertiesWrapper;
import com.alibaba.jstorm.common.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;

/**
 * @author heyc
 * @date 2018/8/27 20:45
 */
@Setter
@Getter
public class RedisConfig implements Serializable{

    private static final long serialVersionUID = -8491541663904804268L;

    private static final String KEY_PERFIX = "redis.";

    private Integer minIdle;

    private Integer maxIdle;

    private Long maxWait;

    private String masterName;

    private String sentinels;

    private String password;

    private int database;

    private int timeout = 2000;

    /**
     * getSentinelSet
     * @return
     */
    public Set<String> getSentinelSet() {
        if (this.sentinels == null) {
            return Collections.emptySet();
        }
        List<String> list = Arrays.asList(this.sentinels.split(",|;"));
        HashSet<String> set = new HashSet<>(list);
        return set;
    }

    public RedisConfig(Properties properties) {
        this.initConfig(properties);
    }

    /**
     * initConfig
     * @param properties
     */
    public void initConfig(Properties properties) {
        PropertiesWrapper wrapper = new PropertiesWrapper(properties);
        this.database = wrapper.getIntValue("redis.database");
        String passwordVal = wrapper.getString("redis.password");
        if (!StringUtils.isEmpty(passwordVal)) {
            this.password = passwordVal;
        }
        String masterVal = wrapper.getString("redis.sentinel.master");
        if (!StringUtils.isEmpty(masterVal)) {
            this.masterName = masterVal;
        }
        String nodesVal = wrapper.getString("redis.sentinel.nodes");
        if (!StringUtils.isEmpty(nodesVal)) {
            this.sentinels = nodesVal;
        }
        Integer minIdleVal = wrapper.getInteger("redis.pool.minIdle");
        if (minIdleVal != null) {
            this.minIdle = minIdleVal;
        }
        Integer maxIdleVal = wrapper.getInteger("redis.pool.maxIdle");
        if (maxIdleVal != null) {
            this.maxIdle = maxIdleVal;
        }
        Long maxWaitVal = wrapper.getLong("redis.pool.maxWait");
        if (maxWaitVal != null) {
            this.maxWait = maxWaitVal;
        }
    }

    @Override
    public String toString() {
        return "RedisConfig{" +
                "minIdle=" + minIdle +
                ", maxIdle=" + maxIdle +
                ", maxWait=" + maxWait +
                ", masterName='" + masterName + '\'' +
                ", sentinels='" + sentinels + '\'' +
                ", password='" + password + '\'' +
                ", database=" + database +
                ", timeout=" + timeout +
                '}';
    }
}
