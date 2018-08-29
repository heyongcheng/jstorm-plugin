package com.alibaba.jstorm.redis.common;

import java.nio.charset.Charset;

/**
 * @author heyc
 * @date 2018/8/29 11:33
 */
public class StringRedisSerializer implements RedisSerialize<String> {

    private final Charset charset;

    public StringRedisSerializer() {
        this(Charset.forName("UTF8"));
    }

    public StringRedisSerializer(Charset charset) {
        this.charset = charset;
    }

    @Override
    public String deserialize(byte[] bytes) {
        return (bytes == null ? null : new String(bytes, charset));
    }

    @Override
    public byte[] serialize(String string) {
        return (string == null ? null : string.getBytes(charset));
    }

}
