package com.alibaba.jstorm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author heyc
 * @date 2018/8/29 11:35
 */
public class ByteArraySerializer {

    private Logger logger = LoggerFactory.getLogger(ByteArraySerializer.class);

    public byte[] serialize(Object value) {
        if (value == null) {
            return null;
        }
        byte[] rv = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream os = null;
        try {
            bos = new ByteArrayOutputStream();
            os = new ObjectOutputStream(bos);
            os.writeObject(value);
            os.close();
            bos.close();
            rv = bos.toByteArray();
        } catch (Exception e) {
            logger.error("can not serialize value", e);
        } finally {
            close(os);
            close(bos);
        }
        return rv;
    }


    public Object deserialize(byte[] in) {
        Object rv = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream is = null;
        try {
            if (in != null) {
                bis = new ByteArrayInputStream(in);
                is = new ObjectInputStream(bis);
                rv = is.readObject();
            }
        } catch (Exception e) {
            logger.error("can not deserialize value", e);
        } finally {
            close(is);
            close(bis);
        }
        return rv;
    }

    /**
     * close
     * @param closeable
     */
    private void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.error("closeable error", e);
            }
        }
    }

}
