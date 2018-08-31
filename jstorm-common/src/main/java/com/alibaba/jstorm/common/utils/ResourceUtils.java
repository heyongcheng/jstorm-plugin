package com.alibaba.jstorm.common.utils;

import lombok.SneakyThrows;
import shade.storm.org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author heyc
 * @date 2018/8/21 9:54
 */
public class ResourceUtils {

    /**
     * exists
     * @param fileName
     * @return
     */
    public static boolean exists(String fileName) {
        if (fileName.startsWith("classpath:")) {
            fileName = fileName.substring("classpath:".length());
            return null != ResourceUtils.class.getClassLoader().getResource(fileName);
        }
        return new File(fileName).exists();
    }

    /**
     * read
     * @param fileName
     * @return
     * @throws FileNotFoundException
     */
    @SneakyThrows
    public static Reader read(String fileName) {
        InputStream inputStream;
        if (fileName.startsWith("classpath:")) {
            fileName = fileName.substring("classpath:".length());
            URL url = ResourceUtils.class.getClassLoader().getResource(fileName);
            if (url == null) {
                throw new FileNotFoundException("file not found: " + fileName);
            }
            inputStream = ResourceUtils.class.getClassLoader().getResourceAsStream(fileName);
        } else {
            inputStream = new FileInputStream(new File(fileName));
        }
        return new InputStreamReader(inputStream);
    }

    /**
     * readAsProperties
     * @param fileName
     * @return
     */
    @SneakyThrows
    public static Properties readAsProperties(String fileName) {
        Reader reader = read(fileName);
        if (fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            return loadYamlAsProperties(reader);
        }
        Properties properties = new Properties();
        properties.load(reader);
        return properties;
    }


    /**
     * loadYaml
     * @param reader
     * @param clazz
     * @param <T>
     * @return
     */
    @SneakyThrows
    public static <T> T loadYaml(Reader reader, Class<T> clazz) {
        Yaml yaml = new Yaml();
        return yaml.loadAs(reader, clazz);
    }

    /**
     * readAllYaml
     * @param fileName
     * @return
     */
    public static Iterable<Object> readAllYaml(String fileName) {
        Reader reader = ResourceUtils.read(fileName);
        Yaml yaml = new Yaml();
        return yaml.loadAll(reader);
    }

    /**
     * readYamlAsProperties
     * @param fileName
     * @return
     */
    @SneakyThrows
    public static Properties readYamlAsProperties(String fileName) {
        return loadYamlAsProperties(read(fileName));
    }

    /**
     * loadYamlAsProperties
     * @param reader
     * @return
     */
    @SneakyThrows
    public static Properties loadYamlAsProperties(Reader reader) {
        Properties properties = new Properties();
        Map<String, ?> map = loadYaml(reader, Map.class);
        copy(map, properties, null);
        return properties;
    }

    /**
     * copy
     * @param map
     * @param properties
     * @param perfix
     */
    private static void copy(final Map<String, ?> map, final Properties properties, String perfix) {
        if (map != null && !map.isEmpty()) {
            Iterator<? extends Map.Entry<String, ?>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, ?> next = iterator.next();
                String key = perfix == null ? next.getKey() : perfix + "." + next.getKey();
                Object value = next.getValue();
                if (!(value instanceof Map)) {
                    properties.put(key, value);
                } else {
                    copy((Map<String, ?>)value, properties, key);
                }
            }
        }
    }
}
