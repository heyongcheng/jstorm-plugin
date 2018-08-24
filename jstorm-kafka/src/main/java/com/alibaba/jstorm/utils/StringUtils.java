package com.alibaba.jstorm.utils;

/**
 * @author heyc
 * @date 2018/8/24 11:20
 */
public class StringUtils {

    /**
     * isEmpty
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * isNotEmpty
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }
}
