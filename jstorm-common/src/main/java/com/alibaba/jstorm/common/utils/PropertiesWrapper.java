package com.alibaba.jstorm.common.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * @author heyc
 * @date 2018/8/28 11:18
 */
public class PropertiesWrapper {

    private Map<Object, Object> properties;

    public PropertiesWrapper(Map<Object, Object> properties) {
        this.properties = properties;
    }

    /**
     * getObject
     * @param key
     * @return
     */
    public Object getObject(Object key) {
        return this.properties.get(key);
    }

    /**
     * put
     * @param key
     * @param value
     */
    public void put(Object key, Object value) {
        this.properties.put(key, value);
    }

    /**
     * getObject
     * @param key
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T getObject(Object key, Class<T> clazz) {
        final Object object = this.getObject(key);
        return object == null ? null : (T) object;
    }

    /**
     * getString
     * @param key
     * @return
     */
    public String getString(Object key) {
        final Object object = this.getObject(key);
        return object == null ? null : object.toString();
    }

    /**
     * getBoolean
     * @param key
     * @return
     */
    public Boolean getBoolean(Object key) {
        final Object object = this.getObject(key);
        return object == null ? null : castToBoolean(object);
    }

    /**
     * getBooleanValue
     * @param key
     * @return
     */
    public boolean getBooleanValue(Object key) {
        final Boolean value = this.getBoolean(key);
        return value == null ? false : value;
    }

    /**
     * getByte
     * @param key
     * @return
     */
    public Byte getByte(Object key) {
        Object value = this.getObject(key);
        return castToByte(value);
    }

    /**
     * getByteValue
     * @param key
     * @return
     */
    public byte getByteValue(Object key) {
        Byte value = this.getByte(key);
        return value == null ? 0 : value;
    }

    /**
     * getShort
     * @param key
     * @return
     */
    public Short getShort(Object key) {
        Object value = this.getObject(key);
        return castToShort(value);
    }

    /**
     * getShortValue
     * @param key
     * @return
     */
    public short getShortValue(Object key) {
        Short value = this.getShort(key);
        return value == null ? 0 : value;
    }

    /**
     * getInteger
     * @param key
     * @return
     */
    public Integer getInteger(Object key) {
        Object value = this.getObject(key);
        return castToInt(value);
    }

    /**
     * getIntValue
     * @param key
     * @return
     */
    public int getIntValue(Object key) {
        Integer value = this.getInteger(key);
        return value == null ? 0 : value;
    }

    /**
     * getLong
     * @param key
     * @return
     */
    public Long getLong(Object key) {
        Object value = this.getObject(key);
        return castToLong(value);
    }

    /**
     * getLongValue
     * @param key
     * @return
     */
    public long getLongValue(Object key) {
        Long value = this.getLong(key);
        return value == null ? 0L : value;
    }

    /**
     * getFloat
     * @param key
     * @return
     */
    public Float getFloat(Object key) {
        Object value = this.getObject(key);
        return castToFloat(value);
    }

    /**
     * getFloatValue
     * @param key
     * @return
     */
    public float getFloatValue(Object key) {
        Float value = this.getFloat(key);
        return value == null ? 0.0F : value;
    }

    /**
     * getDouble
     * @param key
     * @return
     */
    public Double getDouble(Object key) {
        Object value = this.getObject(key);
        return castToDouble(value);
    }

    /**
     * getDoubleValue
     * @param key
     * @return
     */
    public double getDoubleValue(Object key) {
        Double value = this.getDouble(key);
        return value == null ? 0.0D : value;
    }

    /**
     * getBigDecimal
     * @param key
     * @return
     */
    public BigDecimal getBigDecimal(Object key) {
        Object value = this.getObject(key);
        return castToBigDecimal(value);
    }

    /**
     * getBigInteger
     * @param key
     * @return
     */
    public BigInteger getBigInteger(Object key) {
        Object value = this.getObject(key);
        return castToBigInteger(value);
    }

    /**
     * getDate
     * @param key
     * @return
     */
    public Date getDate(Object key) {
        Object value = this.getObject(key);
        return castToDate(value);
    }

    /**
     * getBytes
     * @param key
     * @return
     */
    public byte[] getBytes(Object key) {
        final Object object = this.getObject(key);
        return object == null ? null : castToBytes(object);
    }

    /**
     * castToBoolean
     * @param value
     * @return
     */
    public static final Boolean castToBoolean(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean)value;
        } else if (value instanceof Number) {
            return ((Number)value).intValue() == 1;
        } else {
            if (value instanceof String) {
                String strVal = (String)value;
                if (strVal.length() == 0) {
                    return null;
                }
                if ("true".equalsIgnoreCase(strVal)) {
                    return Boolean.TRUE;
                }
                if ("false".equalsIgnoreCase(strVal)) {
                    return Boolean.FALSE;
                }
                if ("1".equals(strVal)) {
                    return Boolean.TRUE;
                }
                if ("0".equals(strVal)) {
                    return Boolean.FALSE;
                }
                if ("null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }
            }
        }
        throw new DataCastException("can not cast " + value.toString() + " to Boolean");
    }

    /**
     * castToBytes
     * @param value
     * @return
     */
    public static final byte[] castToBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[])((byte[])value);
        } else if (value instanceof String) {
            return ((String) value).getBytes();
        }
        throw new DataCastException("can not cast " + value.toString() + " to Bytes");
    }

    /**
     * castToByte
     * @param value
     * @return
     */
    public static final Byte castToByte(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number)value).byteValue();
        } else if (value instanceof String) {
            String strVal = (String)value;
            if (strVal.length() == 0) {
                return null;
            } else {
                return !"null".equals(strVal) && !"NULL".equals(strVal) ? Byte.parseByte(strVal) : null;
            }
        }
        throw new DataCastException("can not cast " + value.toString() + " to Byte");
    }

    /**
     * castToShort
     * @param value
     * @return
     */
    public static final Short castToShort(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number)value).shortValue();
        } else if (value instanceof String) {
            String strVal = (String)value;
            if (strVal.length() == 0) {
                return null;
            } else {
                return !"null".equals(strVal) && !"NULL".equals(strVal) ? Short.parseShort(strVal) : null;
            }
        }
        throw new DataCastException("can not cast " + value.toString() + " to Short");
    }

    /**
     * castToInt
     * @param value
     * @return
     */
    public static final Integer castToInt(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return (Integer)value;
        } else if (value instanceof Number) {
            return ((Number)value).intValue();
        } else if (value instanceof String) {
            String strVal = (String)value;
            if (strVal.length() == 0) {
                return null;
            } else if ("null".equals(strVal)) {
                return null;
            } else {
                return !"null".equals(strVal) && !"NULL".equals(strVal) ? Integer.parseInt(strVal) : null;
            }
        }
        throw new DataCastException("can not cast " + value.toString() + " to Integer");
    }

    /**
     * castToLong
     * @param value
     * @return
     */
    public static final Long castToLong(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number)value).longValue();
        } else {
            if (value instanceof String) {
                String strVal = (String)value;
                if (strVal.length() == 0) {
                    return null;
                }
                if ("null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }
                return Long.parseLong(strVal);
            }
            throw new DataCastException("can not cast " + value.toString() + " to Long");
        }
    }

    /**
     * castToFloat
     * @param value
     * @return
     */
    public static final Float castToFloat(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number)value).floatValue();
        } else if (value instanceof String) {
            String strVal = value.toString();
            if (strVal.length() == 0) {
                return null;
            } else {
                return !"null".equals(strVal) && !"NULL".equals(strVal) ? Float.parseFloat(strVal) : null;
            }
        }
        throw new DataCastException("can not cast " + value.toString() + " to Float");
    }

    /**
     * castToDouble
     * @param value
     * @return
     */
    public static final Double castToDouble(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number)value).doubleValue();
        } else if (value instanceof String) {
            String strVal = value.toString();
            if (strVal.length() == 0) {
                return null;
            } else {
                return !"null".equals(strVal) && !"NULL".equals(strVal) ? Double.parseDouble(strVal) : null;
            }
        }
        throw new DataCastException("can not cast " + value.toString() + " to Double");
    }

    /**
     * castToBigDecimal
     * @param value
     * @return
     */
    public static final BigDecimal castToBigDecimal(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BigDecimal) {
            return (BigDecimal)value;
        } else if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger)value);
        } else {
            String strVal = value.toString();
            return strVal.length() == 0 ? null : new BigDecimal(strVal);
        }
    }

    /**
     * castToBigInteger
     * @param value
     * @return
     */
    public static final BigInteger castToBigInteger(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BigInteger) {
            return (BigInteger)value;
        } else if (!(value instanceof Float) && !(value instanceof Double)) {
            String strVal = value.toString();
            return strVal.length() == 0 ? null : new BigInteger(strVal);
        } else {
            return BigInteger.valueOf(((Number)value).longValue());
        }
    }

    /**
     * castToDate
     * @param value
     * @return
     */
    public static final Date castToDate(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Date) {
            return (Date)value;
        } else if (value instanceof Calendar) {
            return ((Calendar)value).getTime();
        } else {
            long longValue = -1L;
            if (value instanceof Number) {
                longValue = ((Number)value).longValue();
            }
            if (value instanceof String) {
                String strVal = (String)value;
                TemporalAccessor accessor = null;
                if (strVal.length() == 8) {
                    accessor = DateTimeFormatter.ofPattern("yyyyMMdd").parse(strVal);
                } else if(strVal.length() == 14) {
                    accessor = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").parse(strVal);
                } if (strVal.length() == 10) {
                    accessor = DateTimeFormatter.ofPattern("yyyy-MM-dd").parse(strVal);
                } else if (strVal.length() == 19) {
                    accessor = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").parse(strVal);
                }
                if (accessor != null) {
                    LocalDateTime localDateTime = LocalDateTime.from(accessor);
                    Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
                    longValue = instant.toEpochMilli();
                }
            }
            if (longValue < 0L) {
                throw new DataCastException("can not cast " + value.toString() + " to Date");
            } else {
                return new Date(longValue);
            }
        }
    }

    /**
     * DataCastException
     */
    static class DataCastException extends RuntimeException {
        public DataCastException(String msg) {
            super(msg);
        }
    }
}
