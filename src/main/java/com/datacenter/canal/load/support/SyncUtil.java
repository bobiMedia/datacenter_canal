package com.datacenter.canal.load.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.TimeZone;

@Slf4j
public class SyncUtil {

    public final static String timeZone;    // 当前时区
    private final static DateTimeZone dateTimeZone;

    static {
        TimeZone localTimeZone = TimeZone.getDefault();
        int rawOffset = localTimeZone.getRawOffset();
        String symbol = "+";
        if (rawOffset < 0) {
            symbol = "-";
        }
        rawOffset = Math.abs(rawOffset);
        int offsetHour = rawOffset / 3600000;
        int offsetMinute = rawOffset % 3600000 / 60000;
        String hour = String.format("%1$02d", offsetHour);
        String minute = String.format("%1$02d", offsetMinute);
        timeZone = symbol + hour + ":" + minute;
        dateTimeZone = DateTimeZone.forID(timeZone);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT" + timeZone));
    }

    /**
     * 设置 preparedStatement
     *
     * @param type  sqlType
     * @param ps    需要设置的preparedStatement
     * @param value 值
     * @param i     索引号
     */
    public static void setPStmt(int type, PreparedStatement ps, Object value, int i) throws SQLException {
        switch (type) {
            case Types.BIT:
            case Types.BOOLEAN:
                if (value instanceof Boolean) {
                    ps.setBoolean(i, (Boolean) value);
                } else if (value instanceof String) {
                    boolean v = !value.equals("0");
                    ps.setBoolean(i, v);
                } else if (value instanceof Number) {
                    boolean v = ((Number) value).intValue() != 0;
                    ps.setBoolean(i, v);
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (value instanceof String) {
                    ps.setString(i, (String) value);
                } else if (value == null) {
                    ps.setNull(i, type);
                } else {
                    ps.setString(i, value.toString());
                }
                break;
            case Types.TINYINT:
                // 向上提升一级，处理unsigned情况
                if (value instanceof Number) {
                    ps.setShort(i, ((Number) value).shortValue());
                } else if (value instanceof String) {
                    ps.setShort(i, Short.parseShort((String) value));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.SMALLINT:
                if (value instanceof Number) {
                    ps.setInt(i, ((Number) value).intValue());
                } else if (value instanceof String) {
                    ps.setInt(i, Integer.parseInt((String) value));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.INTEGER:
                if (value instanceof Number) {
                    ps.setLong(i, ((Number) value).longValue());
                } else if (value instanceof String) {
                    ps.setLong(i, Long.parseLong((String) value));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.BIGINT:
                if (value instanceof Number) {
                    ps.setBigDecimal(i, new BigDecimal(value.toString()));
                } else if (value instanceof String) {
                    ps.setBigDecimal(i, new BigDecimal(value.toString()));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (value instanceof BigDecimal) {
                    ps.setBigDecimal(i, (BigDecimal) value);
                } else if (value instanceof Byte) {
                    ps.setInt(i, ((Byte) value).intValue());
                } else if (value instanceof Short) {
                    ps.setInt(i, ((Short) value).intValue());
                } else if (value instanceof Integer) {
                    ps.setInt(i, (Integer) value);
                } else if (value instanceof Long) {
                    ps.setLong(i, (Long) value);
                } else if (value instanceof Float) {
                    ps.setBigDecimal(i, BigDecimal.valueOf((float) value));
                } else if (value instanceof Double) {
                    ps.setBigDecimal(i, BigDecimal.valueOf((double) value));
                } else if (value != null) {
                    ps.setBigDecimal(i, new BigDecimal(value.toString()));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.REAL:
                if (value instanceof Number) {
                    ps.setFloat(i, ((Number) value).floatValue());
                } else if (value instanceof String) {
                    ps.setFloat(i, Float.parseFloat((String) value));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                if (value instanceof Number) {
                    ps.setDouble(i, ((Number) value).doubleValue());
                } else if (value instanceof String) {
                    ps.setDouble(i, Double.parseDouble((String) value));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                if (value instanceof Blob) {
                    ps.setBlob(i, (Blob) value);
                } else if (value instanceof byte[]) {
                    ps.setBytes(i, (byte[]) value);
                } else if (value instanceof String) {
                    ps.setBytes(i, ((String) value).getBytes(StandardCharsets.ISO_8859_1));
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.CLOB:
                if (value instanceof Clob) {
                    ps.setClob(i, (Clob) value);
                } else if (value instanceof byte[]) {
                    ps.setBytes(i, (byte[]) value);
                } else if (value instanceof String) {
                    Reader clobReader = new StringReader((String) value);
                    ps.setCharacterStream(i, clobReader);
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.DATE:
                if (value instanceof java.sql.Date) {
                    ps.setDate(i, (java.sql.Date) value);
                } else if (value instanceof java.util.Date) {
                    ps.setDate(i, new java.sql.Date(((java.util.Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        java.util.Date date = parseDate(v);
                        if (date != null) {
                            ps.setDate(i, new Date(date.getTime()));
                        } else {
                            ps.setNull(i, type);
                        }
                    } else {
                        ps.setObject(i, value);
                    }
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.TIME:
                if (value instanceof java.sql.Time) {
                    ps.setTime(i, (java.sql.Time) value);
                } else if (value instanceof java.util.Date) {
                    ps.setTime(i, new java.sql.Time(((java.util.Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    java.util.Date date = parseDate(v);
                    if (date != null) {
                        ps.setTime(i, new Time(date.getTime()));
                    } else {
                        ps.setNull(i, type);
                    }
                } else {
                    ps.setNull(i, type);
                }
                break;
            case Types.TIMESTAMP:
                if (value instanceof java.sql.Timestamp) {
                    ps.setTimestamp(i, (java.sql.Timestamp) value);
                } else if (value instanceof java.util.Date) {
                    ps.setTimestamp(i, new java.sql.Timestamp(((java.util.Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        java.util.Date date = parseDate(v);
                        if (date != null) {
                            ps.setTimestamp(i, new Timestamp(date.getTime()));
                        } else {
                            ps.setNull(i, type);
                        }
                    } else {
                        ps.setObject(i, value);
                    }
                } else {
                    ps.setNull(i, type);
                }
                break;
            default:
                ps.setObject(i, value, type);
        }
    }

    public static String getBacktickByUrl(String url) throws SQLException {
        String[] tokens = url.split(":");

        if (tokens.length < 2) {
            throw new SQLException("wrong format of jdbc");
        }

        if ("mysql".equalsIgnoreCase(tokens[1]) ||
                "mariadb".equalsIgnoreCase(tokens[1]) ||
                "oceanbase".equalsIgnoreCase(tokens[1])) {
            return "`";
        }

        if ("postgresql".equalsIgnoreCase(tokens[1]) ||
                "redshift".equalsIgnoreCase(tokens[1])) {
            return "\"";
        }

        return "";
    }

    /**
     * 通用日期时间字符解析
     *
     * @param datetimeStr 日期时间字符串
     * @return Date
     */
    public static java.util.Date parseDate(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        datetimeStr = datetimeStr.trim();
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(" ", "T");
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        }

        DateTime dateTime = new DateTime(datetimeStr, dateTimeZone);

        return dateTime.toDate();
    }
}
