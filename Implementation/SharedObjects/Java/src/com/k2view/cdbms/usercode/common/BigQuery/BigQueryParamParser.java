package com.k2view.cdbms.usercode.common.BigQuery;

import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.k2view.fabric.common.ParamConvertor.toBuffer;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.opensearch.geometry.Geometry;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.gson.JsonObject;
import com.k2view.fabric.common.ByteStream;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;

public class BigQueryParamParser {
    private static final String BQ_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String BQ_TIME_FORMAT = "HH:mm:ss.SSSSSS";
    private static final Log log = Log.a(BigQueryParamParser.class);

    private BigQueryParamParser() {
    }

    private static QueryParameterValue iteratorToBqArray(Iterator<?> iterator) {
        if (iterator == null) {
            return null;
        }

        List<Object> elementsList = new ArrayList<>();
        iterator.forEachRemaining(elementsList::add);

        if (elementsList.isEmpty()) {
            return QueryParameterValue.array(new Object[0], StandardSQLTypeName.STRING);
        }

        // Determine the type of elements in the list
        Class<?> elementType = elementsList.get(0).getClass();

        // Map Java types to BigQuery types
        StandardSQLTypeName bqType = getBqType(elementType);

        return QueryParameterValue.array(elementsList.toArray(), bqType);
    }

    public static Type getJavaTypeFromBQType(StandardSQLTypeName type) {
        switch (type) {
            case TIME:
                return LocalTime.class;
            case ARRAY:
                return Iterable.class;
            case STRING:
            case GEOGRAPHY:
                return String.class;
            case FLOAT64:
                return Float.class;
            case INT64:
                return Integer.class;
            case BIGNUMERIC:
            case NUMERIC:
                return BigDecimal.class;
            case BOOL:
                return Boolean.class;
            case BYTES:
                return byte[].class;
            case DATE:
                return java.sql.Date.class;
            case STRUCT:
                return Map.class;
            case TIMESTAMP:
                return Timestamp.class;
            case DATETIME:
                return LocalDateTime.class;
            case JSON:
                return JsonObject.class;
            case INTERVAL:
                return String.class;
            case RANGE:
                return Map.class;
            // return String.class;
        }
        throw new IllegalArgumentException("Unsupported StandardSQLTypeName type " + type);
    }

    static StandardSQLTypeName getBqType(Class<?> elementType) {
        if (elementType == Integer.class || elementType == int.class) {
            return StandardSQLTypeName.INT64;
        } else if (elementType == Long.class || elementType == long.class) {
            return StandardSQLTypeName.INT64;
        } else if (elementType == Double.class || elementType == double.class) {
            return StandardSQLTypeName.FLOAT64;
        } else if (elementType == Float.class || elementType == float.class) {
            return StandardSQLTypeName.FLOAT64;
        } else if (elementType == Boolean.class || elementType == boolean.class) {
            return StandardSQLTypeName.BOOL;
        } else if (elementType == String.class) {
            return StandardSQLTypeName.STRING;
        } else if (elementType == BigDecimal.class) {
            return StandardSQLTypeName.BIGNUMERIC;
        } else if (Iterable.class.isAssignableFrom(elementType)) {
            return StandardSQLTypeName.ARRAY;
        } else if (byte[].class.isAssignableFrom(elementType)) {
            return StandardSQLTypeName.BYTES;
        } else if (elementType == Time.class || elementType == LocalTime.class) {
            return StandardSQLTypeName.TIME;
        } else if (elementType == Timestamp.class) {
            return StandardSQLTypeName.TIMESTAMP;
        } else if (elementType == Date.class || elementType == LocalDate.class) {
            return StandardSQLTypeName.DATE;
        } else if (elementType == LocalDateTime.class) {
            return StandardSQLTypeName.DATETIME;
        } else if (elementType == JsonObject.class) {
            return StandardSQLTypeName.JSON;
        } else if (Map.class.isAssignableFrom(elementType)) {
            return StandardSQLTypeName.STRUCT;
        } else {
            // Handle other types as needed
            return StandardSQLTypeName.STRING;
        }
    }

    public static QueryParameterValue parseToBqParam(Object param) {
        // TO-DO Array?, Clob?
        if (param instanceof String str) {
            return QueryParameterValue.string(str);
        }
        if (param instanceof Number number) {
            if (number instanceof BigDecimal bigDecimal) {
                if (bigDecimal.scale() > 9) {
                    return QueryParameterValue.bigNumeric(bigDecimal);
                }
                return QueryParameterValue.numeric(bigDecimal);
            }
            if (number instanceof Long) {
                return QueryParameterValue.int64(number.longValue());
            }
            if (number instanceof Float) {
                return QueryParameterValue.float64(number.floatValue());
            }
            if (number instanceof Double) {
                return QueryParameterValue.float64(number.doubleValue());
            }
            return QueryParameterValue.int64(number.intValue());
        }
        if (param instanceof ByteStream byteStream) {
            return QueryParameterValue.bytes(toBuffer(byteStream));
        }
        if (param instanceof Iterable<?> itr) {
            return BigQueryParamParser.iteratorToBqArray(itr.iterator());
        }
        if (param instanceof Boolean b) {
            return QueryParameterValue.bool(b);
        }
        if (param instanceof byte[] bytes) {
            return QueryParameterValue.bytes(bytes);
        }
        if (param instanceof Timestamp timestamp) {
            long epochMicros = timestamp.getTime() * 1_000 + (timestamp.getNanos() % 1_000_000) / 1_000;
            return QueryParameterValue.timestamp(epochMicros);
        }
        if (param instanceof Time t) {
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(BQ_TIME_FORMAT);
            return QueryParameterValue.time(t.toLocalTime().format(timeFormatter));
        }
        if (param instanceof LocalTime t) {
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(BQ_TIME_FORMAT);
            return QueryParameterValue.time(t.format(timeFormatter));
        }
        if (param instanceof LocalDate || param instanceof java.sql.Date) {
            return QueryParameterValue.date(param.toString());
        }
        if (param instanceof LocalDateTime dateTime) {
            DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern(BQ_DATETIME_FORMAT);
            return QueryParameterValue.dateTime(dateTime.format(datetimeFormatter));
        }
        if (param instanceof JsonObject) {
            return QueryParameterValue.json((JsonObject) param);
        }
        if (param instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> paramMap = (Map<String, Object>) param;
            Map<String, QueryParameterValue> struct = new HashMap<>();
            for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
                struct.put(entry.getKey(), parseToBqParam(entry.getValue()));
            }
            return QueryParameterValue.struct(struct);
        }
        if (param instanceof Blob) {
            return QueryParameterValue.string(new String(toBuffer(param)));
        }
        if (param instanceof ByteBuffer) {
            return QueryParameterValue.bytes(toBuffer(param));
        }
        if (param instanceof Geometry) {
            return QueryParameterValue.geography(param.toString());
        }
        throw new IllegalArgumentException("Unexpected param type for " + param);
    }

    public static Object parseBqValue(Field field, FieldValue fieldValue, boolean isInRepeated) {
        LegacySQLTypeName type = field.getType();

        if (fieldValue.isNull() ||
                (fieldValue.getAttribute() == FieldValue.Attribute.PRIMITIVE
                        && "null".equalsIgnoreCase(fieldValue.getStringValue()))) {
            return null;
        }
        if (field.getMode() == REPEATED && !isInRepeated) {
            List<FieldValue> valuesList = fieldValue.getRepeatedValue();
            List<Object> valuesResult = new ArrayList<>();
            for (FieldValue innerValue : valuesList) {
                valuesResult.add(parseBqValue(field, innerValue, true));
            }
            return valuesResult;
        } else {
            switch (type.getStandardType()) {
                case INT64:
                    return fieldValue.getLongValue();
                case FLOAT64:
                    return fieldValue.getDoubleValue();
                case BIGNUMERIC:
                    return fieldValue.getValue();
                case NUMERIC:
                    return fieldValue.getNumericValue();
                case BOOL:
                    return fieldValue.getBooleanValue();
                case BYTES:
                    return fieldValue.getBytesValue();
                case TIMESTAMP:
                    return Timestamp.from(fieldValue.getTimestampInstant());
                case DATE:
                    return fieldValue.getValue();
                case TIME:
                    return fieldValue.getValue();
                case DATETIME:
                    return fieldValue.getValue();
                case STRING:
                case JSON:
                case INTERVAL:
                    return fieldValue.getValue();
                case GEOGRAPHY:
                    // Keeping below code as reference to show how Geography data can be parsed
                    // in case it needs to be masked/manipulated.
                    // GeographyValidator geographyValidator = new GeographyValidator(false);
                    // WellKnownText wkt = new WellKnownText(false, geographyValidator);
                    // Geometry geometry = wkt.fromWKT((String) fieldValue.getValue());
                    return fieldValue.getStringValue();
                case STRUCT:
                    FieldValueList recordValue = fieldValue.getRecordValue();
                    Map<String, Object> valuesMap = new LinkedHashMap<>();
                    int recordValueIndex = 0;
                    for (FieldValue innerValue : recordValue) {
                        Field innerField = field.getSubFields().get(recordValueIndex++);
                        valuesMap.put(innerField.getName(), parseBqValue(innerField, innerValue, false));
                    }
                    return valuesMap;
                case ARRAY:
                    List<FieldValue> repeatedValue = fieldValue.getRepeatedValue();
                    List<Object> values = new ArrayList<>();
                    int repeatedValueIndex = 0;
                    for (FieldValue innerValue : repeatedValue) {
                        Field innerField = field.getSubFields().get(repeatedValueIndex++);
                        values.add(parseBqValue(innerField, innerValue, false));
                    }
                    return values;
                case RANGE:
                    return fieldValue.getRangeValue().getValues();
                // return Json.get().toJson(fieldValue.getRangeValue().getValues());
                default:
                    return fieldValue.getValue();
            }
        }
    }

    static Object parseAvroValue(Object value, org.apache.avro.Schema.Field field) {
        String logicalTypeString;
        LogicalType logicalType;

        org.apache.avro.Schema.Type schemaType = field.schema().getType();
        List<org.apache.avro.Schema> typesList = null;
        if (value == null) {
            return null;
        }
        if (org.apache.avro.Schema.Type.UNION.equals(schemaType)) {
            typesList = field.schema().getTypes();
            logicalType = typesList.get(typesList.size() - 1).getLogicalType();
        } else {
            logicalType = field.schema().getLogicalType();
        }
        if (logicalType == null) {
            logicalTypeString = typesList == null ? null : typesList.get(typesList.size() - 1).getProp("logicalType");
        } else {
            logicalTypeString = String.valueOf(logicalType.getName());
        }
        if ("date".equalsIgnoreCase(logicalTypeString)) {
            long epochDays = (int) value;
            return new java.sql.Date(epochDays * 24L * 60 * 60 * 1000); // Convert days to milliseconds
        } else if ("datetime".equalsIgnoreCase(logicalTypeString)) {
            return LocalDateTime.parse(value.toString());
        } else if ("time-micros".equalsIgnoreCase(logicalTypeString)) {
            long timeInMicros = (Long) value;
            return LocalTime.ofSecondOfDay(timeInMicros / 1000000);
        } else if ("time-millis".equalsIgnoreCase(logicalTypeString)) {
            return LocalTime.ofSecondOfDay(((int) value) / 1000);
        } else if ("timestamp-micros".equalsIgnoreCase(logicalTypeString)) {
            long timestampInMicros = (Long) value;
            long milliseconds = timestampInMicros / 1_000;
            int nanoseconds = (int) (timestampInMicros % 1_000) * 1_000;

            Instant instant = Instant.ofEpochMilli(milliseconds).plusNanos(nanoseconds);
            return Timestamp.from(instant);
        } else if ("timestamp-millis".equalsIgnoreCase(logicalTypeString)) {
            // TO-DO check if reachable
            log.error("convertGenericData: Cannot parse timestamp " + value + ": Not supported");
            throw new RuntimeException();
        } else if ("decimal".equalsIgnoreCase(logicalTypeString)) {
            Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
            LogicalTypes.Decimal decimalLogicalType = (LogicalTypes.Decimal) logicalType;
            return conversion.fromBytes((ByteBuffer) value, field.schema(), decimalLogicalType);
        } else if (value instanceof Utf8) {
            return value.toString();
        } else if (value instanceof GenericData.Array) {
            ArrayList<Object> internalArr = new ArrayList<>();
            for (Object item : (GenericData.Array<?>) value) {
                internalArr.add(parseAvroValue(item, field));
            }
            return internalArr;
        } else if (value instanceof GenericData.Record) {
            // GenericData.Record=Struct
            GenericData.Record genericDataRecord = (GenericData.Record) value;
            org.apache.avro.Schema recordSchema = genericDataRecord.getSchema();
            if ("interval".equalsIgnoreCase(recordSchema.getName())) {
                int months = genericDataRecord.hasField("months") ? (Integer) genericDataRecord.get("months") : 0;
                int days = genericDataRecord.hasField("days") ? (Integer) genericDataRecord.get("days") : 0;
                int hours = genericDataRecord.hasField("hours") ? (Integer) genericDataRecord.get("hours") : 0;
                int minutes = genericDataRecord.hasField("minutes") ? (Integer) genericDataRecord.get("minutes")
                        : 0;
                double seconds = genericDataRecord.hasField("seconds") ? (Double) genericDataRecord.get("seconds")
                        : 0.0;

                int years = months / 12;
                int remainingMonths = months % 12;

                String yearsMonthsSign = (months >= 0) ? "+" : "-";
                String daysSign = (days >= 0) ? "+" : "-";
                String timeSign = (hours >= 0 || minutes >= 0 || seconds >= 0) ? "+" : "-";

                return String.format("%s%d-%d %s%d %s%d:%d:%.3f",
                        yearsMonthsSign, Math.abs(years), Math.abs(remainingMonths),
                        daysSign, Math.abs(days),
                        timeSign, Math.abs(hours), Math.abs(minutes), Math.abs(seconds));
            }
            Map<Object, Object> res = new LinkedHashMap<>();
            for (Schema.Field innerField : recordSchema.getFields()) {
                res.put(innerField.name(), parseAvroValue(genericDataRecord.get(innerField.name()), innerField));
            }
            return res;
        } else if (value instanceof ByteBuffer bb) {
            return bb.array();
        } else {
            return value;
        }
    }

    public static Object parseToBqByField(Object param, Field field) {
        if (param == null || field == null) {
            return null;
        }

        boolean isRepeated = field.getMode() == Field.Mode.REPEATED;
        if (isRepeated) {

            if (!(param instanceof Iterable)) {
                throw new IllegalArgumentException("Expected an iterable for repeated field: " + field.getName());
            }
            Iterable<?> iterableParam = (Iterable<?>) param;
            List<Object> parsedList = new ArrayList<>();
            Iterator<?> iterator = iterableParam.iterator();
            while (iterator.hasNext()) {
                parsedList.add(parseUnrepeatedValue(iterator.next(), field));
            }
            return parsedList;
        } else {
            return parseUnrepeatedValue(param, field);
        }
    }

    private static Object parseUnrepeatedValue(Object param, Field field) {
        switch (field.getType().getStandardType()) {
            case STRING:
                return param.toString();

            case INT64:
                return toLong(param);

            case FLOAT64:
                return toDouble(param);

            case BOOL:
                return toBoolean(param);

            case BYTES:
                return toByteArray(param);

            case TIMESTAMP:
                return toTimestampString(param);

            case DATE:
                return toDateString(param);

            case TIME:
                return toTimeString(param);

            case DATETIME:
                return toDateTimeString(param);

            case NUMERIC:
            case BIGNUMERIC:
                return toBigDecimalString(param);

            case STRUCT:
                return toStruct(param, field);

            case ARRAY:
                return toArray(param, field);

            case INTERVAL:
                return param;

            case GEOGRAPHY:
                // BigQuery expects GEOGRAPHY as a WKT (Well-Known Text) or GeoJSON string
                return param.toString(); // Ensure the value is in WKT or GeoJSON format

            case JSON:
                // Ensure JSON is stored as a valid string
                return param instanceof String ? param : Json.get().toJson(param);

            case RANGE:
                return param;
            // return Json.get().fromJson((String) param);
            default:
                throw new IllegalArgumentException("Unsupported BigQuery field: " + field);
        }
    }

    private static Long toLong(Object param) {
        if (param instanceof Number num) {
            return num.longValue();
        }
        throw new IllegalArgumentException("Expected INT64 but received: " + param.getClass().getName());
    }

    private static Double toDouble(Object param) {
        if (param instanceof Number num) {
            return num.doubleValue();
        }
        throw new IllegalArgumentException("Expected FLOAT64 but received: " + param.getClass().getName());
    }

    private static Boolean toBoolean(Object param) {
        return ParamConvertor.toBool(param);
    }

    private static byte[] toByteArray(Object param) {
        return ParamConvertor.toBuffer(param);
    }

    private static String toTimestampString(Object param) {
        if (param instanceof Timestamp ts) {
            return ts.toInstant().toString();
        } else if (param instanceof Instant instant) {
            return instant.toString();
        } else if (param instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException("Expected TIMESTAMP but received: " + param.getClass().getName());
    }

    private static String toDateString(Object param) {
        if (param instanceof LocalDate localDate) {
            return localDate.toString();
        } else if (param instanceof java.sql.Date sqlDate) {
            return sqlDate.toString();
        } else if (param instanceof Date utilDate) {
            return Instant.ofEpochMilli(utilDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate().toString();
        } else if (param instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException("Expected DATE but received: " + param.getClass().getName());
    }

    private static String toTimeString(Object param) {
        if (param instanceof Time time) {
            return time.toLocalTime().toString();
        } else if (param instanceof LocalTime localTime) {
            return localTime.toString();
        } else if (param instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException("Expected TIME but received: " + param.getClass().getName());
    }

    private static String toDateTimeString(Object param) {
        if (param instanceof LocalDateTime localDateTime) {
            return localDateTime.toString().replace("T", " ");
        } else if (param instanceof Timestamp t) {
            return t.toLocalDateTime().toString().replace("T", " ");
        }else if (param instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException("Expected DATETIME but received: " + param.getClass().getName());
    }

    private static Object toBigDecimalString(Object param) {
        if (param instanceof BigDecimal bigDecimal) {
            return bigDecimal.scale() > 9 ? bigDecimal.toPlainString() : bigDecimal;
        } else if (param instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException("Expected NUMERIC/BIGNUMERIC but received: " + param.getClass().getName());
    }

    private static List<Object> toArray(Object param, Field field) {
        if (!(param instanceof Iterable<?> iterable)) {
            throw new IllegalArgumentException("Expected an Iterable but received: " + param.getClass().getName());

        }
        Field elementField = field.getSubFields().get(0); // Assuming only one subfield for ARRAY
        List<Object> convertedList = new ArrayList<>();
        for (Object item : iterable) {
            convertedList.add(parseToBqByField(item, elementField)); // Recursively convert list elements
        }
        return convertedList;
    }

    private static Map<String, Object> toStruct(Object param, Field field) {
        if (!(param instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Expected a Map but received: " + param.getClass().getName());
        }
        Map<String, Object> convertedMap = new HashMap<>();
        for (Field subField : field.getSubFields()) {
            Object value = map.get(subField.getName());
            convertedMap.put(subField.getName(), parseToBqByField(value, subField));
        }
        return convertedMap;
    }

}
