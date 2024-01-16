package com.k2view.cdbms.usercode.common.BigQuery;

import com.google.cloud.bigquery.*;
import com.google.gson.JsonObject;
import com.google.type.Interval;
import com.k2view.fabric.common.ByteStream;
import com.k2view.fabric.common.Log;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.opensearch.geometry.Geometry;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.k2view.fabric.common.ParamConvertor.toBuffer;

public class BigQueryParamParser {
    private static final Log log = Log.a(BigQueryParamParser.class);

    private BigQueryParamParser() {}
    @SuppressWarnings("unchecked")
    private static QueryParameterValue iteratorToBqArray(Iterator<?> iterator) {
        if (iterator == null) {
            return null;
        }

        List elementsList = new ArrayList<>();
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
                return LocalDate.class;
            case STRUCT:
                return Map.class;
            case TIMESTAMP:
                return Instant.class;
            case DATETIME:
                return LocalDateTime.class;
            case JSON:
                return JsonObject.class;
            case INTERVAL:
                // TODO Handle interval
                return Interval.class;
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
        } else if (elementType == LocalTime.class) {
            return StandardSQLTypeName.TIME;
        } else if (elementType == Instant.class) {
            return StandardSQLTypeName.TIMESTAMP;
        } else if (elementType == LocalDate.class) {
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
        // TODO Array?, Clob?
        if (param instanceof String) {
            return QueryParameterValue.string((String) param);
        }
        if (param instanceof Number) {
            Number number = (Number)param;
            if (number instanceof BigDecimal) {
                BigDecimal bigDecimal = (BigDecimal)number;
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
        if (param instanceof ByteStream) {
            return QueryParameterValue.bytes(toBuffer(param));
        }
        if (param instanceof Iterable) {
            Iterator<?> itr = ((Iterable)param).iterator();
            return BigQueryParamParser.iteratorToBqArray(itr);
        }
        if (param instanceof Boolean) {
            return  QueryParameterValue.bool((Boolean) param);
        }
        if (param instanceof byte[]) {
            return QueryParameterValue.bytes((byte[]) param);
        }
        if (param instanceof Instant) {
            Instant instant = (Instant) param;
            instant = instant.minusNanos(instant.getNano() - (instant.getNano()/1000L)*1000L);
            long epochMicros = instant.getEpochSecond()*1_000_000 + instant.getNano()/1000;
            return QueryParameterValue.timestamp(epochMicros);
        }
        if (param instanceof LocalTime){
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
            LocalTime time = ((LocalTime) param).withNano(0);
            return QueryParameterValue.time(time.format(timeFormatter));
        }
        if (param instanceof LocalDate) {
            return QueryParameterValue.date(param.toString());
        }
        if (param instanceof LocalDateTime) {
            DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime dateTime = (LocalDateTime) param;
            return QueryParameterValue.dateTime(dateTime.format(datetimeFormatter));
        }
        if (param instanceof JsonObject) {
            return QueryParameterValue.json((JsonObject) param);
        }
        if (param instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> paramMap = (Map<String, Object>) param;
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
                (fieldValue.getAttribute() == FieldValue.Attribute.PRIMITIVE && "null".equalsIgnoreCase(fieldValue.getStringValue()))) {
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
                case NUMERIC:
                    return fieldValue.getNumericValue();
                case BOOL:
                    return fieldValue.getBooleanValue();
                case BYTES:
                    return fieldValue.getBytesValue();
                case TIMESTAMP:
                    return fieldValue.getTimestampInstant();
                case DATE:
                    return LocalDate.parse((String) fieldValue.getValue());
                case TIME:
                    return LocalTime.parse((String) fieldValue.getValue());
                case DATETIME:
                    return LocalDateTime.parse((String) fieldValue.getValue());
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
                    int recordValueIndex=0;
                    for (FieldValue innerValue : recordValue) {
                        Field innerField = field.getSubFields().get(recordValueIndex++);
                        valuesMap.put(innerField.getName(), parseBqValue(innerField, innerValue, false));
                    }
                    return valuesMap;
                case ARRAY:
                    List<FieldValue> repeatedValue = fieldValue.getRepeatedValue();
                    List<Object> values = new ArrayList<>();
                    int repeatedValueIndex=0;
                    for (FieldValue innerValue : repeatedValue) {
                        Field innerField = field.getSubFields().get(repeatedValueIndex++);
                        values.add(parseBqValue(innerField, innerValue, false));
                    }
                    return values;
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
        if(org.apache.avro.Schema.Type.UNION.equals(schemaType)) {
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
        if ("date".equalsIgnoreCase(logicalTypeString)){
            return LocalDate.ofEpochDay((int)value);
        } else if ("datetime".equalsIgnoreCase(logicalTypeString)) {
            return LocalDateTime.parse(value.toString());
        } else if ("time-micros".equalsIgnoreCase(logicalTypeString)){
            long timeInMicros = (Long) value;
            return LocalTime.ofSecondOfDay(timeInMicros/1000000);
        } else if ("time-millis".equalsIgnoreCase(logicalTypeString)) {
            return LocalTime.ofSecondOfDay(((int) value) / 1000);
        } else if ("timestamp-micros".equalsIgnoreCase(logicalTypeString)) {
            long timestampInMicros = (Long) value;
            int nanoseconds = (int) ((timestampInMicros % 1_000) * 1_000);
            Instant instant = Instant.ofEpochMilli(timestampInMicros/1_000);
            instant = instant.plusNanos(nanoseconds);
            return instant;
        } else if ("timestamp-millis".equalsIgnoreCase(logicalTypeString)){
            // TODO check if reachable
            log.error("convertGenericData: Cannot parse timestamp " + value + ": Not supported");
            throw new RuntimeException();
        } else if("decimal".equalsIgnoreCase(logicalTypeString)) {
            Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
            LogicalTypes.Decimal decimalLogicalType = (LogicalTypes.Decimal)logicalType;
            return conversion.fromBytes((ByteBuffer) value, field.schema(), decimalLogicalType);
        } else if (value instanceof Utf8) {
            return value.toString();
        } else if(value instanceof GenericData.Array){
            ArrayList<Object> internalArr = new ArrayList<>();
            for(Object item : (GenericData.Array<?>) value){
                internalArr.add(parseAvroValue(item, field));
            }
            return internalArr;
        } else if(value instanceof GenericData.Record) {
            // GenericData.Record=Struct
            GenericData.Record genericDataRecord = (GenericData.Record) value;
            Map<Object, Object> res = new LinkedHashMap<>();
            org.apache.avro.Schema recordSchema = genericDataRecord.getSchema();
            for (Schema.Field innerField : recordSchema.getFields()) {
                res.put(innerField.name(), parseAvroValue(genericDataRecord.get(innerField.name()), innerField));
            }
            return res;
        } else {
            return value;
        }
    }

}
