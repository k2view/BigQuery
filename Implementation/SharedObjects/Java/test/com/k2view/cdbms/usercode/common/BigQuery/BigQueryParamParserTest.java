package com.k2view.cdbms.usercode.common.BigQuery;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.gson.JsonObject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigQueryParamParserTest {

    // --- getJavaTypeFromBQType ---

    @Test
    void getJavaTypeFromBQType_mapsKnownTypes() {
        assertEquals(LocalTime.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.TIME));
        assertEquals(Iterable.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.ARRAY));
        assertEquals(String.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.STRING));
        assertEquals(Float.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.FLOAT64));
        assertEquals(Integer.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.INT64));
        assertEquals(BigDecimal.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.NUMERIC));
        assertEquals(Boolean.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.BOOL));
        assertEquals(byte[].class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.BYTES));
        assertEquals(Timestamp.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.TIMESTAMP));
        assertEquals(LocalDateTime.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.DATETIME));
        assertEquals(JsonObject.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.JSON));
        assertEquals(Map.class, BigQueryParamParser.getJavaTypeFromBQType(StandardSQLTypeName.STRUCT));
    }

    // --- parseToBqParam ---

    @Test
    void parseToBqParam_string() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam("hello");
        assertEquals(StandardSQLTypeName.STRING, result.getType());
        assertEquals("hello", result.getValue());
    }

    @Test
    void parseToBqParam_longNumber() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(5L);
        assertEquals(StandardSQLTypeName.INT64, result.getType());
        assertEquals("5", result.getValue());
    }

    @Test
    void parseToBqParam_doubleNumber() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(3.5d);
        assertEquals(StandardSQLTypeName.FLOAT64, result.getType());
        assertEquals("3.5", result.getValue());
    }

    @Test
    void parseToBqParam_bigDecimalSmallScale_isNumeric() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(new BigDecimal("1.23"));
        assertEquals(StandardSQLTypeName.NUMERIC, result.getType());
    }

    @Test
    void parseToBqParam_bigDecimalLargeScale_isBigNumeric() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(new BigDecimal("1.2345678901"));
        assertEquals(StandardSQLTypeName.BIGNUMERIC, result.getType());
    }

    @Test
    void parseToBqParam_boolean() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(true);
        assertEquals(StandardSQLTypeName.BOOL, result.getType());
        assertEquals("true", result.getValue());
    }

    @Test
    void parseToBqParam_byteArray() {
        byte[] bytes = {1, 2, 3};
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(bytes);
        assertEquals(StandardSQLTypeName.BYTES, result.getType());
    }

    @Test
    void parseToBqParam_localDate() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(date);
        assertEquals(StandardSQLTypeName.DATE, result.getType());
        assertEquals("2024-01-15", result.getValue());
    }

    @Test
    void parseToBqParam_map_isStruct() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", "hello");
        map.put("b", 5L);

        QueryParameterValue result = BigQueryParamParser.parseToBqParam(map);

        assertEquals(StandardSQLTypeName.STRUCT, result.getType());
        Map<String, QueryParameterValue> struct = result.getStructValues();
        assertEquals("hello", struct.get("a").getValue());
        assertEquals("5", struct.get("b").getValue());
    }

    @Test
    void parseToBqParam_iterable_isArray() {
        QueryParameterValue result = BigQueryParamParser.parseToBqParam(List.of(1L, 2L, 3L));

        assertEquals(StandardSQLTypeName.ARRAY, result.getType());
        assertEquals(StandardSQLTypeName.INT64, result.getArrayType());
        assertEquals(3, result.getArrayValues().size());
    }

    @Test
    void parseToBqParam_unsupportedType_throws() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryParamParser.parseToBqParam(new Object()));
    }

    // --- getBqType ---

    @Test
    void getBqType_mapsJavaClassesToBqTypes() {
        assertEquals(StandardSQLTypeName.INT64, BigQueryParamParser.getBqType(Integer.class));
        assertEquals(StandardSQLTypeName.INT64, BigQueryParamParser.getBqType(long.class));
        assertEquals(StandardSQLTypeName.FLOAT64, BigQueryParamParser.getBqType(Double.class));
        assertEquals(StandardSQLTypeName.BOOL, BigQueryParamParser.getBqType(Boolean.class));
        assertEquals(StandardSQLTypeName.STRING, BigQueryParamParser.getBqType(String.class));
        assertEquals(StandardSQLTypeName.BIGNUMERIC, BigQueryParamParser.getBqType(BigDecimal.class));
        assertEquals(StandardSQLTypeName.ARRAY, BigQueryParamParser.getBqType(List.class));
        assertEquals(StandardSQLTypeName.BYTES, BigQueryParamParser.getBqType(byte[].class));
        assertEquals(StandardSQLTypeName.TIME, BigQueryParamParser.getBqType(LocalTime.class));
        assertEquals(StandardSQLTypeName.TIMESTAMP, BigQueryParamParser.getBqType(Timestamp.class));
        assertEquals(StandardSQLTypeName.DATE, BigQueryParamParser.getBqType(LocalDate.class));
        assertEquals(StandardSQLTypeName.DATETIME, BigQueryParamParser.getBqType(LocalDateTime.class));
        assertEquals(StandardSQLTypeName.JSON, BigQueryParamParser.getBqType(JsonObject.class));
        assertEquals(StandardSQLTypeName.STRUCT, BigQueryParamParser.getBqType(Map.class));
        assertEquals(StandardSQLTypeName.STRING, BigQueryParamParser.getBqType(Object.class));
    }

    // --- parseToBqByField ---

    @Test
    void parseToBqByField_nullParamOrField_returnsNull() {
        Field field = Field.of("name", StandardSQLTypeName.STRING);
        assertNull(BigQueryParamParser.parseToBqByField(null, field));
        assertNull(BigQueryParamParser.parseToBqByField("value", null));
    }

    @Test
    void parseToBqByField_nonRepeated_convertsScalar() {
        Field field = Field.of("name", StandardSQLTypeName.STRING);
        assertEquals("123", BigQueryParamParser.parseToBqByField(123, field));
    }

    @Test
    void parseToBqByField_repeated_convertsEachElement() {
        Field field = Field.newBuilder("nums", StandardSQLTypeName.INT64)
                .setMode(Field.Mode.REPEATED)
                .build();

        Object result = BigQueryParamParser.parseToBqByField(List.of(1, 2, 3), field);

        assertEquals(List.of(1L, 2L, 3L), result);
    }

    @Test
    void parseToBqByField_repeatedWithNonIterable_throws() {
        Field field = Field.newBuilder("nums", StandardSQLTypeName.INT64)
                .setMode(Field.Mode.REPEATED)
                .build();

        assertThrows(IllegalArgumentException.class, () -> BigQueryParamParser.parseToBqByField("not-iterable", field));
    }

    // --- parseBqValue ---

    @Test
    void parseBqValue_nullFieldValue_returnsNull() {
        Field field = Field.of("name", StandardSQLTypeName.INT64);
        FieldValue fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, null);

        assertNull(BigQueryParamParser.parseBqValue(field, fieldValue, false));
    }

    @Test
    void parseBqValue_int64_returnsLong() {
        Field field = Field.of("name", StandardSQLTypeName.INT64);
        FieldValue fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42");

        assertEquals(42L, BigQueryParamParser.parseBqValue(field, fieldValue, false));
    }

    @Test
    void parseBqValue_repeatedField_returnsList() {
        Field field = Field.newBuilder("nums", StandardSQLTypeName.INT64)
                .setMode(Field.Mode.REPEATED)
                .build();
        FieldValue fieldValue = FieldValue.of(FieldValue.Attribute.REPEATED, List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2")));

        Object result = BigQueryParamParser.parseBqValue(field, fieldValue, false);

        assertEquals(List.of(1L, 2L), result);
    }
}
