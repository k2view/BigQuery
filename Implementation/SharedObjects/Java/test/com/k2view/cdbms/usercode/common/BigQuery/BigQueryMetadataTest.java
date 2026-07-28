package com.k2view.cdbms.usercode.common.BigQuery;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.k2view.broadway.metadata.ArrayType;
import com.k2view.broadway.metadata.ObjectType;
import com.k2view.broadway.metadata.Primitive;
import com.k2view.broadway.metadata.Schema;
import com.k2view.broadway.metadata.Type;
import com.k2view.discovery.rules.CrawlerRules;
import com.k2view.discovery.rules.DataPlatformMetaDataInfo;
import com.k2view.discovery.rules.DataPlatformMetaDataInfo.MetaDataListInfo;
import com.k2view.discovery.schema.io.CrawlerAbortedException;
import com.k2view.discovery.schema.model.impl.PrimitiveClass;
import com.k2view.discovery.schema.model.types.BooleanClass;
import com.k2view.discovery.schema.model.types.BytesClass;
import com.k2view.discovery.schema.model.types.DateClass;
import com.k2view.discovery.schema.model.types.DateTimeClass;
import com.k2view.discovery.schema.model.types.IntegerClass;
import com.k2view.discovery.schema.model.types.RealClass;
import com.k2view.discovery.schema.model.types.StringClass;
import com.k2view.discovery.schema.model.types.TimeClass;
import com.k2view.discovery.schema.model.types.UnknownClass;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoSession;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQueryMetadataTest {

    private static final String INTERFACE_NAME = "bqInterface";

    // ================= reflection helpers =================

    private static Object getStaticField(String name) throws Exception {
        java.lang.reflect.Field f = BigQueryMetadata.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(null);
    }

    private static Object getField(Object target, String name) throws Exception {
        java.lang.reflect.Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        java.lang.reflect.Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object invokeStatic(String name, Class<?>[] paramTypes, Object... args) throws Exception {
        Method m = BigQueryMetadata.class.getDeclaredMethod(name, paramTypes);
        m.setAccessible(true);
        return m.invoke(null, args);
    }

    private static Object invokeInstance(Object target, String name, Class<?>[] paramTypes, Object... args)
            throws Exception {
        Method m = target.getClass().getDeclaredMethod(name, paramTypes);
        m.setAccessible(true);
        return m.invoke(target, args);
    }

    private static Throwable causeOf(InvocationTargetException e) {
        return e.getCause() != null ? e.getCause() : e;
    }

    // ================= construction helpers =================

    private static Map<String, Object> propsWithRules(CrawlerRules rules) {
        Map<String, Object> props = new HashMap<>();
        props.put("uuid", "uuid-1");
        props.put("rules", rules);
        return props;
    }

    /** CrawlerRules mock whose getMetaData(...) returns null -> constructor's fast (no-crawl-rules) path. */
    private static CrawlerRules rulesReturningNullMetadata() {
        CrawlerRules rules = mock(CrawlerRules.class);
        when(rules.getMetaData(INTERFACE_NAME)).thenReturn(null);
        return rules;
    }

    private static BigQueryMetadata newMetadata(IoSession commandSession, IoSession readSession, BigQuery bqClient,
            boolean snapshotViaStorage, CrawlerRules rules) throws Exception {
        return new BigQueryMetadata(INTERFACE_NAME, commandSession, readSession, bqClient, "myProject",
                snapshotViaStorage, propsWithRules(rules));
    }

    private static BigQueryMetadata newMetadataWithNullRules() throws Exception {
        return newMetadata(mock(IoSession.class), mock(IoSession.class), mock(BigQuery.class), false,
                rulesReturningNullMetadata());
    }

    // ============================================================
    // createBQToSQLTypeMap / SQL_TYPE_MAPPING
    // ============================================================

    @Test
    @SuppressWarnings("unchecked")
    void sqlTypeMapping_knownEntries_mapToExpectedJavaSqlTypes() throws Exception {
        Map<StandardSQLTypeName, Integer> map = (Map<StandardSQLTypeName, Integer>) getStaticField("SQL_TYPE_MAPPING");

        assertEquals(Types.BOOLEAN, map.get(StandardSQLTypeName.BOOL));
        assertEquals(Types.BIGINT, map.get(StandardSQLTypeName.INT64));
        assertEquals(Types.TIMESTAMP, map.get(StandardSQLTypeName.TIMESTAMP));
        assertEquals(Types.TIMESTAMP, map.get(StandardSQLTypeName.DATETIME));
        assertEquals(Types.DECIMAL, map.get(StandardSQLTypeName.BIGNUMERIC));
        assertEquals(Types.NUMERIC, map.get(StandardSQLTypeName.NUMERIC));
        assertEquals(Types.DOUBLE, map.get(StandardSQLTypeName.FLOAT64));
        assertEquals(Types.BINARY, map.get(StandardSQLTypeName.BYTES));
        assertEquals(Types.DATE, map.get(StandardSQLTypeName.DATE));
        assertEquals(Types.TIME, map.get(StandardSQLTypeName.TIME));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.STRING));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.STRUCT));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.ARRAY));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.GEOGRAPHY));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.JSON));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.INTERVAL));
        assertEquals(Types.VARCHAR, map.get(StandardSQLTypeName.RANGE));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sqlTypeMapping_coversEveryStandardSqlType() throws Exception {
        Map<StandardSQLTypeName, Integer> map = (Map<StandardSQLTypeName, Integer>) getStaticField("SQL_TYPE_MAPPING");
        assertEquals(17, StandardSQLTypeName.values().length,
                "sanity check: StandardSQLTypeName enum shape changed, tests below need re-evaluating");
        assertEquals(17, map.size(), "SQL_TYPE_MAPPING should cover all 17 StandardSQLTypeName values");
    }

    // ============================================================
    // createBQToDefinedByMap / DEFINED_BY_MAPPING
    // ============================================================

    @Test
    @SuppressWarnings("unchecked")
    void definedByMapping_knownEntries_mapToExpectedPrimitiveClasses() throws Exception {
        Map<StandardSQLTypeName, PrimitiveClass> map =
                (Map<StandardSQLTypeName, PrimitiveClass>) getStaticField("DEFINED_BY_MAPPING");

        assertSame(BooleanClass.BOOLEAN, map.get(StandardSQLTypeName.BOOL));
        assertSame(IntegerClass.INTEGER, map.get(StandardSQLTypeName.INT64));
        assertSame(StringClass.STRING, map.get(StandardSQLTypeName.STRING));
        assertSame(StringClass.STRING, map.get(StandardSQLTypeName.JSON));
        assertSame(BytesClass.BYTES, map.get(StandardSQLTypeName.BYTES));
        assertSame(DateClass.DATE, map.get(StandardSQLTypeName.DATE));
        assertSame(DateTimeClass.DATETIME, map.get(StandardSQLTypeName.DATETIME));
        assertSame(DateTimeClass.DATETIME, map.get(StandardSQLTypeName.TIMESTAMP));
        assertSame(TimeClass.TIME, map.get(StandardSQLTypeName.TIME));
        assertSame(RealClass.REAL, map.get(StandardSQLTypeName.NUMERIC));
        assertSame(RealClass.REAL, map.get(StandardSQLTypeName.BIGNUMERIC));
        assertSame(RealClass.REAL, map.get(StandardSQLTypeName.FLOAT64));
        assertSame(UnknownClass.UNKNOWN, map.get(StandardSQLTypeName.GEOGRAPHY));
        assertSame(UnknownClass.UNKNOWN, map.get(StandardSQLTypeName.INTERVAL));
        assertSame(UnknownClass.UNKNOWN, map.get(StandardSQLTypeName.STRUCT));
        assertSame(UnknownClass.UNKNOWN, map.get(StandardSQLTypeName.RANGE));
    }

    @Test
    @SuppressWarnings("unchecked")
    void definedByMapping_arrayIsMissing_fallsBackToGetOrDefault() throws Exception {
        // FINDING: unlike SQL_TYPE_MAPPING, DEFINED_BY_MAPPING has no entry for ARRAY at all.
        // definedBy(...) still "works" only because callers go through getOrDefault(..., UNKNOWN),
        // silently returning UNKNOWN for ARRAY instead of a deliberate classification.
        Map<StandardSQLTypeName, PrimitiveClass> map =
                (Map<StandardSQLTypeName, PrimitiveClass>) getStaticField("DEFINED_BY_MAPPING");
        assertFalse(map.containsKey(StandardSQLTypeName.ARRAY));
        assertEquals(16, map.size(), "DEFINED_BY_MAPPING covers 16 of the 17 StandardSQLTypeName values (ARRAY absent)");
    }

    // ============================================================
    // definedBy(String)
    // ============================================================

    @Test
    void definedBy_null_returnsUnknown() throws Exception {
        Object result = invokeStatic("definedBy", new Class[] { String.class }, new Object[] { null });
        assertSame(UnknownClass.UNKNOWN, result);
    }

    @Test
    void definedBy_lowercaseKnownType_isCaseInsensitive() throws Exception {
        Object result = invokeStatic("definedBy", new Class[] { String.class }, "bool");
        assertSame(BooleanClass.BOOLEAN, result);
    }

    @Test
    void definedBy_struct_returnsUnknownViaExplicitMapping() throws Exception {
        Object result = invokeStatic("definedBy", new Class[] { String.class }, "STRUCT");
        assertSame(UnknownClass.UNKNOWN, result);
    }

    @Test
    void definedBy_arrayType_returnsUnknownBecauseMapHasNoEntry() throws Exception {
        Object result = invokeStatic("definedBy", new Class[] { String.class }, "ARRAY");
        assertSame(UnknownClass.UNKNOWN, result);
    }

    @Test
    void definedBy_repeatedPrefixedType_fallsThroughToUnknownViaUnmatchableEmptyKey() throws Exception {
        // FINDING (code smell): definedBy() special-cases "REPEATED ..." inputs by looking up the
        // key "" in DEFINED_BY_MAPPING (a Map<StandardSQLTypeName, ...>), which can never match any
        // real entry, so getOrDefault always falls back to UNKNOWN. This "actually happens" to
        // produce a defensible result (REPEATED-shaped input never has a sensible single PrimitiveClass
        // anyway) but the mechanism -- a key of a type that isn't even the map's key type, chosen
        // specifically because it can never match -- only compiles because Map.getOrDefault(Object,V)
        // accepts a raw Object key. This matches the shape convertFieldToSchema actually produces:
        // "REPEATED " + bqType.name().
        Object result = invokeStatic("definedBy", new Class[] { String.class }, "REPEATED STRING");
        assertSame(UnknownClass.UNKNOWN, result);

        Object result2 = invokeStatic("definedBy", new Class[] { String.class }, "REPEATED STRUCT");
        assertSame(UnknownClass.UNKNOWN, result2);
    }

    @Test
    void definedBy_unrecognizedType_propagatesIllegalArgumentException() {
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeStatic("definedBy", new Class[] { String.class }, "NOT_A_REAL_BQ_TYPE"));
        assertInstanceOf(IllegalArgumentException.class, causeOf(ex));
    }

    // ============================================================
    // convertFieldListToObjectType / convertFieldToSchema
    // ============================================================

    @Test
    void convertFieldToSchema_scalarField_returnsPrimitiveWithTypeNameAsDescription() throws Exception {
        Field field = Field.of("id", StandardSQLTypeName.INT64);

        Object schema = invokeStatic("convertFieldToSchema", new Class[] { Field.class }, field);

        assertInstanceOf(Primitive.class, schema);
        assertEquals(Type.integer, ((Schema) schema).type());
        assertEquals("INT64", ((Schema) schema).description());
    }

    @Test
    void convertFieldToSchema_repeatedPrimitiveField_returnsArrayTypeWrappingPrimitive() throws Exception {
        Field field = Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();

        Object schema = invokeStatic("convertFieldToSchema", new Class[] { Field.class }, field);

        assertInstanceOf(ArrayType.class, schema);
        assertEquals(Type.array, ((Schema) schema).type());
        assertEquals("REPEATED STRING", ((Schema) schema).description());

        Schema items = ((ArrayType) schema).items();
        assertInstanceOf(Primitive.class, items);
        assertEquals(Type.string, items.type());
        assertEquals("STRING", items.description());
    }

    @Test
    void convertFieldToSchema_structField_returnsObjectTypeWithRecursiveProperties() throws Exception {
        FieldList subFields = FieldList.of(
                Field.of("street", StandardSQLTypeName.STRING),
                Field.of("zip", StandardSQLTypeName.INT64));
        Field field = Field.newBuilder("address", StandardSQLTypeName.STRUCT, subFields).build();

        Object schema = invokeStatic("convertFieldToSchema", new Class[] { Field.class }, field);

        assertInstanceOf(ObjectType.class, schema);
        assertEquals("STRUCT", ((Schema) schema).description());
        ObjectType obj = (ObjectType) schema;
        assertEquals(2, obj.properties().size());
        assertEquals(Type.string, obj.properties().get("street").type());
        assertEquals(Type.integer, obj.properties().get("zip").type());
    }

    @Test
    void convertFieldToSchema_repeatedStructField_returnsArrayTypeWrappingObjectType() throws Exception {
        FieldList subFields = FieldList.of(Field.of("k", StandardSQLTypeName.STRING));
        Field field = Field.newBuilder("items", StandardSQLTypeName.STRUCT, subFields)
                .setMode(Field.Mode.REPEATED)
                .build();

        Object schema = invokeStatic("convertFieldToSchema", new Class[] { Field.class }, field);

        assertInstanceOf(ArrayType.class, schema);
        assertEquals("REPEATED STRUCT", ((Schema) schema).description());

        Schema items = ((ArrayType) schema).items();
        assertInstanceOf(ObjectType.class, items);
        ObjectType obj = (ObjectType) items;
        assertEquals(1, obj.properties().size());
        assertEquals(Type.string, obj.properties().get("k").type());
        assertEquals("STRUCT", obj.description());
    }

    @Test
    void convertFieldListToObjectType_nullParent_descriptionIsNull() throws Exception {
        FieldList fields = FieldList.of(Field.of("a", StandardSQLTypeName.BOOL));

        Object result = invokeStatic("convertFieldListToObjectType", new Class[] { FieldList.class, Field.class },
                fields, null);

        assertInstanceOf(ObjectType.class, result);
        assertNull(((Schema) result).description());
        assertEquals(1, ((ObjectType) result).properties().size());
    }

    @Test
    void convertFieldListToObjectType_withParent_descriptionIsParentStandardTypeName() throws Exception {
        FieldList fields = FieldList.of(Field.of("a", StandardSQLTypeName.BOOL));
        Field parent = Field.newBuilder("parentField", StandardSQLTypeName.STRUCT, fields).build();

        Object result = invokeStatic("convertFieldListToObjectType", new Class[] { FieldList.class, Field.class },
                fields, parent);

        assertEquals("STRUCT", ((Schema) result).description());
    }

    // ============================================================
    // mapBigQueryTypeToPrimitive(StandardSQLTypeName)
    // ============================================================

    @Test
    void mapBigQueryTypeToPrimitive_explicitlyHandledTypes_returnCorrectPrimitiveType() throws Exception {
        Map<StandardSQLTypeName, Type> expected = new HashMap<>();
        expected.put(StandardSQLTypeName.BIGNUMERIC, Type.real);
        expected.put(StandardSQLTypeName.NUMERIC, Type.real);
        expected.put(StandardSQLTypeName.FLOAT64, Type.real);
        expected.put(StandardSQLTypeName.BOOL, Type.bool);
        expected.put(StandardSQLTypeName.BYTES, Type.blob);
        expected.put(StandardSQLTypeName.DATE, Type.date);
        expected.put(StandardSQLTypeName.INT64, Type.integer);
        expected.put(StandardSQLTypeName.STRING, Type.string);
        expected.put(StandardSQLTypeName.DATETIME, Type.string);
        expected.put(StandardSQLTypeName.GEOGRAPHY, Type.string);
        expected.put(StandardSQLTypeName.INTERVAL, Type.string);
        expected.put(StandardSQLTypeName.JSON, Type.string);
        expected.put(StandardSQLTypeName.TIME, Type.string);
        expected.put(StandardSQLTypeName.TIMESTAMP, Type.string);
        expected.put(StandardSQLTypeName.RANGE, Type.string);

        for (Map.Entry<StandardSQLTypeName, Type> entry : expected.entrySet()) {
            Object result = invokeStatic("mapBigQueryTypeToPrimitive", new Class[] { StandardSQLTypeName.class },
                    entry.getKey());
            assertInstanceOf(Primitive.class, result, "expected a Primitive for " + entry.getKey());
            Primitive primitive = (Primitive) result;
            assertEquals(entry.getValue(), primitive.type(), "wrong Type for " + entry.getKey());
            assertEquals(entry.getKey().name(), primitive.description());
        }
        // 15 explicit values + STRUCT (null) + ARRAY (throws) = all 17 StandardSQLTypeName values.
        assertEquals(15, expected.size());
    }

    @Test
    void mapBigQueryTypeToPrimitive_struct_fallsThroughToNull() throws Exception {
        // FINDING: STRUCT is not covered by any case in the switch, so it silently returns null via
        // the `default -> null` arm. This happens to be harmless today because convertFieldToSchema
        // never uses the returned value when bqType == STRUCT (it recurses into
        // convertFieldListToObjectType instead) -- but any other/future caller of
        // mapBigQueryTypeToPrimitive(STRUCT) would receive null unexpectedly.
        Object result = invokeStatic("mapBigQueryTypeToPrimitive", new Class[] { StandardSQLTypeName.class },
                StandardSQLTypeName.STRUCT);
        assertNull(result);
    }

    @Test
    void mapBigQueryTypeToPrimitive_array_throwsIllegalArgumentException() {
        // FINDING (bug, high confidence -- confirmed by direct execution against the production
        // broadway-metadata jar): case ARRAY -> new Primitive(Type.array, bqType.name(), null).
        // Primitive's constructor calls Primitive.assertPrimitive(Type), which throws
        // IllegalArgumentException("\"array\" is not a primitive") because Type.array.isPrimitive()
        // is false (array/object are the only two non-primitive Type constants). So this switch arm
        // can never successfully construct a Primitive -- it always throws instead of returning a
        // value. See BigQueryMetadata.java:185.
        //
        // Reachability from the real crawl path is unclear: convertFieldToSchema calls
        // mapBigQueryTypeToPrimitive(bqType) unconditionally for every field, so if BigQuery ever
        // resolves a real table Field's getType().getStandardType() to ARRAY (as opposed to using
        // Field.Mode.REPEATED with a scalar/struct base type, which is BigQuery's normal
        // representation for arrays), the crawl would blow up on that field.
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeStatic("mapBigQueryTypeToPrimitive", new Class[] { StandardSQLTypeName.class },
                        StandardSQLTypeName.ARRAY));
        assertInstanceOf(IllegalArgumentException.class, causeOf(ex));
    }

    @Test
    void mapBigQueryTypeToPrimitive_enumShapeSanityCheck() {
        assertEquals(17, StandardSQLTypeName.values().length,
                "StandardSQLTypeName enum shape changed; re-verify ARRAY/STRUCT findings above");
    }

    // ============================================================
    // Constructor / setIncludeSchema / populateTableLists / handleSchema
    // ============================================================

    @Test
    @SuppressWarnings("unchecked")
    void constructor_nullDataPlatformMetaDataInfo_skipsSetIncludeSchema() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();

        assertNull(getField(metadata, "dataPlatformMetaDataInfo"));
        assertTrue(((Set<String>) getField(metadata, "schemasInclude")).isEmpty());
        assertTrue(((Set<String>) getField(metadata, "schemasExclude")).isEmpty());
        assertTrue(((Map<String, List<String>>) getField(metadata, "tablesInclude")).isEmpty());
        assertTrue(((Map<String, List<String>>) getField(metadata, "tablesExclude")).isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void constructor_schemaIncludeList_populatesSchemasIncludeAndTablesInclude() throws Exception {
        CrawlerRules rules = mock(CrawlerRules.class);
        DataPlatformMetaDataInfo metaInfo = mock(DataPlatformMetaDataInfo.class);
        when(rules.getMetaData(INTERFACE_NAME)).thenReturn(metaInfo);

        MetaDataListInfo schemaMeta = mock(MetaDataListInfo.class);
        schemaMeta.isIncludeOrExcludeList = true;
        when(schemaMeta.getSet()).thenReturn(new HashSet<>(Set.of("schemaA")));
        when(metaInfo.getSchemaMetadata()).thenReturn(schemaMeta);
        when(metaInfo.getTableSetPerSchema()).thenReturn(Set.of());

        MetaDataListInfo tableMeta = mock(MetaDataListInfo.class);
        tableMeta.isIncludeOrExcludeList = true;
        when(tableMeta.getSet()).thenReturn(new HashSet<>(Set.of("table1")));
        when(metaInfo.getTableListPerSchema("schemaA")).thenReturn(tableMeta);

        BigQueryMetadata metadata = newMetadata(mock(IoSession.class), mock(IoSession.class), mock(BigQuery.class),
                false, rules);

        assertEquals(Set.of("schemaA"), getField(metadata, "schemasInclude"));
        Map<String, List<String>> tablesInclude = (Map<String, List<String>>) getField(metadata, "tablesInclude");
        assertEquals(List.of("table1"), tablesInclude.get("schemaA"));
        assertTrue(((Map<String, List<String>>) getField(metadata, "tablesExclude")).isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void constructor_schemaExcludeList_populatesSchemasExcludeAndTablesExclude() throws Exception {
        CrawlerRules rules = mock(CrawlerRules.class);
        DataPlatformMetaDataInfo metaInfo = mock(DataPlatformMetaDataInfo.class);
        when(rules.getMetaData(INTERFACE_NAME)).thenReturn(metaInfo);

        MetaDataListInfo schemaMeta = mock(MetaDataListInfo.class);
        schemaMeta.isIncludeOrExcludeList = false;
        when(schemaMeta.getSet()).thenReturn(new HashSet<>()); // empty -> exclude branch
        when(metaInfo.getSchemaMetadata()).thenReturn(schemaMeta);
        when(metaInfo.getTableSetPerSchema()).thenReturn(Set.of("public"));

        MetaDataListInfo tableMeta = mock(MetaDataListInfo.class);
        tableMeta.isIncludeOrExcludeList = false;
        when(tableMeta.getSet()).thenReturn(new HashSet<>(Set.of("excludedTable")));
        when(metaInfo.getTableListPerSchema("public")).thenReturn(tableMeta);

        BigQueryMetadata metadata = newMetadata(mock(IoSession.class), mock(IoSession.class), mock(BigQuery.class),
                false, rules);

        assertTrue(((Set<String>) getField(metadata, "schemasExclude")).isEmpty());
        Map<String, List<String>> tablesExclude = (Map<String, List<String>>) getField(metadata, "tablesExclude");
        assertEquals(List.of("excludedTable"), tablesExclude.get("public"));
        assertTrue(((Map<String, List<String>>) getField(metadata, "tablesInclude")).isEmpty());
    }

    // ============================================================
    // close()
    // ============================================================

    @Test
    void close_externallyProvidedSessions_areNotClosed() throws Exception {
        IoSession commandSession = mock(IoSession.class);
        IoSession readSession = mock(IoSession.class);
        BigQueryMetadata metadata = newMetadata(commandSession, readSession, mock(BigQuery.class), true,
                rulesReturningNullMetadata());

        metadata.close();

        verify(commandSession, never()).close();
        verify(readSession, never()).close();
    }

    @Test
    void close_selfCreatedFlagsSet_closesSessions() throws Exception {
        // NOTE: the real path that flips selfCreatedCommandSession/selfCreatedReadSession to true
        // lives in the constructor's `commandSession == null` / `readSession == null` branches, which
        // call InterfacesManager.getInstance() -- a static singleton that (per Mockito 4.11, no
        // mockito-inline) cannot be mocked here. This test instead verifies close()'s own gating logic
        // in isolation by setting the flags directly via reflection, which is the part of the method
        // fully within this class's control.
        IoSession commandSession = mock(IoSession.class);
        IoSession readSession = mock(IoSession.class);
        BigQueryMetadata metadata = newMetadata(commandSession, readSession, mock(BigQuery.class), true,
                rulesReturningNullMetadata());

        setField(metadata, "selfCreatedCommandSession", true);
        setField(metadata, "selfCreatedReadSession", true);

        metadata.close();

        verify(commandSession).close();
        verify(readSession).close();
    }

    // ============================================================
    // snapshotDataset
    // ============================================================

    @Test
    void snapshotDataset_fourArgOverload_constructsBigQuerySnapshotWithPassThroughArgs() throws Exception {
        IoSession commandSession = mock(IoSession.class);
        IoSession readSession = mock(IoSession.class);
        BigQueryMetadata metadata = newMetadata(commandSession, readSession, mock(BigQuery.class), true,
                rulesReturningNullMetadata());
        SampleSize size = mock(SampleSize.class);

        BigQuerySnapshot snapshot = metadata.snapshotDataset("myDataset", "mySchema", size, new HashMap<>());

        assertSame(commandSession, getField(snapshot, "commandSession"));
        assertSame(readSession, getField(snapshot, "readSession"));
        assertEquals("myDataset", getField(snapshot, "table"));
        assertEquals("mySchema", getField(snapshot, "schema"));
        assertEquals("myProject", getField(snapshot, "datasetsProjectId"));
        assertSame(size, getField(snapshot, "size"));
        assertEquals(true, getField(snapshot, "useStorageApi"));
    }

    @Test
    void snapshotDataset_fiveArgOverload_alwaysThrowsUnsupportedOperationException() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();
        assertThrows(UnsupportedOperationException.class,
                () -> metadata.snapshotDataset("a", "b", "c", mock(SampleSize.class), new HashMap<>()));
    }

    // ============================================================
    // abort() / assertAborted()
    // ============================================================

    @Test
    void assertAborted_beforeAbort_doesNotThrow() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();
        assertDoesNotThrow(() -> invokeInstance(metadata, "assertAborted", new Class[0]));
    }

    @Test
    void assertAborted_afterAbort_throwsCrawlerAbortedException() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();
        metadata.abort();

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeInstance(metadata, "assertAborted", new Class[0]));
        assertInstanceOf(CrawlerAbortedException.class, causeOf(ex));
    }

    // ============================================================
    // buildFieldDescriptionsByTable(IoCommand.Result)
    // ============================================================

    @Test
    void buildFieldDescriptionsByTable_rowsWithNullDescription_areSkipped() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();

        IoCommand.Row row = mock(IoCommand.Row.class);
        when(row.get("description")).thenReturn(null);

        IoCommand.Result result = mock(IoCommand.Result.class);
        when(result.iterator()).thenReturn(List.of(row).iterator());

        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> byTable = (Map<String, Map<String, String>>) invokeInstance(metadata,
                "buildFieldDescriptionsByTable", new Class[] { IoCommand.Result.class }, result);

        assertTrue(byTable.isEmpty());
    }

    @Test
    void buildFieldDescriptionsByTable_groupsDescriptionsByTableAndFieldPath() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();

        IoCommand.Row row1 = mock(IoCommand.Row.class);
        when(row1.get("description")).thenReturn("First column");
        when(row1.get("table_name")).thenReturn("t1");
        when(row1.get("field_path")).thenReturn("col1");

        IoCommand.Row rowSkipped = mock(IoCommand.Row.class);
        when(rowSkipped.get("description")).thenReturn(null);

        IoCommand.Row row2 = mock(IoCommand.Row.class);
        when(row2.get("description")).thenReturn("Second column");
        when(row2.get("table_name")).thenReturn("t1");
        when(row2.get("field_path")).thenReturn("col2");

        IoCommand.Row row3 = mock(IoCommand.Row.class);
        when(row3.get("description")).thenReturn("Other table column");
        when(row3.get("table_name")).thenReturn("t2");
        when(row3.get("field_path")).thenReturn("colX");

        IoCommand.Result result = mock(IoCommand.Result.class);
        when(result.iterator()).thenReturn(List.of(row1, rowSkipped, row2, row3).iterator());

        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> byTable = (Map<String, Map<String, String>>) invokeInstance(metadata,
                "buildFieldDescriptionsByTable", new Class[] { IoCommand.Result.class }, result);

        assertEquals(2, byTable.size());
        assertEquals("First column", byTable.get("t1").get("col1"));
        assertEquals("Second column", byTable.get("t1").get("col2"));
        assertEquals("Other table column", byTable.get("t2").get("colX"));
    }

    // ============================================================
    // appendTableFilter(String, String, List<Object>)
    // ============================================================

    @Test
    @SuppressWarnings("unchecked")
    void appendTableFilter_tablesIncludePresent_appendsInClauseAndParam() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();
        Map<String, List<String>> tablesInclude = (Map<String, List<String>>) getField(metadata, "tablesInclude");
        tablesInclude.put("schemaA", List.of("t1", "t2"));

        List<Object> params = new ArrayList<>();
        String result = (String) invokeInstance(metadata, "appendTableFilter",
                new Class[] { String.class, String.class, List.class }, "SELECT 1", "schemaA", params);

        assertEquals("SELECT 1 AND table_name IN UNNEST (?)", result);
        assertEquals(1, params.size());
        assertEquals(List.of("t1", "t2"), params.get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void appendTableFilter_tablesExcludePresent_appendsNotInClauseAndParam() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();
        Map<String, List<String>> tablesExclude = (Map<String, List<String>>) getField(metadata, "tablesExclude");
        tablesExclude.put("schemaA", List.of("t3"));

        List<Object> params = new ArrayList<>();
        String result = (String) invokeInstance(metadata, "appendTableFilter",
                new Class[] { String.class, String.class, List.class }, "SELECT 1", "schemaA", params);

        assertEquals("SELECT 1 AND table_name NOT IN UNNEST (?)", result);
        assertEquals(1, params.size());
        assertEquals(List.of("t3"), params.get(0));
    }

    @Test
    void appendTableFilter_neitherIncludeNorExclude_returnsQueryAndParamsUnchanged() throws Exception {
        BigQueryMetadata metadata = newMetadataWithNullRules();

        List<Object> params = new ArrayList<>();
        String result = (String) invokeInstance(metadata, "appendTableFilter",
                new Class[] { String.class, String.class, List.class }, "SELECT 1", "schemaWithNoFilters", params);

        assertEquals("SELECT 1", result);
        assertTrue(params.isEmpty());
    }
}
