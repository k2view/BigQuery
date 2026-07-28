package com.k2view.cdbms.usercode.common.BigQuery;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.k2view.cdbms.interfaces.GenericInterface;
import com.k2view.cdbms.lut.InterfacesManager;
import com.k2view.cdbms.lut.LUType;
import com.k2view.cdbms.lut.LudbColumn;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedLogicTest {

    // =====================================================================
    // bigQueryIoProvider
    // =====================================================================

    @Test
    void bigQueryIoProvider_returnsNewBigQueryIoProviderInstance() throws Exception {
        assertInstanceOf(BigQueryIoProvider.class, SharedLogic.bigQueryIoProvider());
    }

    // =====================================================================
    // mapToStandardSQLType (private helper, exercised via reflection since
    // it is the novel logic behind bqParseTdmQueryParam)
    // =====================================================================

    private static StandardSQLTypeName mapToStandardSQLType(String type) throws Exception {
        Method m = SharedLogic.class.getDeclaredMethod("mapToStandardSQLType", String.class);
        m.setAccessible(true);
        try {
            return (StandardSQLTypeName) m.invoke(null, type);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw e;
        }
    }

    @Test
    void mapToStandardSQLType_stripsTrailingPrecisionSpec_string() throws Exception {
        assertEquals(StandardSQLTypeName.STRING, mapToStandardSQLType("STRING(10)"));
    }

    @Test
    void mapToStandardSQLType_stripsTrailingPrecisionSpec_numeric() throws Exception {
        assertEquals(StandardSQLTypeName.NUMERIC, mapToStandardSQLType("NUMERIC(10,2)"));
    }

    @Test
    void mapToStandardSQLType_arrayPrefix_returnsArrayType() throws Exception {
        assertEquals(StandardSQLTypeName.ARRAY, mapToStandardSQLType("ARRAY<STRING>"));
    }

    @Test
    void mapToStandardSQLType_int64Aliases() throws Exception {
        assertEquals(StandardSQLTypeName.INT64, mapToStandardSQLType("INT64"));
        assertEquals(StandardSQLTypeName.INT64, mapToStandardSQLType("INTEGER"));
    }

    @Test
    void mapToStandardSQLType_float64Aliases() throws Exception {
        assertEquals(StandardSQLTypeName.FLOAT64, mapToStandardSQLType("FLOAT64"));
        assertEquals(StandardSQLTypeName.FLOAT64, mapToStandardSQLType("FLOAT"));
    }

    @Test
    void mapToStandardSQLType_boolAliases() throws Exception {
        assertEquals(StandardSQLTypeName.BOOL, mapToStandardSQLType("BOOLEAN"));
        assertEquals(StandardSQLTypeName.BOOL, mapToStandardSQLType("BOOL"));
    }

    @Test
    void mapToStandardSQLType_dateTimeFamily() throws Exception {
        assertEquals(StandardSQLTypeName.DATETIME, mapToStandardSQLType("DATETIME"));
        assertEquals(StandardSQLTypeName.TIMESTAMP, mapToStandardSQLType("TIMESTAMP"));
        assertEquals(StandardSQLTypeName.DATE, mapToStandardSQLType("DATE"));
    }

    @Test
    void mapToStandardSQLType_numericFamily() throws Exception {
        assertEquals(StandardSQLTypeName.NUMERIC, mapToStandardSQLType("NUMERIC"));
        assertEquals(StandardSQLTypeName.BIGNUMERIC, mapToStandardSQLType("BIGNUMERIC"));
        assertEquals(StandardSQLTypeName.BYTES, mapToStandardSQLType("BYTES"));
    }

    @Test
    void mapToStandardSQLType_lowerCaseInput_isCaseInsensitive() throws Exception {
        assertEquals(StandardSQLTypeName.STRING, mapToStandardSQLType("string"));
    }

    @Test
    void mapToStandardSQLType_record_throwsIllegalArgumentException() {
        // GAP: BigQuery's INFORMATION_SCHEMA reports nested/struct columns with
        // data_type = "RECORD" in some contexts. This switch has no RECORD case,
        // so mapToStandardSQLType (and therefore bqParseTdmQueryParam) blows up
        // on any such column instead of mapping it to StandardSQLTypeName.STRUCT.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> mapToStandardSQLType("RECORD"));
        assertTrue(ex.getMessage().contains("RECORD"));
    }

    @Test
    void mapToStandardSQLType_struct_throwsIllegalArgumentException() {
        // GAP: same as RECORD above but for the "STRUCT<...>" spelling BigQuery
        // uses in other contexts. Note this is NOT caught by the ARRAY<...>
        // special case (that only matches a literal "ARRAY<" prefix), and there
        // is no STRUCT case in the switch, so it always falls to the default throw.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> mapToStandardSQLType("STRUCT<x INT64>"));
        assertTrue(ex.getMessage().contains("STRUCT<x INT64>"));
    }

    @Test
    void mapToStandardSQLType_unknownType_throwsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> mapToStandardSQLType("GEOGRAPHY"));
        assertTrue(ex.getMessage().contains("GEOGRAPHY"));
    }

    // =====================================================================
    // bqParseTdmQueryParam
    // =====================================================================

    @Test
    void bqParseTdmQueryParam_stringType_returnsRawStringValue() {
        Object result = SharedLogic.bqParseTdmQueryParam("f", "hello", "STRING(10)");
        assertEquals("hello", result);
    }

    @Test
    void bqParseTdmQueryParam_integerType_returnsLong() {
        Object result = SharedLogic.bqParseTdmQueryParam("f", "42", "INTEGER");
        assertEquals(42L, result);
    }

    @Test
    void bqParseTdmQueryParam_numericType_returnsBigDecimal() {
        Object result = SharedLogic.bqParseTdmQueryParam("f", "3.14", "NUMERIC(10,2)");
        assertEquals(new java.math.BigDecimal("3.14"), result);
    }

    @Test
    void bqParseTdmQueryParam_unsupportedRecordType_throws() {
        // Same RECORD gap as above, surfaced through the public entry point.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SharedLogic.bqParseTdmQueryParam("f", "x", "RECORD"));
        assertTrue(ex.getMessage().contains("RECORD"));
    }

    // =====================================================================
    // bqReplaceFilterPlaceholders
    // =====================================================================

    @Test
    void bqReplaceFilterPlaceholders_nullFilter_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> SharedLogic.bqReplaceFilterPlaceholders(null, List.of()));
    }

    @Test
    void bqReplaceFilterPlaceholders_nullValues_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> SharedLogic.bqReplaceFilterPlaceholders("a = ?", null));
    }

    @Test
    void bqReplaceFilterPlaceholders_tooFewValues_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> SharedLogic.bqReplaceFilterPlaceholders("a = ? AND b = ?", List.of(1)));
    }

    @Test
    void bqReplaceFilterPlaceholders_tooManyValues_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> SharedLogic.bqReplaceFilterPlaceholders("a = ?", List.of(1, 2)));
    }

    @Test
    void bqReplaceFilterPlaceholders_noPlaceholdersAndNoValues_returnsFilterUnchanged() {
        assertEquals("SELECT 1", SharedLogic.bqReplaceFilterPlaceholders("SELECT 1", List.of()));
    }

    @Test
    void bqReplaceFilterPlaceholders_stringValueWithSingleQuote_isEscaped() {
        String result = SharedLogic.bqReplaceFilterPlaceholders("name = ?", List.of("O'Brien"));
        assertEquals("name = 'O''Brien'", result);
    }

    @Test
    void bqReplaceFilterPlaceholders_nullValue_becomesNullLiteral() {
        List<Object> values = new ArrayList<>();
        values.add(null);
        String result = SharedLogic.bqReplaceFilterPlaceholders("a = ?", values);
        assertEquals("a = NULL", result);
    }

    @Test
    void bqReplaceFilterPlaceholders_numericAndBooleanValues_areInlinedWithoutQuotes() {
        String result = SharedLogic.bqReplaceFilterPlaceholders("a = ? AND b = ?", List.of(5, true));
        assertEquals("a = 5 AND b = true", result);
    }

    @Test
    void bqReplaceFilterPlaceholders_stringValueContainingDollarDigit_throwsIndexOutOfBounds() {
        // BUG (high confidence, verified against the JDK's actual Matcher behavior):
        // The code passes the escaped replacement string straight to
        // Matcher.appendReplacement(StringBuilder, String) (SharedLogic.java:72).
        // That method treats '$' followed by digits in the REPLACEMENT text as a
        // regex backreference (e.g. "$100" -> group 1, then digits "00" literal),
        // even though this pattern ("\\?") has zero capture groups. Any filter value
        // containing something like a dollar amount ("$100") throws instead of being
        // inlined as a literal.
        IndexOutOfBoundsException ex = assertThrows(IndexOutOfBoundsException.class,
                () -> SharedLogic.bqReplaceFilterPlaceholders("a = ?", List.of("50% and $100")));
        assertTrue(ex.getMessage().contains("group"));
    }

    @Test
    void bqReplaceFilterPlaceholders_stringValueContainingLoneDollarSign_throwsIllegalArgumentException() {
        // Same root cause as above: even a '$' not followed by a digit still trips
        // the backreference parser ("Illegal group reference") instead of being
        // treated as a literal character.
        assertThrows(IllegalArgumentException.class,
                () -> SharedLogic.bqReplaceFilterPlaceholders("a = ?", List.of("$ off today")));
    }

    @Test
    void bqReplaceFilterPlaceholders_stringValueContainingBackslash_isSilentlyCorrupted() {
        // BUG (high confidence, verified against the JDK's actual Matcher behavior):
        // appendReplacement's replacement-text parser also treats a lone backslash as
        // an escape character: '\' + next-char is replaced by just next-char, i.e. the
        // backslash silently vanishes from the output instead of being preserved
        // literally. This corrupts any filter value containing backslashes (Windows
        // paths, regex text, etc.) without throwing anything - the caller gets a wrong
        // result with no signal that something went wrong.
        String result = SharedLogic.bqReplaceFilterPlaceholders("path = ?", List.of("C:\\temp\\file"));
        assertEquals("path = 'C:tempfile'", result); // note: both backslashes silently dropped
    }

    // =====================================================================
    // bqParentRowsMapper
    //
    // LUType.getTypeByName(...) normally resolves through a lazily-installed
    // default Function backed by the live Fabric LU registry
    // (LUTypeFactoryImpl.getInstance()), which isn't available in this bare
    // JUnit harness. However LUType exposes a public static
    // setLuTypeByName(Function<String,LUType>) that fully overrides that
    // resolution function, and both LUType (public constructor) and LudbColumn
    // (public no-arg constructor + public fields) are plain in-memory objects
    // with no file/DB/network I/O in their constructors (verified by
    // disassembling cdbms-core-8.5.0_176-SNAPSHOT.jar - the LUType constructor
    // only builds TreeMaps/HashMaps/ArrayLists and a zero-URL URLClassLoader).
    // This makes the method fully unit-testable without a running Fabric
    // project, by installing a small in-test HashMap-backed registry.
    // =====================================================================

    private static Function<String, LUType> originalLuTypeByName;

    @BeforeAll
    static void captureOriginalLuTypeRegistry() {
        // Snapshot (without invoking) whatever resolver was installed/defaulted
        // before this test class ran, so it can be restored afterwards and not
        // leak into any other test class sharing this JVM.
        originalLuTypeByName = LUType.getLuTypeByName();
    }

    @AfterAll
    static void restoreOriginalLuTypeRegistry() {
        LUType.setLuTypeByName(originalLuTypeByName);
    }

    private static LUType luTypeWithEntityIdColumn(String luName, String colName, String colType) {
        LUType lu = new LUType(luName);
        // LUType's constructor already pre-populates ludbEntityIDColumnObject
        // with a fresh LudbColumn(); just fill in the two fields SharedLogic reads.
        LudbColumn column = lu.ludbEntityIDColumnObject;
        column.originColumnName = colName;
        column.originalColumnDataType = colType;
        return lu;
    }

    private static void registerLuType(LUType lu) {
        Map<String, LUType> registry = new HashMap<>();
        registry.put(lu.luName, lu);
        LUType.setLuTypeByName(registry::get);
    }

    @Test
    void bqParentRowsMapper_nullParentRows_returnsEmptyList() {
        Iterable<Map<String, Object>> result = SharedLogic.bqParentRowsMapper("anyLu", "table", null);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    void bqParentRowsMapper_integerColType_convertsViaParamConvertorToInteger() {
        LUType lu = luTypeWithEntityIdColumn("lu_int_" + UUID.randomUUID(), "ID", "integer");
        registerLuType(lu);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", "42");
        row.put("NAME", "foo");
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertEquals(42L, converted.get("ID"));
        assertEquals("foo", converted.get("NAME"));
    }

    @Test
    void bqParentRowsMapper_realColType_convertsViaParamConvertorToReal() {
        LUType lu = luTypeWithEntityIdColumn("lu_real_" + UUID.randomUUID(), "ID", "real");
        registerLuType(lu);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", "3.14");
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertEquals(3.14d, (Double) converted.get("ID"), 0.0001);
    }

    @Test
    void bqParentRowsMapper_dateTimeColType_convertsViaParamConvertorToDate() {
        LUType lu = luTypeWithEntityIdColumn("lu_date_" + UUID.randomUUID(), "ID", "datetime");
        registerLuType(lu);

        java.util.Date now = new java.util.Date();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", now);
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertEquals(now, converted.get("ID"));
    }

    @Test
    void bqParentRowsMapper_blobColType_convertsViaParamConvertorToBuffer() {
        LUType lu = luTypeWithEntityIdColumn("lu_blob_" + UUID.randomUUID(), "ID", "blob");
        registerLuType(lu);

        byte[] bytes = { 1, 2, 3 };
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", bytes);
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertArrayEquals(bytes, (byte[]) converted.get("ID"));
    }

    @Test
    void bqParentRowsMapper_textColType_convertsViaParamConvertorToString() {
        LUType lu = luTypeWithEntityIdColumn("lu_text_" + UUID.randomUUID(), "ID", "text");
        registerLuType(lu);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 42L);
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertEquals("42", converted.get("ID"));
    }

    @Test
    void bqParentRowsMapper_unknownColType_leavesValueUnchanged() {
        LUType lu = luTypeWithEntityIdColumn("lu_other_" + UUID.randomUUID(), "ID", "geography");
        registerLuType(lu);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", "raw-value");
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertEquals("raw-value", converted.get("ID"));
    }

    @Test
    void bqParentRowsMapper_rowMissingEntityIdColumn_isLeftAsIs() {
        LUType lu = luTypeWithEntityIdColumn("lu_missing_" + UUID.randomUUID(), "ID", "integer");
        registerLuType(lu);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OTHER", "value"); // no "ID" key present at all
        List<Map<String, Object>> parentRows = List.of(row);

        Map<String, Object> converted = SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows)
                .iterator().next();

        assertEquals("value", converted.get("OTHER"));
        assertFalse(converted.containsKey("ID"));
    }

    @Test
    void bqParentRowsMapper_originalParentRowsAreNotMutated() {
        LUType lu = luTypeWithEntityIdColumn("lu_immutable_" + UUID.randomUUID(), "ID", "integer");
        registerLuType(lu);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", "7");
        List<Map<String, Object>> parentRows = new ArrayList<>();
        parentRows.add(row);

        SharedLogic.bqParentRowsMapper(lu.luName, "table", parentRows);

        // The method is documented/expected to copy each row via
        // `new LinkedHashMap<>(originalRow)` before mutating it, so the caller's
        // original map/value must remain untouched (still the raw String "7",
        // not converted to Long 7L).
        assertEquals("7", row.get("ID"));
        assertInstanceOf(String.class, row.get("ID"));
    }

    // =====================================================================
    // bqGetDatasetsProject
    //
    // InterfacesManager.getInstance() is a classic lazy singleton whose
    // constructor only builds empty in-memory maps (verified by
    // disassembling cdbms-core-8.5.0_176-SNAPSHOT.jar) - no bootstrap
    // required. It exposes a public addInterface(FabricInterface, String env)
    // that registers an interface under a given name/environment, which is
    // exactly what bqGetDatasetsProject looks up via getTypedInterface. Since
    // GenericInterface (implements FabricInterface) has all abstract members
    // implemented and only needs its protected `name`/`properties` fields
    // populated, a tiny same-file subclass is enough to exercise this method
    // for real, without mocks.
    //
    // Caveat: InterfacesManager's registry is a process-wide static singleton
    // shared by the whole test JVM. To avoid any chance of collision with
    // other concurrently-authored test classes in this suite, the interface
    // name and environment used below are randomized per run.
    // =====================================================================

    private static class TestGenericInterface extends GenericInterface {
        TestGenericInterface(String name) {
            this.name = name;
        }

        void setProp(String key, String value) {
            this.properties.put(key, value);
        }
    }

    @Test
    void bqGetDatasetsProject_delegatesToInterfacesManager() {
        String ifaceName = "shared-logic-test-iface-" + UUID.randomUUID();
        String env = "shared-logic-test-env-" + UUID.randomUUID();

        TestGenericInterface iface = new TestGenericInterface(ifaceName);
        iface.setProp(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT, "my-project-id");

        InterfacesManager.getInstance().addInterface(iface, env);

        String result = SharedLogic.bqGetDatasetsProject(ifaceName, env);

        assertEquals("my-project-id", result);
    }

    // =====================================================================
    // bqQueryBuilderAddLimit
    // =====================================================================

    @Test
    void bqQueryBuilderAddLimit_nullSql_returnsNull() {
        assertNull(SharedLogic.bqQueryBuilderAddLimit(null, 10));
    }

    @Test
    void bqQueryBuilderAddLimit_emptySql_returnsAsIs() {
        assertEquals("", SharedLogic.bqQueryBuilderAddLimit("", 10));
    }

    @Test
    void bqQueryBuilderAddLimit_blankSql_returnsAsIs() {
        assertEquals("   ", SharedLogic.bqQueryBuilderAddLimit("   ", 10));
    }

    @Test
    void bqQueryBuilderAddLimit_bareSelect_appendsLimit() {
        assertEquals("SELECT * FROM t LIMIT 5;", SharedLogic.bqQueryBuilderAddLimit("SELECT * FROM t", 5));
    }

    @Test
    void bqQueryBuilderAddLimit_selectWithTrailingSemicolon_appendsLimitBeforeSemicolon() {
        assertEquals("SELECT * FROM t LIMIT 5;", SharedLogic.bqQueryBuilderAddLimit("SELECT * FROM t;", 5));
    }

    @Test
    void bqQueryBuilderAddLimit_existingLimitClause_leftUntouched() {
        assertEquals("SELECT * FROM t LIMIT 10", SharedLogic.bqQueryBuilderAddLimit("SELECT * FROM t LIMIT 10", 5));
    }

    @Test
    void bqQueryBuilderAddLimit_existingLimitClauseIsCaseInsensitive_leftUntouched() {
        assertEquals("select * from t limit 10",
                SharedLogic.bqQueryBuilderAddLimit("select * from t limit 10", 5));
    }

    @Test
    void bqQueryBuilderAddLimit_hasLimitFalsePositiveInsideStringLiteral_bugDemonstration() {
        // BUG (medium-high confidence): hasLimit's regex "(?s).*\\blimit\\s+\\d+.*"
        // (SharedLogic.java:258) runs over the whole statement text with no
        // awareness of string literals - unlike splitStatementsSafely, which IS
        // quote/comment-aware for finding statement *boundaries*. Here the only
        // occurrence of "limit 5" is inside a single-quoted string literal, not a
        // real LIMIT clause, yet hasLimit reports true and the method skips
        // appending a real LIMIT entirely. A real BigQuery engine would run this
        // exact SQL with no row cap at all.
        String sql = "SELECT * FROM t WHERE x = 'limit 5'";
        assertEquals(sql, SharedLogic.bqQueryBuilderAddLimit(sql, 5));
    }

    @Test
    void bqQueryBuilderAddLimit_lineCommentContainingSemicolon_isNotTreatedAsStatementBoundary() {
        // splitStatementsSafely IS comment-aware (unlike hasLimit above): a ';'
        // inside a "-- ..." line comment must not split the statement, so the
        // single logical statement still starts with SELECT and gets LIMIT
        // appended at its true end.
        String sql = "SELECT * FROM t -- note; keep going\nWHERE id = 1";
        String expected = "SELECT * FROM t -- note; keep going\nWHERE id = 1 LIMIT 5;";
        assertEquals(expected, SharedLogic.bqQueryBuilderAddLimit(sql, 5));
    }

    @Test
    void bqQueryBuilderAddLimit_blockCommentContainingSemicolon_isNotTreatedAsStatementBoundary() {
        String sql = "SELECT * FROM t /* block ; comment */ WHERE id = 1";
        String expected = "SELECT * FROM t /* block ; comment */ WHERE id = 1 LIMIT 5;";
        assertEquals(expected, SharedLogic.bqQueryBuilderAddLimit(sql, 5));
    }

    @Test
    void bqQueryBuilderAddLimit_multipleStatements_lastIsSelect_onlyLastGetsLimit() {
        String sql = "UPDATE t SET x=1; SELECT * FROM t";
        String expected = "UPDATE t SET x=1; SELECT * FROM t LIMIT 5;";
        assertEquals(expected, SharedLogic.bqQueryBuilderAddLimit(sql, 5));
    }

    @Test
    void bqQueryBuilderAddLimit_multipleStatements_lastIsNotSelect_nothingIsTouched() {
        // Per the code, only statements.get(last) is ever inspected/modified. If the
        // final statement isn't a SELECT, the whole original SQL is returned
        // unchanged - even an earlier SELECT statement in the same batch is left
        // without a LIMIT.
        String sql = "SELECT * FROM t; UPDATE t SET x=1";
        assertEquals(sql, SharedLogic.bqQueryBuilderAddLimit(sql, 5));
    }

    @Test
    void bqQueryBuilderAddLimit_cteQuery_neverGetsLimitAppended_gap() {
        // GAP: lastStmt.toLowerCase().startsWith("select") is false for a
        // statement that begins with "WITH ...", so any CTE-based query is
        // returned completely unchanged - bqQueryBuilderAddLimit can never add a
        // row cap to a "WITH cte AS (...) SELECT ..." statement. This may be an
        // intentional limitation, but it's a real, surprising gap worth flagging:
        // any TDM/query-builder caller relying on this to always cap result size
        // will silently get an uncapped query whenever the user writes a CTE.
        String sql = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        assertEquals(sql, SharedLogic.bqQueryBuilderAddLimit(sql, 5));
    }
}
