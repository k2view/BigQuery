package com.k2view.cdbms.usercode.common.BigQuery;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.basic.IoSimpleRow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BigQueryCommandIoSessionTest {

    // ============================================================================================
    // Test helpers
    // ============================================================================================

    /** Reflectively invokes the private static BigQueryCommandStatement.replaceProjectId(String, String). */
    private static String replaceProjectId(String sql, String projectId) throws Exception {
        Class<?> statementClass = Class.forName(
                "com.k2view.cdbms.usercode.common.BigQuery.BigQueryCommandIoSession$BigQueryCommandStatement");
        Method m = statementClass.getDeclaredMethod("replaceProjectId", String.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, sql, projectId);
    }

    private static Map<String, Object> baseProps(String datasetsProjectId) {
        Map<String, Object> props = new HashMap<>();
        props.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "DEFAULT");
        props.put(BigQueryIoProvider.SESSION_PROP_INTERFACE, "testInterface");
        props.put(BigQueryIoProvider.SESSION_PROP_USER_PROJECT, "userProj");
        props.put(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT, datasetsProjectId);
        props.put(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE, false);
        return props;
    }

    /**
     * A BigQueryCommandIoSession whose package-private client() is overridden to return a
     * caller-supplied (mocked) BigQuery client instead of ever touching real credentials/network -
     * this lets execute() run its full real logic (job submission, result/error handling, row
     * decoding) against a fully controlled fake client.
     */
    private static class FakeSession extends BigQueryCommandIoSession {
        private final BigQuery fakeClient;

        FakeSession(Map<String, Object> props, BigQuery fakeClient) {
            super(props);
            this.fakeClient = fakeClient;
        }

        @Override
        BigQuery client() {
            return fakeClient;
        }
    }

    /** Builds a BigQuery/Job/JobStatus mock chain that succeeds and returns the given TableResult. */
    private static BigQuery mockSuccessfulClient(TableResult tableResult, QueryStatistics statistics) throws Exception {
        JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(null);

        Job job = mock(Job.class);
        when(job.getStatus()).thenReturn(status);
        when(job.isDone()).thenReturn(true);
        when(job.waitFor()).thenReturn(job);
        when(job.getQueryResults()).thenReturn(tableResult);
        when(job.<QueryStatistics>getStatistics()).thenReturn(statistics);

        BigQuery bq = mock(BigQuery.class);
        when(bq.create(any(JobInfo.class))).thenReturn(job);
        return bq;
    }

    // ============================================================================================
    // --- replaceProjectId ---
    // ============================================================================================

    @Test
    void replaceProjectId_singleOccurrence_isSubstituted() throws Exception {
        String result = replaceProjectId("SELECT * FROM $projectId.dataset.table", "proj1");
        assertEquals("SELECT * FROM proj1.dataset.table", result);
    }

    @Test
    void replaceProjectId_multipleOccurrences_areAllSubstituted() throws Exception {
        String result = replaceProjectId(
                "SELECT a.x FROM $projectId.ds1.t1 a JOIN $projectId.ds2.t2 b ON a.id = b.id", "proj1");
        assertEquals(
                "SELECT a.x FROM proj1.ds1.t1 a JOIN proj1.ds2.t2 b ON a.id = b.id", result);
    }

    @Test
    void replaceProjectId_noOccurrence_returnsUnchanged() throws Exception {
        String sql = "SELECT * FROM dataset.table";
        assertEquals(sql, replaceProjectId(sql, "proj1"));
    }

    @Test
    void replaceProjectId_occurrenceInsideSingleQuotedLiteral_isNotSubstituted() throws Exception {
        // The literal string value must be left alone; only the unquoted table reference is substituted.
        String result = replaceProjectId("SELECT '$projectId.foo' FROM $projectId.bar", "proj1");
        assertEquals("SELECT '$projectId.foo' FROM proj1.bar", result);
    }

    @Test
    void replaceProjectId_occurrenceInsideBacktickQuotedIdentifier_isSubstituted() throws Exception {
        // Backtick-quoted fully-qualified BigQuery identifiers are exactly where $projectId. is meant
        // to be expanded, and the parser only tracks single quotes ('), so this correctly substitutes.
        String result = replaceProjectId("SELECT * FROM `$projectId.dataset.table`", "proj1");
        assertEquals("SELECT * FROM `proj1.dataset.table`", result);
    }

    @Test
    void replaceProjectId_escapedSingleQuoteInLiteral_doesNotMisfire() throws Exception {
        // SQL escapes a quote inside a string literal by doubling it (''). A naive char-by-char
        // toggle parser flips `inString` on *every* quote character, including both of the pair -
        // but because the two ticks are adjacent (no content between them), the double-toggle
        // cancels out and the parser ends up back in the same (correct) "inString=true" state
        // immediately afterwards. So this specific, common escaping pattern does NOT misfire:
        // the token stays inside the literal and is correctly left untouched.
        String sql = "SELECT 'it''s $projectId.data' FROM t";
        assertEquals(sql, replaceProjectId(sql, "proj1"));
    }

    @Test
    void replaceProjectId_multipleEscapedQuotesInLiteral_stillDoesNotMisfire() throws Exception {
        String sql = "SELECT '$projectId.a''$projectId.b' FROM t";
        assertEquals(sql, replaceProjectId(sql, "proj1"));
    }

    @Test
    void replaceProjectId_apostropheInsideBacktickIdentifier_confusesQuoteTracking_realBug() throws Exception {
        // BUG: BigQuery backtick-quoted identifiers may legally contain characters such as an
        // apostrophe (e.g. a column/table named `foo's column`). Since replaceProjectId only tracks
        // single quotes and has no notion of backtick-quoting at all, a lone apostrophe inside a
        // backtick-quoted identifier flips `inString` to true and never flips back (no closing
        // single-quote follows) - so every $projectId. occurrence AFTER that point in the SQL is
        // silently left unexpanded, even though it appears in a legitimate backtick-quoted
        // identifier that should have been substituted. This is a real, concrete bug: it would
        // produce a query BigQuery rejects, because "$projectId" is never a valid literal dataset
        // segment - it must be substituted before reaching BigQuery.
        String sql = "SELECT `foo's column` FROM `$projectId.dataset.table`";
        String result = replaceProjectId(sql, "proj1");

        // Document the ACTUAL (buggy) behavior: the substitution is skipped.
        assertEquals(sql, result, "documents current (buggy) behavior: substitution is incorrectly skipped");
        assertTrue(result.contains("$projectId."),
                "the trailing $projectId. reference was left unexpanded because of the stray apostrophe");
    }

    @Test
    void replaceProjectId_tokenAtVeryEndOfString_isSubstitutedWithoutOverflow() throws Exception {
        String result = replaceProjectId("SELECT * FROM $projectId.", "proj1");
        assertEquals("SELECT * FROM proj1.", result);
    }

    @Test
    void replaceProjectId_unterminatedStringLiteral_leavesRestUnsubstituted() throws Exception {
        // Malformed/unterminated SQL: once inString flips true with no closing quote, everything
        // after is treated as "inside a string" for the rest of the scan - a known limitation of
        // this simple approach for genuinely malformed input, not something we consider a bug.
        String sql = "SELECT 'unterminated $projectId.x FROM t";
        assertEquals(sql, replaceProjectId(sql, "proj1"));
    }

    // ============================================================================================
    // --- BigQueryCommandStatement construction ---
    // ============================================================================================

    @Test
    void constructor_appliesReplaceProjectId_usingSessionsDatasetsProjectId() throws Exception {
        BigQueryCommandIoSession session = new BigQueryCommandIoSession(baseProps("myproj"));
        IoCommand.Statement stmt = session.prepareStatement("SELECT * FROM $projectId.dataset.table");

        java.lang.reflect.Field commandField = stmt.getClass().getDeclaredField("command");
        commandField.setAccessible(true);
        String command = (String) commandField.get(stmt);

        assertEquals("SELECT * FROM myproj.dataset.table", command);
    }

    @Test
    void constructor_differentDatasetsProjectId_producesDifferentCommand() throws Exception {
        BigQueryCommandIoSession sessionA = new BigQueryCommandIoSession(baseProps("projA"));
        BigQueryCommandIoSession sessionB = new BigQueryCommandIoSession(baseProps("projB"));

        IoCommand.Statement stmtA = sessionA.prepareStatement("SELECT * FROM $projectId.t");
        IoCommand.Statement stmtB = sessionB.prepareStatement("SELECT * FROM $projectId.t");

        java.lang.reflect.Field commandField = stmtA.getClass().getDeclaredField("command");
        commandField.setAccessible(true);

        assertEquals("SELECT * FROM projA.t", (String) commandField.get(stmtA));
        assertEquals("SELECT * FROM projB.t", (String) commandField.get(stmtB));
    }

    // ============================================================================================
    // --- execute(): positional parameters ---
    // ============================================================================================

    @Test
    void execute_nullParamsArray_resultsInEmptyPositionalParameterList() throws Exception {
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(null);
        when(tableResult.iterateAll()).thenReturn(List.of());

        Job job = mock(Job.class);
        JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);
        when(job.isDone()).thenReturn(true);
        when(job.waitFor()).thenReturn(job);
        when(job.getQueryResults()).thenReturn(tableResult);

        BigQuery bq = mock(BigQuery.class);
        ArgumentCaptor<JobInfo> captor = ArgumentCaptor.forClass(JobInfo.class);
        when(bq.create(captor.capture())).thenReturn(job);

        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT 1");
        stmt.execute((Object[]) null);

        QueryJobConfiguration config = captor.getValue().getConfiguration();
        assertNotNull(config.getPositionalParameters());
        assertTrue(config.getPositionalParameters().isEmpty());
    }

    @Test
    void execute_emptyParamsArray_alsoResultsInEmptyPositionalParameterList() throws Exception {
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(null);
        when(tableResult.iterateAll()).thenReturn(List.of());

        Job job = mock(Job.class);
        JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);
        when(job.isDone()).thenReturn(true);
        when(job.waitFor()).thenReturn(job);
        when(job.getQueryResults()).thenReturn(tableResult);

        BigQuery bq = mock(BigQuery.class);
        ArgumentCaptor<JobInfo> captor = ArgumentCaptor.forClass(JobInfo.class);
        when(bq.create(captor.capture())).thenReturn(job);

        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT 1");
        stmt.execute(); // varargs with no args => new Object[0], NOT null

        QueryJobConfiguration config = captor.getValue().getConfiguration();
        assertNotNull(config.getPositionalParameters());
        assertTrue(config.getPositionalParameters().isEmpty());
        // Confirms: params==null and params.length==0 are NOT distinguishable downstream - the
        // BigQuery client library's own Builder.setPositionalParameters(...) normalizes both a null
        // Iterable and an empty Iterable to the same empty list, so the null/empty-list branch in
        // BigQueryCommandStatement.execute is not a functional bug, just unnecessary defensiveness.
    }

    @Test
    void execute_withParams_mapsEachParamInOrderViaBigQueryParamParser() throws Exception {
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(null);
        when(tableResult.iterateAll()).thenReturn(List.of());

        Job job = mock(Job.class);
        JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);
        when(job.isDone()).thenReturn(true);
        when(job.waitFor()).thenReturn(job);
        when(job.getQueryResults()).thenReturn(tableResult);

        BigQuery bq = mock(BigQuery.class);
        ArgumentCaptor<JobInfo> captor = ArgumentCaptor.forClass(JobInfo.class);
        when(bq.create(captor.capture())).thenReturn(job);

        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT ?, ?");
        stmt.execute("hello", 42L);

        QueryJobConfiguration config = captor.getValue().getConfiguration();
        List<QueryParameterValue> params = config.getPositionalParameters();
        assertEquals(2, params.size());
        assertEquals(StandardSQLTypeName.STRING, params.get(0).getType());
        assertEquals("hello", params.get(0).getValue());
        assertEquals(StandardSQLTypeName.INT64, params.get(1).getType());
        assertEquals("42", params.get(1).getValue());
    }

    // ============================================================================================
    // --- execute(): job outcome handling ---
    // ============================================================================================

    @Test
    void execute_jobErrorPresent_throwsRuntimeExceptionWithErrorMessage() throws Exception {
        BigQueryError error = new BigQueryError("reason", "location", "custom failure message");
        JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(error);

        Job job = mock(Job.class);
        when(job.getStatus()).thenReturn(status);
        when(job.isDone()).thenReturn(true);
        when(job.waitFor()).thenReturn(job);

        BigQuery bq = mock(BigQuery.class);
        when(bq.create(any(JobInfo.class))).thenReturn(job);

        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT 1");

        RuntimeException ex = assertThrows(RuntimeException.class, stmt::execute);
        assertEquals("custom failure message", ex.getMessage());
    }

    @Test
    void execute_jobNotDoneAndNoError_throwsGenericRuntimeException() throws Exception {
        JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(null);

        Job job = mock(Job.class);
        when(job.getStatus()).thenReturn(status);
        when(job.isDone()).thenReturn(false);
        when(job.waitFor()).thenReturn(job);

        BigQuery bq = mock(BigQuery.class);
        when(bq.create(any(JobInfo.class))).thenReturn(job);

        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT 1");

        RuntimeException ex = assertThrows(RuntimeException.class, stmt::execute);
        assertTrue(ex.getMessage().contains("Failed to execute sql="));
        assertTrue(ex.getMessage().contains("SELECT 1"));
    }

    // ============================================================================================
    // --- BigQueryCommandResult: DDL/DML (null schema) path ---
    // ============================================================================================

    @Test
    void execute_ddlOrDml_returnsEmptyIterationAndEmptyLabelsAndTypes() throws Exception {
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(null);
        when(tableResult.iterateAll()).thenReturn(List.of());

        QueryStatistics stats = mock(QueryStatistics.class);
        when(stats.getNumDmlAffectedRows()).thenReturn(5L);

        BigQuery bq = mockSuccessfulClient(tableResult, stats);
        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("UPDATE $projectId.t SET x = 1");

        IoCommand.Result result = stmt.execute();

        assertFalse(result.iterator().hasNext());
        assertArrayEquals(new String[0], result.labels());
        assertArrayEquals(new Type[0], result.types());
        assertEquals(5, result.rowsAffected());
    }

    @Test
    void rowsAffected_narrowsLongToInt_truncatesForValuesBeyondIntRange() throws Exception {
        // NOTE: rowsAffected() is `(int) numDmlAffectedRows` - a long->int narrowing cast. For a
        // DML statement affecting more rows than Integer.MAX_VALUE (2^31-1), this silently
        // truncates/wraps instead of throwing or saturating. Extremely unlikely in practice (a
        // single DML statement affecting >2.1 billion rows) but a real, silent-corruption edge case.
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(null);
        when(tableResult.iterateAll()).thenReturn(List.of());

        long beyondIntRange = Integer.MAX_VALUE + 100L;
        QueryStatistics stats = mock(QueryStatistics.class);
        when(stats.getNumDmlAffectedRows()).thenReturn(beyondIntRange);

        BigQuery bq = mockSuccessfulClient(tableResult, stats);
        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("UPDATE $projectId.t SET x = 1");

        IoCommand.Result result = stmt.execute();

        int truncated = result.rowsAffected();
        assertEquals((int) beyondIntRange, truncated);
        assertTrue(truncated < 0, "demonstrates the silent wraparound: a huge positive row count "
                + "narrows into a negative int");
    }

    // ============================================================================================
    // --- BigQueryCommandResult: SELECT row-decoding happy path ---
    // ============================================================================================

    @Test
    void execute_selectQuery_decodesLabelsTypesAndRows() throws Exception {
        Field nameField = Field.of("name", StandardSQLTypeName.STRING);
        Field ageField = Field.of("age", StandardSQLTypeName.INT64);
        FieldList schemaFields = FieldList.of(nameField, ageField);
        Schema schema = Schema.of(schemaFields);

        FieldValueList row1 = FieldValueList.of(List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Alice"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "30")), schemaFields);

        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(List.of(row1));

        BigQuery bq = mockSuccessfulClient(tableResult, null);
        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT name, age FROM $projectId.t");

        IoCommand.Result result = stmt.execute();

        assertArrayEquals(new String[] {"name", "age"}, result.labels());
        assertEquals(String.class, result.types()[0]);
        assertEquals(Integer.class, result.types()[1]);
        assertEquals(-1, result.rowsAffected()); // no QueryStatistics => stays at the initial -1

        Iterator<IoCommand.Row> it = result.iterator();
        assertTrue(it.hasNext());
        IoCommand.Row row = it.next();
        assertEquals("Alice", row.get("name"));
        assertEquals(30L, row.get("age"));
        assertFalse(it.hasNext());
    }

    // ============================================================================================
    // --- BigQueryCommandResult: field parse failure -> empty values[] bug path ---
    // ============================================================================================

    @Test
    void next_fieldParseFailure_doesNotThrowButProducesRowThatSilentlyReturnsNullForEveryColumn()
            throws Exception {
        Field nameField = Field.of("name", StandardSQLTypeName.STRING);
        Field badAgeField = Field.of("badAge", StandardSQLTypeName.INT64);
        FieldList schemaFields = FieldList.of(nameField, badAgeField);
        Schema schema = Schema.of(schemaFields);

        // "not-a-number" makes parseBqValue's INT64 branch (Long.parseLong) throw a
        // NumberFormatException mid-row, for the SECOND field.
        FieldValueList row1 = FieldValueList.of(List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Alice"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "not-a-number")), schemaFields);

        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(List.of(row1));

        BigQuery bq = mockSuccessfulClient(tableResult, null);
        BigQueryCommandIoSession session = new FakeSession(baseProps("myproj"), bq);
        IoCommand.Statement stmt = session.prepareStatement("SELECT name, badAge FROM $projectId.t");

        IoCommand.Result result = stmt.execute();
        Iterator<IoCommand.Row> it = result.iterator();

        assertTrue(it.hasNext());
        // BUG (real, confirmed): next() swallows the parse exception (values = new Object[]{}; break;)
        // and hands the mismatched, zero-length values array to a row factory that was built for
        // TWO named columns (schemaFields = name, badAge). next() itself does not throw...
        IoCommand.Row row = it.next();

        // ...but the returned row is corrupted: EVERY column - including "name", whose own value
        // ("Alice") parsed just fine before the failure on "badAge" - silently reads back as null,
        // because IoSimpleRow's Row.get() bounds-checks the index against the (now empty) values
        // array and returns null instead of throwing. This masks the failure as if the row were
        // legitimately all-NULL, rather than surfacing that decoding failed.
        assertNull(row.get("name"), "expected 'Alice' but the empty values[] silently reports null");
        assertNull(row.get("badAge"));

        // toString()/entrySet() go through the same safe get()-based path and likewise do NOT
        // throw - they just render every column as null, which looks like ordinary (mis)data
        // rather than an error.
        String rendered = row.toString();
        assertTrue(rendered.contains("name=null") && rendered.contains("badAge=null"),
                "row prints as an all-null row instead of surfacing the decode failure");

        // However: forEach(BiConsumer) - a normal way to iterate a Map, and something downstream
        // Fabric code may well call directly on an IoCommand.Row - takes an UNGUARDED path
        // (VirtualMap$Row.forEach's lambda indexes the values[] array directly, with no bounds
        // check) and DOES throw, for the exact same row, with no indication this was ever a
        // "handled" error:
        assertThrows(ArrayIndexOutOfBoundsException.class,
                () -> row.forEach((k, v) -> { /* no-op */ }),
                "forEach() bypasses the bounds-checked get() path and blows up on the truncated array");

        assertFalse(it.hasNext());
    }

    /**
     * Direct, BigQuery-independent reproduction of the same IoSimpleRow inconsistency, isolating it
     * from BigQueryCommandResult so the row-factory behavior itself is unambiguous: building a row
     * for N declared column names but supplying a 0-length values array (exactly what
     * BigQueryCommandResult.next()'s catch block does) yields a row that reports a correct size(),
     * lies about every value via get()/toString() (silently null), and only actually throws via the
     * unguarded forEach() path.
     */
    @Test
    void ioSimpleRow_factoryAppliedToMismatchedEmptyArray_isSilentlyCorruptExceptViaForEach() {
        var factory = IoSimpleRow.factory(List.of("name", "badAge"));
        IoCommand.Row row = factory.apply(new Object[0]);

        assertEquals(2, row.size(), "size() reports the full declared column count");
        assertFalse(row.isEmpty());
        assertNull(row.get("name"));
        assertNull(row.get("badAge"));
        assertTrue(row.values().isEmpty(),
                "values() disagrees with size(): it reflects the truncated backing array, not the column count");
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> row.forEach((k, v) -> { }));
    }
}
