package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.k2view.cdbms.usercode.common.BigQuery.BigQueryWriteIoSession.BigQueryWriteStatement;
import com.k2view.discovery.rules.CrawlerRules;
import com.k2view.fabric.common.io.IoSession.IoSessionCompartment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/*
 * BigQueryWriteStatement.batch() lazily creates its `writeStream` via the static factory
 * WriteStream.createWriteStream(...), which (for PENDING/DEFAULT) constructs a real
 * BigQueryWriteClient against live Google Cloud infra. Mockito 4.11 here has no
 * mockito-inline/static-mocking, so that factory call cannot be intercepted. Every test below
 * that exercises batch()'s accumulation/flush/schema logic therefore pre-seeds the private
 * `writeStream` field via reflection with a Mockito mock BEFORE calling batch(), so the
 * `if (writeStream == null)` branch (and its static factory call) is never reached. Likewise,
 * `tableSchema` is seeded directly - it's a package-private field on the (non-static, public)
 * inner class BigQueryWriteStatement, reachable without reflection from this same-package test -
 * bypassing the client().getTable(...) network call.
 *
 * The one test that must reach the real BigQuerySession.client() (getMetadata(), which passes
 * client() as a constructor argument to BigQueryMetadata) uses a small subclass overriding the
 * package-private, non-final client() method instead.
 */
class BigQueryWriteIoSessionTest {

    private static Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD, "DEFAULT");
        props.put(BigQueryIoProvider.SESSION_PROP_INTERFACE, "testIface");
        props.put(BigQueryIoProvider.SESSION_PROP_USER_PROJECT, "job-project");
        props.put(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT, "data-project");
        props.put(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE, false);
        return props;
    }

    private static BigQueryWriteIoSession newSession() {
        return new BigQueryWriteIoSession(baseProps());
    }

    private static void setWriteStream(BigQueryWriteIoSession session, WriteStream writeStream) throws Exception {
        java.lang.reflect.Field f = BigQueryWriteIoSession.class.getDeclaredField("writeStream");
        f.setAccessible(true);
        f.set(session, writeStream);
    }

    private static WriteStream getWriteStream(BigQueryWriteIoSession session) throws Exception {
        java.lang.reflect.Field f = BigQueryWriteIoSession.class.getDeclaredField("writeStream");
        f.setAccessible(true);
        return (WriteStream) f.get(session);
    }

    private static Map<String, Object> inputWithData(String dataset, String table, Map<String, Object> data) {
        Map<String, Object> input = new HashMap<>();
        input.put(BigQueryWriteIoSession.INPUT_DATASET, dataset);
        input.put(BigQueryWriteIoSession.INPUT_TABLE, table);
        input.put(BigQueryWriteIoSession.INPUT_DATA, data);
        return input;
    }

    // batch()'s "data" map must be mutable: it calls data.replaceAll(...) in place to convert
    // each value via BigQueryParamParser. Map.of(...) would throw UnsupportedOperationException.
    private static Map<String, Object> row(String key, Object value) {
        Map<String, Object> data = new HashMap<>();
        data.put(key, value);
        return data;
    }

    /** Test-only subclass overriding the package-private, non-final client()/credentials() so
     *  no test ever reaches real Google Cloud auth/network resolution. */
    private static class TestSession extends BigQueryWriteIoSession {
        private final BigQuery mockClient;

        TestSession(Map<String, Object> props, BigQuery mockClient) {
            super(props);
            this.mockClient = mockClient;
        }

        @Override
        BigQuery client() {
            return mockClient;
        }
    }

    // --- batch: argument validation ---

    @Test
    void batch_noArgs_throwsIllegalArgumentException() {
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) newSession().statement();
        assertThrows(IllegalArgumentException.class, stmt::batch);
    }

    @Test
    void batch_nullArgsArray_throwsIllegalArgumentException() {
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) newSession().statement();
        assertThrows(IllegalArgumentException.class, () -> stmt.batch((Object[]) null));
    }

    @Test
    void batch_nonMapFirstArg_throwsIllegalArgumentException() {
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) newSession().statement();
        assertThrows(IllegalArgumentException.class, () -> stmt.batch("not-a-map"));
    }

    @Test
    void batch_notInTransaction_throwsIllegalStateException() {
        BigQueryWriteIoSession session = newSession();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        // beginTransaction() deliberately never called.
        Map<String, Object> input = inputWithData("ds", "tbl", row("a", 1));

        assertThrows(IllegalStateException.class, () -> stmt.batch(input));
    }

    @Test
    void execute_throwsUnsupportedOperationException() {
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) newSession().statement();
        assertThrows(UnsupportedOperationException.class, stmt::execute);
    }

    // --- batch: unknown-field-in-data behavior (silent-null hypothesis check) ---

    @Test
    void batch_unknownFieldInData_throwsIllegalArgumentException_ratherThanSilentlyNulling() throws Exception {
        // FieldList.get(String) (com.google.cloud.bigquery.FieldList, verified by bytecode
        // reading) THROWS IllegalArgumentException("Field with name '<x>' was not found") when
        // asked for a name it doesn't contain - it does NOT return null like Map.get would. So a
        // data-map key that isn't in the target table's schema (a typo, or an extra/renamed
        // field) makes batch() fail loudly with IllegalArgumentException, not silently convert
        // that value to null. This test refutes the "silent null on unknown field" hypothesis.
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
        setWriteStream(session, mock(WriteStream.class));

        Map<String, Object> data = new HashMap<>();
        data.put("typoField", "value");
        Map<String, Object> input = inputWithData("ds", "tbl", data);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> stmt.batch(input));
        assertTrue(ex.getMessage().contains("typoField"));
    }

    @Test
    void batch_knownField_convertsValueViaParamParser_beforeAccumulating() throws Exception {
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("id", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> data = new HashMap<>();
        data.put("id", 123); // int -> BigQueryParamParser.parseToBqByField converts to "123" for a STRING field
        Map<String, Object> input = inputWithData("ds", "tbl", data);
        input.put(BigQueryWriteIoSession.INPUT_BATCH_SIZE, 1); // flush immediately

        stmt.batch(input);

        org.mockito.ArgumentCaptor<JSONArray> captor = org.mockito.ArgumentCaptor.forClass(JSONArray.class);
        verify(writeStream, times(1)).write(eq("ds"), eq("tbl"), captor.capture());
        assertEquals(1, captor.getValue().length());
        assertEquals("123", captor.getValue().getJSONObject(0).get("id"));
    }

    // --- batch: batchSize accumulation/flush threshold ---

    @Test
    void batch_belowBatchSizeThreshold_accumulatesWithoutFlushing() throws Exception {
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> input = inputWithData("ds", "tbl", row("name", "a"));
        input.put(BigQueryWriteIoSession.INPUT_BATCH_SIZE, 5);

        stmt.batch(input);

        verify(writeStream, never()).write(any(), any(), any());
    }

    @Test
    void batch_reachesBatchSize_flushesAllAccumulatedRowsAtOnce() throws Exception {
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> input1 = inputWithData("ds", "tbl", row("name", "a"));
        input1.put(BigQueryWriteIoSession.INPUT_BATCH_SIZE, 2);
        Map<String, Object> input2 = inputWithData("ds", "tbl", row("name", "b"));
        input2.put(BigQueryWriteIoSession.INPUT_BATCH_SIZE, 2);

        stmt.batch(input1);
        verify(writeStream, never()).write(any(), any(), any());
        stmt.batch(input2);

        org.mockito.ArgumentCaptor<JSONArray> captor = org.mockito.ArgumentCaptor.forClass(JSONArray.class);
        verify(writeStream, times(1)).write(eq("ds"), eq("tbl"), captor.capture());
        assertEquals(2, captor.getValue().length());
    }

    @Test
    void batch_missingBatchSize_toIntegerOfNullIsZero_fallsBackToDefaultWithoutThrowing() throws Exception {
        // ParamConvertor.toInteger(null) -> toNumber(null) -> Integer.valueOf(0) (verified by
        // bytecode reading), so batchSize computes to 0, which is <= 0 and falls back to
        // DEFAULT_BATCH_SIZE (1000). It does NOT throw. This test refutes the hypothesis that
        // omitting INPUT_BATCH_SIZE would break batch().
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> input = inputWithData("ds", "tbl", row("name", "a"));
        // INPUT_BATCH_SIZE intentionally absent.

        assertDoesNotThrow(() -> stmt.batch(input));
        verify(writeStream, never()).write(any(), any(), any()); // 1 row is far below the 1000 default
    }

    // --- transaction lifecycle ---

    @Test
    void commit_flushesRemainingBatch_closesWriteStream_andEndsTransaction() throws Exception {
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> input = inputWithData("ds", "tbl", row("name", "a"));
        // Default batch size (1000) - one row never auto-flushes.
        stmt.batch(input);
        verify(writeStream, never()).write(any(), any(), any());

        session.commit();

        verify(writeStream, times(1)).write(eq("ds"), eq("tbl"), any(JSONArray.class));
        verify(writeStream, times(1)).close();
        verify(writeStream, never()).abort();
        assertNull(getWriteStream(session));
        // inTransaction reset to false -> a further batch() call must fail.
        assertThrows(IllegalStateException.class, () -> stmt.batch(input));
    }

    @Test
    void rollback_doesNotFlushPendingBatch_abortsWriteStreamInsteadOfClosing() throws Exception {
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> input = inputWithData("ds", "tbl", row("name", "a"));
        stmt.batch(input); // accumulates 1 row, default batch size (1000) never auto-flushes

        session.rollback();

        verify(writeStream, never()).write(any(), any(), any());
        verify(writeStream, times(1)).abort();
        verify(writeStream, never()).close();
        assertNull(getWriteStream(session));
    }

    @Test
    void abort_behavesLikeRollback_abortsWriteStreamInsteadOfClosing() throws Exception {
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        session.abort();

        verify(writeStream, times(1)).abort();
        verify(writeStream, never()).close();
        assertNull(getWriteStream(session));
    }

    @Test
    void commit_withNoAccumulatedData_doesNotThrow_evenWhenWriteStreamNeverCreated() throws Exception {
        // writeToBigQuery()'s accumulatedBatchSize <= 0 early-return guard means a commit() with
        // no batch() calls beforehand (writeStream still null) must not NPE.
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();

        assertDoesNotThrow(session::commit);
        assertNull(getWriteStream(session));
    }

    @Test
    void close_doesNotFlushPendingAccumulatedBatch_unlikeCommit() throws Exception {
        // Likely bug: BigQueryWriteIoSession.close() calls cleanup(true) directly, without first
        // calling writeToBigQuery() the way commit() does. If the framework ever closes this
        // session without an explicit prior commit(), any batch() rows accumulated below the
        // batchSize threshold are silently dropped - never written to BigQuery - even though the
        // write stream itself still gets finalized/closed as if everything had been flushed.
        BigQueryWriteIoSession session = newSession();
        session.beginTransaction();
        BigQueryWriteStatement stmt = (BigQueryWriteStatement) session.statement();
        stmt.tableSchema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
        WriteStream writeStream = mock(WriteStream.class);
        setWriteStream(session, writeStream);

        Map<String, Object> input = inputWithData("ds", "tbl", row("name", "a"));
        stmt.batch(input); // accumulates 1 row, default batch size (1000) never auto-flushes
        verify(writeStream, never()).write(any(), any(), any());

        session.close();

        verify(writeStream, never()).write(any(), any(), any()); // the pending row was never sent
        verify(writeStream, times(1)).close(); // yet the stream is finalized as if it had been
        assertNull(getWriteStream(session));
    }

    // --- misc session behavior ---

    @Test
    void isTransactional_returnsTrue() {
        assertTrue(newSession().isTransactional());
    }

    @Test
    void compartment_returnsShared() {
        assertEquals(IoSessionCompartment.SHARED, newSession().compartment());
    }

    @Test
    void getMetadata_withNoRegisteredInterface_throwsRatherThanSilentlyReturning() {
        // BigQueryMetadata's constructor (called with commandIoSession=null) falls back to
        // InterfacesManager.getInstance().getInterface(interfaceName).getIoSession(null) - in a
        // bare unit-test environment (no Fabric server/interface registry bootstrapped) this
        // interface lookup won't be registered, so construction is expected to fail fast (no
        // network involved, no hang). client() is overridden here purely so the eager
        // `client()` constructor-argument evaluation never reaches real Google Cloud auth.
        BigQuery mockClient = mock(BigQuery.class);
        TestSession session = new TestSession(baseProps(), mockClient);
        Map<String, Object> params = new HashMap<>();
        params.put("uuid", "job-1");
        params.put("rules", mock(CrawlerRules.class));

        assertThrows(Exception.class, () -> session.getMetadata(params));
    }
}
