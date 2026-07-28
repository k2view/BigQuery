package com.k2view.cdbms.usercode.common.BigQuery;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.objenesis.ObjenesisStd;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.k2view.fabric.common.Log;

import io.grpc.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/*
 * PendingWriteStream's DataWriter/AppendCompleteCallback are private static nested classes that
 * reach a real JsonStreamWriter/BigQueryWriteClient through initialize(), which are not
 * constructible without live GCP infra. This test drives their PURE logic (error accumulation,
 * cleanup ordering, close()/abort()'s per-writer iteration, write()'s exception handling)
 * directly via reflection, bypassing initialize()/the public constructor entirely - no
 * network/GCP calls are ever made.
 *
 * NOTE ON COVERAGE GAP: unlike DefaultWriteStream.DataWriter.cleanup(), this class's
 * cleanup(BigQueryWriteClient) calls client.finalizeWriteStream(...) on its *successful* (no
 * prior error) path, and BigQueryWriteClient.finalizeWriteStream(...)/batchCommitWriteStreams(...)
 * /close() are all `final` methods (see javap output) that plain Mockito (no mockito-inline
 * available here) cannot intercept - stubbing them would invoke the real final method body
 * against a constructor-less mock and NPE. So only the "error already present" branch of
 * cleanup() (which returns/throws before ever touching the client) is safely testable here;
 * the happy-path finalize/commit/shutdown flow is not covered and would require live infra or
 * mockito-inline.
 */
class PendingWriteStreamTest {

    private static final String DATA_WRITER =
            "com.k2view.cdbms.usercode.common.BigQuery.PendingWriteStream$DataWriter";
    private static final String CALLBACK =
            "com.k2view.cdbms.usercode.common.BigQuery.PendingWriteStream$DataWriter$AppendCompleteCallback";

    private static Class<?> classFor(String name) throws Exception {
        return Class.forName(name);
    }

    private static Object newDataWriter() throws Exception {
        Constructor<?> ctor = classFor(DATA_WRITER).getDeclaredConstructor();
        ctor.setAccessible(true);
        return ctor.newInstance();
    }

    private static Object newCallback(Object dataWriter) throws Exception {
        Constructor<?> ctor = classFor(CALLBACK).getDeclaredConstructor(classFor(DATA_WRITER));
        ctor.setAccessible(true);
        return ctor.newInstance(dataWriter);
    }

    /**
     * Builds a PendingWriteStream without running its real constructor (which would make a live
     * GCP call via BigQueryWriteClient.create()), via Objenesis - the same bypass-the-constructor
     * mechanism Mockito itself uses internally. Since this skips every field initializer, `log`
     * (otherwise assigned by `Log.a(this.getClass())` at construction) must be injected by hand,
     * or the very first line of write()/abort() that logs would NPE.
     */
    private static PendingWriteStream newOuterWithLog() throws Exception {
        PendingWriteStream outer = new ObjenesisStd().newInstance(PendingWriteStream.class);
        setField(outer, PendingWriteStream.class, "log", Log.a(PendingWriteStream.class));
        return outer;
    }

    private static void setField(Object target, Class<?> declaringClass, String name, Object value) throws Exception {
        Field f = declaringClass.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object getField(Object target, Class<?> declaringClass, String name) throws Exception {
        Field f = declaringClass.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<RuntimeException> errorRef(Object dataWriter) throws Exception {
        return (AtomicReference<RuntimeException>) getField(dataWriter, classFor(DATA_WRITER), "error");
    }

    private static void invokeCleanup(Object dataWriter, BigQueryWriteClient client) throws Throwable {
        try {
            Method m = classFor(DATA_WRITER).getDeclaredMethod("cleanup", BigQueryWriteClient.class);
            m.setAccessible(true);
            m.invoke(dataWriter, client);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void invokeOnSuccess(Object callback, Object response) throws Throwable {
        try {
            Method m = classFor(CALLBACK).getDeclaredMethod("onSuccess", AppendRowsResponse.class);
            m.setAccessible(true);
            m.invoke(callback, response);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void invokeOnFailure(Object callback, Throwable throwable) throws Throwable {
        try {
            Method m = classFor(CALLBACK).getDeclaredMethod("onFailure", Throwable.class);
            m.setAccessible(true);
            m.invoke(callback, throwable);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    // --- DataWriter.cleanup ---

    @Test
    void cleanup_errorPresent_closesStreamWriterThenThrowsStoredError_neverTouchesClient() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);
        RuntimeException seeded = new RuntimeException("boom");
        errorRef(writer).set(seeded);

        // client is deliberately null: cleanup() must throw the stored error before it ever
        // dereferences the client parameter, or this call would NPE instead of surfacing `seeded`.
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> invokeCleanup(writer, null));

        assertSame(seeded, thrown);
        verify(mockStream).close();
    }

    // --- PendingWriteStream.close (bug #2: does one writer's cleanup() failure skip the rest?) ---

    @Test
    void close_firstWriterCleanupThrows_skipsRemainingWriters_neverClearsMap() throws Exception {
        PendingWriteStream outer = newOuterWithLog();

        Object writer1 = newDataWriter();
        JsonStreamWriter sw1 = mock(JsonStreamWriter.class);
        setField(writer1, classFor(DATA_WRITER), "streamWriter", sw1);
        RuntimeException seeded = new RuntimeException("writer1 failed");
        errorRef(writer1).set(seeded);

        Object writer2 = newDataWriter();
        JsonStreamWriter sw2 = mock(JsonStreamWriter.class);
        setField(writer2, classFor(DATA_WRITER), "streamWriter", sw2);
        // writer2 has no error recorded -> its cleanup()+commit() would run if ever reached.

        // LinkedHashMap pins iteration order so the test is deterministic: writer1 (which
        // throws) is guaranteed to be visited before writer2.
        Map<String, Object> writers = new LinkedHashMap<>();
        writers.put("t1", writer1);
        writers.put("t2", writer2);
        setField(outer, PendingWriteStream.class, "dataWriters", writers);
        // bigQueryWriteClient is intentionally left null: since writer1's cleanup() throws
        // before ever touching the client argument, this stays safe. If the bug were fixed
        // (loop continuing to writer2, whose cleanup() *would* reach the client), this test
        // would start NPE-ing instead of asserting - a useful tripwire in itself.

        Exception thrown = assertThrows(Exception.class, outer::close);

        assertSame(seeded, thrown);
        verify(sw1).close();
        verify(sw2, never()).close();

        // dataWriters.clear() (the statement after the for-loop) was never reached either -
        // the map still holds both entries, meaning writer2's data was neither committed
        // nor is there any remaining way for a caller to know it was left in limbo.
        Map<?, ?> remaining = (Map<?, ?>) getField(outer, PendingWriteStream.class, "dataWriters");
        assertEquals(2, remaining.size());
    }

    // --- PendingWriteStream.abort (same short-circuit risk as close(), via a separate forEach) ---

    @Test
    void abort_firstWriterCleanupThrows_skipsRemainingWriters() throws Exception {
        PendingWriteStream outer = newOuterWithLog();

        Object writer1 = newDataWriter();
        JsonStreamWriter sw1 = mock(JsonStreamWriter.class);
        setField(writer1, classFor(DATA_WRITER), "streamWriter", sw1);
        RuntimeException seeded = new RuntimeException("writer1 failed");
        errorRef(writer1).set(seeded);

        Object writer2 = newDataWriter();
        JsonStreamWriter sw2 = mock(JsonStreamWriter.class);
        setField(writer2, classFor(DATA_WRITER), "streamWriter", sw2);

        Map<String, Object> writers = new LinkedHashMap<>();
        writers.put("t1", writer1);
        writers.put("t2", writer2);
        setField(outer, PendingWriteStream.class, "dataWriters", writers);

        RuntimeException thrown = assertThrows(RuntimeException.class, outer::abort);

        assertSame(seeded, thrown);
        verify(sw1).close();
        verify(sw2, never()).close();
    }

    // --- PendingWriteStream.write (bug #1: is ExecutionException from append() really swallowed?) ---

    @Test
    void write_dataWriterAppendThrowsPreExistingRuntimeError_propagatesUncaught_becauseItIsNotAnExecutionException()
            throws Exception {
        // This test cannot exercise the literal `catch (ExecutionException e)` branch: reading
        // DataWriter.append(JSONArray)'s body shows it never actually throws a checked
        // ExecutionException in the current implementation (its `throws ExecutionException`
        // clause is vestigial/unreachable - nothing in the method body throws one; it doesn't
        // call future.get()). What IS reachable and directly demonstrates how narrow that catch
        // is: append() unconditionally rethrows any *pre-existing* RuntimeException recorded in
        // `error` before doing anything else. That RuntimeException is not an ExecutionException,
        // so write()'s catch does not touch it and it propagates uncaught - proving the catch
        // block swallows only its one specific exception type, not append() failures generally.
        PendingWriteStream outer = newOuterWithLog();
        setField(outer, PendingWriteStream.class, "datasetsProjectId", "proj");

        Object writer = newDataWriter();
        RuntimeException seeded = new RuntimeException("earlier append failed");
        errorRef(writer).set(seeded);

        TableName parentTable = TableName.of("proj", "ds", "tbl");
        Map<String, Object> writers = new HashMap<>();
        writers.put(parentTable.toString(), writer);
        setField(outer, PendingWriteStream.class, "dataWriters", writers);

        JSONArray rows = new JSONArray();
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> outer.write("ds", "tbl", rows));

        assertSame(seeded, thrown);
    }

    // --- AppendCompleteCallback.onSuccess ---

    @Test
    void onSuccess_deregistersInFlightRequest() throws Throwable {
        Object writer = newDataWriter();
        Object callback = newCallback(writer);

        Phaser phaser = (Phaser) getField(writer, classFor(DATA_WRITER), "inFlightRequestCount");
        assertEquals(1, phaser.getUnarrivedParties());

        invokeOnSuccess(callback, AppendRowsResponse.getDefaultInstance());

        assertEquals(0, phaser.getRegisteredParties());
    }

    // --- AppendCompleteCallback.onFailure (no retry logic here, unlike DefaultWriteStream's twin) ---

    @Test
    void onFailure_evenOnARetryableStatusCode_setsErrorImmediately_noRetryUnlikeDefaultWriteStream() throws Throwable {
        Object writer = newDataWriter();
        Object callback = newCallback(writer);

        // DefaultWriteStream's callback would retry an INTERNAL status; this twin has no such
        // logic at all - any failure unconditionally records an error.
        invokeOnFailure(callback, Status.INTERNAL.asRuntimeException());

        assertNotNull(errorRef(writer).get());
        Phaser phaser = (Phaser) getField(writer, classFor(DATA_WRITER), "inFlightRequestCount");
        assertEquals(0, phaser.getRegisteredParties());
    }

    @Test
    void onFailure_calledTwice_keepsFirstErrorInstance() throws Throwable {
        Object writer = newDataWriter();
        Object callback1 = newCallback(writer);
        Object callback2 = newCallback(writer);

        invokeOnFailure(callback1, new RuntimeException("first"));
        RuntimeException firstError = errorRef(writer).get();
        assertNotNull(firstError);

        invokeOnFailure(callback2, new RuntimeException("second"));

        assertSame(firstError, errorRef(writer).get());
    }
}
