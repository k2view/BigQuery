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

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.k2view.fabric.common.Log;

import io.grpc.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
 * DefaultWriteStream's DataWriter/AppendContext/AppendCompleteCallback are private static nested
 * classes that reach a real JsonStreamWriter/BigQueryWriteClient through initialize(), which are
 * not constructible without live GCP infra. This test drives their PURE logic (retry accounting,
 * error accumulation, cleanup ordering, close()'s per-writer iteration) directly via reflection,
 * bypassing initialize()/the public constructor entirely - no network/GCP calls are ever made.
 */
class DefaultWriteStreamTest {

    private static final String DATA_WRITER =
            "com.k2view.cdbms.usercode.common.BigQuery.DefaultWriteStream$DataWriter";
    private static final String APPEND_CONTEXT =
            "com.k2view.cdbms.usercode.common.BigQuery.DefaultWriteStream$AppendContext";
    private static final String CALLBACK =
            "com.k2view.cdbms.usercode.common.BigQuery.DefaultWriteStream$DataWriter$AppendCompleteCallback";

    private static Class<?> classFor(String name) throws Exception {
        return Class.forName(name);
    }

    private static Object newDataWriter() throws Exception {
        Constructor<?> ctor = classFor(DATA_WRITER).getDeclaredConstructor();
        ctor.setAccessible(true);
        return ctor.newInstance();
    }

    /**
     * Builds a DefaultWriteStream without running its real constructor (which would make a live
     * GCP call via BigQueryWriteClient.create()), via Objenesis - the same bypass-the-constructor
     * mechanism Mockito itself uses internally. Since this skips every field initializer, `log`
     * (otherwise assigned by `Log.a(this.getClass())` at construction) must be injected by hand,
     * or the very first line of write()/close() that logs would NPE.
     */
    private static DefaultWriteStream newOuterWithLog() throws Exception {
        DefaultWriteStream outer = new ObjenesisStd().newInstance(DefaultWriteStream.class);
        setField(outer, DefaultWriteStream.class, "log", Log.a(DefaultWriteStream.class));
        return outer;
    }

    private static Object newAppendContext(JSONArray data, int retryCount) throws Exception {
        Constructor<?> ctor = classFor(APPEND_CONTEXT).getDeclaredConstructor(JSONArray.class, int.class);
        ctor.setAccessible(true);
        return ctor.newInstance(data, retryCount);
    }

    private static Object newCallback(Object dataWriter, Object appendContext) throws Exception {
        Constructor<?> ctor = classFor(CALLBACK).getDeclaredConstructor(classFor(DATA_WRITER), classFor(APPEND_CONTEXT));
        ctor.setAccessible(true);
        return ctor.newInstance(dataWriter, appendContext);
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

    private static int getIntField(Object target, Class<?> declaringClass, String name) throws Exception {
        Field f = declaringClass.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(target);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<RuntimeException> errorRef(Object dataWriter) throws Exception {
        return (AtomicReference<RuntimeException>) getField(dataWriter, classFor(DATA_WRITER), "error");
    }

    private static void invokeCleanup(Object dataWriter) throws Throwable {
        try {
            Method m = classFor(DATA_WRITER).getDeclaredMethod("cleanup");
            m.setAccessible(true);
            m.invoke(dataWriter);
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
    void cleanup_noError_closesStreamWriterAndReturnsNormally() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);

        invokeCleanup(writer);

        verify(mockStream).close();
    }

    @Test
    void cleanup_errorPresent_closesStreamWriterThenThrowsStoredError() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);
        RuntimeException seeded = new RuntimeException("boom");
        errorRef(writer).set(seeded);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> invokeCleanup(writer));

        assertSame(seeded, thrown);
        verify(mockStream).close();
    }

    // --- DefaultWriteStream.close (bug #2: does one writer's cleanup() failure skip the rest?) ---

    @Test
    void close_firstWriterCleanupThrows_skipsRemainingWriters_neverReachesClientShutdown() throws Exception {
        DefaultWriteStream outer = newOuterWithLog();

        Object writer1 = newDataWriter();
        JsonStreamWriter sw1 = mock(JsonStreamWriter.class);
        setField(writer1, classFor(DATA_WRITER), "streamWriter", sw1);
        RuntimeException seeded = new RuntimeException("writer1 failed");
        errorRef(writer1).set(seeded);

        Object writer2 = newDataWriter();
        JsonStreamWriter sw2 = mock(JsonStreamWriter.class);
        setField(writer2, classFor(DATA_WRITER), "streamWriter", sw2);
        // writer2 has no error recorded -> its cleanup() would succeed if ever reached.

        // LinkedHashMap pins iteration order so the test is deterministic: writer1 (which
        // throws) is guaranteed to be visited by forEach before writer2.
        Map<String, Object> writers = new LinkedHashMap<>();
        writers.put("t1", writer1);
        writers.put("t2", writer2);
        setField(outer, DefaultWriteStream.class, "dataWriters", writers);
        // bigQueryWriteClient is intentionally left null: close()'s forEach(...) must throw
        // and propagate BEFORE the two lines that touch it, or this test would NPE instead
        // of surfacing `seeded`.

        RuntimeException thrown = assertThrows(RuntimeException.class, outer::close);

        assertSame(seeded, thrown);
        verify(sw1).close();
        verify(sw2, never()).close();
    }

    // --- DefaultWriteStream.write (contrast with PendingWriteStream's ExecutionException swallow) ---

    @Test
    void write_dataWriterHasPreExistingError_propagatesUncaught_noCatchAtAllInWrite() throws Exception {
        DefaultWriteStream outer = newOuterWithLog();
        setField(outer, DefaultWriteStream.class, "datasetsProjectId", "proj");

        Object writer = newDataWriter();
        RuntimeException seeded = new RuntimeException("earlier append failed");
        errorRef(writer).set(seeded);

        TableName parentTable = TableName.of("proj", "ds", "tbl");
        Map<String, Object> writers = new HashMap<>();
        writers.put(parentTable.toString(), writer);
        setField(outer, DefaultWriteStream.class, "dataWriters", writers);

        JSONArray rows = new JSONArray();
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> outer.write("ds", "tbl", rows));

        assertSame(seeded, thrown);
    }

    // --- AppendCompleteCallback.onSuccess ---

    @Test
    void onSuccess_deregistersInFlightRequest() throws Throwable {
        Object writer = newDataWriter();
        Object appendContext = newAppendContext(new JSONArray(), 0);
        Object callback = newCallback(writer, appendContext);

        Phaser phaser = (Phaser) getField(writer, classFor(DATA_WRITER), "inFlightRequestCount");
        assertEquals(1, phaser.getUnarrivedParties());

        invokeOnSuccess(callback, AppendRowsResponse.getDefaultInstance());

        assertEquals(0, phaser.getRegisteredParties());
    }

    // --- AppendCompleteCallback.onFailure (retry-count / error-accumulation logic) ---

    @Test
    void onFailure_retryableStatusUnderMaxRetries_incrementsRetryCountAndRetriesAppend_doesNotSetError() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        // A future that never completes: the retry's own callback is registered but never
        // fires, so this test observes only the retry dispatch itself, nothing downstream.
        when(mockStream.append(any(JSONArray.class))).thenReturn(SettableApiFuture.create());
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);

        Object appendContext = newAppendContext(new JSONArray(), 0);
        Object callback = newCallback(writer, appendContext);

        invokeOnFailure(callback, Status.INTERNAL.asRuntimeException());

        assertEquals(1, getIntField(appendContext, classFor(APPEND_CONTEXT), "retryCount"));
        verify(mockStream, times(1)).append(any(JSONArray.class));
        assertNull(errorRef(writer).get());
    }

    @Test
    void onFailure_nonRetryableStatus_setsErrorWithoutRetrying() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);
        Object appendContext = newAppendContext(new JSONArray(), 0);
        Object callback = newCallback(writer, appendContext);

        invokeOnFailure(callback, Status.INVALID_ARGUMENT.asRuntimeException());

        verify(mockStream, never()).append(any(JSONArray.class));
        assertNotNull(errorRef(writer).get());
        assertEquals(0, getIntField(appendContext, classFor(APPEND_CONTEXT), "retryCount"));
    }

    @Test
    void onFailure_retryableStatusButRetriesExhausted_setsErrorWithoutRetrying() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);
        Object appendContext = newAppendContext(new JSONArray(), 2); // == MAX_RETRY_COUNT
        Object callback = newCallback(writer, appendContext);

        invokeOnFailure(callback, Status.INTERNAL.asRuntimeException());

        verify(mockStream, never()).append(any(JSONArray.class));
        assertNotNull(errorRef(writer).get());
    }

    @Test
    void onFailure_calledTwice_keepsFirstErrorInstance() throws Throwable {
        Object writer = newDataWriter();
        JsonStreamWriter mockStream = mock(JsonStreamWriter.class);
        setField(writer, classFor(DATA_WRITER), "streamWriter", mockStream);
        Object ctx1 = newAppendContext(new JSONArray(), 0);
        Object ctx2 = newAppendContext(new JSONArray(), 0);
        Object callback1 = newCallback(writer, ctx1);
        Object callback2 = newCallback(writer, ctx2);

        invokeOnFailure(callback1, Status.INVALID_ARGUMENT.asRuntimeException());
        RuntimeException firstError = errorRef(writer).get();
        assertNotNull(firstError);

        invokeOnFailure(callback2, Status.INVALID_ARGUMENT.asRuntimeException());

        assertSame(firstError, errorRef(writer).get());
    }
}
