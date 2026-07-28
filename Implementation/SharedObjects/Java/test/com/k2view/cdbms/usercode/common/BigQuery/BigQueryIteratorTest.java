package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
 * NOTE ON TEST STRATEGY:
 * BigQueryIterator's constructor only exercises the real BigQueryReadClient chain
 * (readClient.readRowsCallable().call(...).iterator()) when streamName != null. That chain
 * can't be driven from a plain Mockito mock: BigQueryReadClient.readRowsCallable() is a
 * *final* method, and Mockito 4.11 (no mockito-inline here) cannot intercept final methods -
 * calling it on a mock would execute the real final method body against a mock's null internal
 * fields and NPE inside the constructor itself.
 *
 * To exercise the "populated stream" behavior (multiple responses, limit cutoff, decode-error
 * handling) without touching that chain, every test below constructs the iterator with
 * streamName == null (safe: responseIterator becomes Collections.emptyIterator(), no client
 * interaction at all), then reflectively swaps the private final "responseIterator" field for a
 * controlled Iterator<ReadRowsResponse>, and re-invokes the private advanceToNextRecord() method
 * once to pick up the first record under the new iterator. The GenericDatumReader itself is a
 * real constructor argument (not final, mockable normally), so its .read(...) calls are fully
 * controlled via Mockito without needing real Avro binary encoding - matching the assignment's
 * suggestion that mocking the reader instead of real Avro bytes is an acceptable approach here.
 */
class BigQueryIteratorTest {

    private static ReadRowsResponse emptyResponse() {
        // Content is irrelevant since GenericDatumReader.read(...) is mocked and never actually
        // consumes the decoder's bytes; a 0-length payload just keeps BinaryDecoder.isEnd() from
        // ever getting in the way.
        return ReadRowsResponse.newBuilder()
                .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(ByteString.EMPTY).build())
                .build();
    }

    @SuppressWarnings("unchecked")
    private static BigQueryIterator withResponses(GenericDatumReader<GenericRecord> reader, long limit,
            Runnable assertAborted, BigQueryReadClient readClient, List<ReadRowsResponse> responses) throws Exception {
        BigQueryIterator it = new BigQueryIterator(readClient, null, reader, limit, assertAborted);
        Field responseIteratorField = BigQueryIterator.class.getDeclaredField("responseIterator");
        responseIteratorField.setAccessible(true);
        responseIteratorField.set(it, responses.iterator());
        Method advance = BigQueryIterator.class.getDeclaredMethod("advanceToNextRecord");
        advance.setAccessible(true);
        advance.invoke(it);
        return it;
    }

    private static BigQueryIterator withResponses(GenericDatumReader<GenericRecord> reader, long limit,
            Runnable assertAborted, List<ReadRowsResponse> responses) throws Exception {
        return withResponses(reader, limit, assertAborted, null, responses);
    }

    // --- constructor: streamName == null ---

    @Test
    void constructor_nullStreamName_yieldsNoRecords() {
        @SuppressWarnings("unchecked")
        GenericDatumReader<GenericRecord> reader = mock(GenericDatumReader.class);
        Runnable assertAborted = mock(Runnable.class);

        BigQueryIterator it = new BigQueryIterator(null, null, reader, 0, assertAborted);

        assertFalse(it.hasNext());
        // Note: next() does not throw NoSuchElementException when exhausted (it just returns the
        // null nextRecord) - a minor deviation from the java.util.Iterator contract, not the focus
        // of this test, but documented here so it isn't mistaken for an oversight.
        assertNull(it.next());
    }

    // --- hasNext(): assertAborted contract ---

    @Test
    void hasNext_invokesAssertAbortedEveryCall() {
        @SuppressWarnings("unchecked")
        GenericDatumReader<GenericRecord> reader = mock(GenericDatumReader.class);
        Runnable assertAborted = mock(Runnable.class);

        BigQueryIterator it = new BigQueryIterator(null, null, reader, 0, assertAborted);

        it.hasNext();
        it.hasNext();
        it.hasNext();

        verify(assertAborted, times(3)).run();
    }

    @Test
    void hasNext_abortedSession_throwsFromHasNextItself_notJustNext() {
        // Subtle contract: assertAborted.run() is called unconditionally at the top of hasNext(),
        // before the nextRecord == null check - so an aborted session throws from hasNext(), even
        // though there may still be a legitimately buffered nextRecord.
        @SuppressWarnings("unchecked")
        GenericDatumReader<GenericRecord> reader = mock(GenericDatumReader.class);
        Runnable assertAborted = mock(Runnable.class);
        RuntimeException abortSignal = new RuntimeException("aborted");
        doThrow(abortSignal).when(assertAborted).run();

        BigQueryIterator it = new BigQueryIterator(null, null, reader, 0, assertAborted);

        RuntimeException thrown = assertThrows(RuntimeException.class, it::hasNext);
        assertSame(abortSignal, thrown);
    }

    // --- limit semantics ---

    @Test
    void advanceToNextRecord_limitZero_readsUntilResponseIteratorExhausted() throws Exception {
        @SuppressWarnings("unchecked")
        GenericDatumReader<GenericRecord> reader = mock(GenericDatumReader.class);
        GenericRecord r1 = mock(GenericRecord.class);
        GenericRecord r2 = mock(GenericRecord.class);
        GenericRecord r3 = mock(GenericRecord.class);
        when(reader.read(isNull(), any())).thenReturn(r1, r2, r3);

        BigQueryIterator it = withResponses(reader, 0, mock(Runnable.class),
                List.of(emptyResponse(), emptyResponse(), emptyResponse()));

        assertTrue(it.hasNext());
        assertSame(r1, it.next());
        assertTrue(it.hasNext());
        assertSame(r2, it.next());
        assertTrue(it.hasNext());
        assertSame(r3, it.next());
        assertFalse(it.hasNext(), "responseIterator is exhausted (3 responses, 3 reads) - iteration must end cleanly");
        verify(reader, times(3)).read(isNull(), any());
    }

    @Test
    void advanceToNextRecord_limitGreaterThanZero_stopsEarlyEvenWithMoreRowsUpstream() throws Exception {
        @SuppressWarnings("unchecked")
        GenericDatumReader<GenericRecord> reader = mock(GenericDatumReader.class);
        GenericRecord r1 = mock(GenericRecord.class);
        GenericRecord r2 = mock(GenericRecord.class);
        GenericRecord r3 = mock(GenericRecord.class);
        GenericRecord r4 = mock(GenericRecord.class);
        GenericRecord r5 = mock(GenericRecord.class);
        when(reader.read(isNull(), any())).thenReturn(r1, r2, r3, r4, r5);

        // 5 responses available upstream, but limit=2 must cut the iteration off after 2 rows.
        BigQueryIterator it = withResponses(reader, 2, mock(Runnable.class),
                List.of(emptyResponse(), emptyResponse(), emptyResponse(), emptyResponse(), emptyResponse()));

        assertTrue(it.hasNext());
        assertSame(r1, it.next());
        assertTrue(it.hasNext());
        assertSame(r2, it.next());
        assertFalse(it.hasNext(), "limit=2 reached - must stop even though 3 more responses/records remain upstream");
        verify(reader, times(2)).read(isNull(), any());
    }

    // --- catch-all exception handling in advanceToNextRecord() (bug candidate) ---

    @Test
    void advanceToNextRecord_decodeErrorMidStream_isSwallowed_hasNextReturnsFalseSilently() throws Exception {
        // This demonstrates the swallowed-exception bug candidate described for this class:
        // a genuine decode/stream error (reader.read throwing) is caught by the catch-all in
        // advanceToNextRecord(), logged, and turned into "no more records" - with no way for a
        // caller to tell this apart from "the stream really did end after 1 row".
        @SuppressWarnings("unchecked")
        GenericDatumReader<GenericRecord> reader = mock(GenericDatumReader.class);
        GenericRecord r1 = mock(GenericRecord.class);
        IOException decodeFailure = new IOException("simulated corrupt/truncated Avro row");
        when(reader.read(isNull(), any())).thenReturn(r1).thenThrow(decodeFailure);

        BigQueryReadClient readClient = mock(BigQueryReadClient.class);
        BigQueryIterator it = withResponses(reader, 0, mock(Runnable.class), readClient,
                List.of(emptyResponse(), emptyResponse()));

        assertTrue(it.hasNext());
        assertSame(r1, it.next()); // consuming r1 triggers the next advance, which hits decodeFailure internally

        assertFalse(it.hasNext(), "BUG: a real mid-stream decode error is indistinguishable from a clean end-of-stream");
        assertDoesNotThrow(it::hasNext, "the IOException from reader.read(...) must not surface to the caller");

        Field readClientField = BigQueryIterator.class.getDeclaredField("readClient");
        readClientField.setAccessible(true);
        assertNull(readClientField.get(it), "on the exception path the readClient field should be closed and nulled");
    }
}
