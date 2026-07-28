package com.k2view.cdbms.usercode.common.BigQuery;

import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.storage.v1.WriteStream.Type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * WriteStream.createWriteStream(Type, String, Credentials) is a static factory. Its PENDING and
 * default (TYPE_UNSPECIFIED/anything-else) arms construct PendingWriteStream/DefaultWriteStream,
 * whose constructors call the real BigQueryWriteClient.create(...) against live Google Cloud
 * infrastructure - not something safe to invoke from a unit test (risk of hanging on DNS/network
 * resolution in a sandboxed CI environment, per the assignment's own caution, and PendingWriteStream/
 * DefaultWriteStream are covered directly by a sibling agent anyway).
 *
 * What IS safely and fully unit-testable without touching any Google Cloud code is the switch's
 * own dispatch/routing logic for the arms that throw before ever reaching a constructor:
 * BUFFERED, COMMITTED, and UNRECOGNIZED. Those three tests, plus a source read of the switch,
 * are enough to confirm there's no dead/overlapping logic between the explicit `UNRECOGNIZED`
 * arm and the `default` arm: `default` only ever matches TYPE_UNSPECIFIED (the enum's 0 value)
 * because every other declared Type constant (PENDING, BUFFERED, COMMITTED, UNRECOGNIZED) has its
 * own explicit case; UNRECOGNIZED can never fall into `default` and vice versa.
 *
 * PENDING/default routing to the correct concrete class is therefore verified by code reading,
 * not by executing createWriteStream(PENDING/TYPE_UNSPECIFIED, ...) here.
 */
class WriteStreamTest {

    private static final String PROJECT_ID = "test-project";

    // A credentials value is never actually used by the BUFFERED/COMMITTED/UNRECOGNIZED arms
    // (they throw before touching it), so null is passed directly below.

    // --- createWriteStream(Type, String, Credentials) ---

    @Test
    void createWriteStream_buffered_throwsIllegalArgumentException_beforeTouchingNetwork() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriteStream.createWriteStream(Type.BUFFERED, PROJECT_ID, null));
        assertTrue(ex.getMessage().contains("Unimplemented type of stream"));
        assertTrue(ex.getMessage().contains("BUFFERED"));
    }

    @Test
    void createWriteStream_committed_throwsIllegalArgumentException_beforeTouchingNetwork() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriteStream.createWriteStream(Type.COMMITTED, PROJECT_ID, null));
        assertTrue(ex.getMessage().contains("Unimplemented type of stream"));
        assertTrue(ex.getMessage().contains("COMMITTED"));
    }

    @Test
    void createWriteStream_unrecognized_throwsIllegalArgumentException_withDedicatedMessage() {
        // Confirms UNRECOGNIZED has its own explicit arm and is never silently swallowed by
        // `default` (which would otherwise attempt to build a DefaultWriteStream instead of
        // throwing - a real routing bug, but NOT what the code does).
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriteStream.createWriteStream(Type.UNRECOGNIZED, PROJECT_ID, null));
        assertEquals("Unrecognized type of stream.", ex.getMessage());
    }

    // --- abort() default method ---

    @Test
    void abort_defaultMethod_delegatesToClose() throws Exception {
        RecordingWriteStream stream = new RecordingWriteStream();

        stream.abort();

        assertTrue(stream.closed);
    }

    /** Minimal, network-free WriteStream implementation used purely to observe abort()'s delegation. */
    private static class RecordingWriteStream implements WriteStream {
        boolean closed = false;

        @Override
        public void write(String dataset, String table, JSONArray rows) {
            // not exercised by this test
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
