package com.k2view.cdbms.usercode.common.BigQuery;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoSession;

class BigQuerySnapshotTest {

    private static final String TABLE = "orders";
    private static final String SCHEMA_NAME = "sales";
    private static final String PROJECT_ID = "proj-1";

    private IoSession commandSession;
    private IoSession readSession;
    private SampleSize size;

    @BeforeEach
    void setUp() {
        commandSession = mock(IoSession.class);
        readSession = mock(IoSession.class);
        size = mock(SampleSize.class);
    }

    private BigQuerySnapshot newSnapshot(boolean useStorageApi) {
        return new BigQuerySnapshot(commandSession, readSession, TABLE, SCHEMA_NAME, PROJECT_ID, size, useStorageApi);
    }

    /** Stubs commandSession's row-count query (the __TABLES__ lookup) to return the given row count. */
    private void stubRowCount(long rowCount) throws Exception {
        IoCommand.Statement countStatement = mock(IoCommand.Statement.class);
        IoCommand.Result countResult = mock(IoCommand.Result.class);
        when(commandSession.prepareStatement(contains("__TABLES__"))).thenReturn(countStatement);
        when(countStatement.execute(any())).thenReturn(countResult);
        when(countResult.firstValue()).thenReturn(rowCount);
    }

    private void stubSampleSize(long min, long max, long percentage) {
        when(size.getMin()).thenReturn(min);
        when(size.getMax()).thenReturn(max);
        when(size.getPercentage()).thenReturn(percentage);
    }

    // --- getLimit() clamp branches (exercised indirectly through fetch(), non-storage-api path) ---

    @Test
    void fetch_rowCountPercentageBelowMin_clampsToMin() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(15); // 15 * 50 / 100 = 7 < min(10)

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        when(commandSession.prepareStatement(contains("select * from"))).thenReturn(readStatement);
        ArgumentCaptor<Object> limitCaptor = ArgumentCaptor.forClass(Object.class);
        when(readStatement.execute(limitCaptor.capture())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        newSnapshot(false).fetch();

        assertEquals(10L, ((Number) limitCaptor.getValue()).longValue());
    }

    @Test
    void fetch_rowCountPercentageInRange_usesComputedPercentage() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(500); // 500 * 50 / 100 = 250, within [10, 1000)

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        when(commandSession.prepareStatement(contains("select * from"))).thenReturn(readStatement);
        ArgumentCaptor<Object> limitCaptor = ArgumentCaptor.forClass(Object.class);
        when(readStatement.execute(limitCaptor.capture())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        newSnapshot(false).fetch();

        assertEquals(250L, ((Number) limitCaptor.getValue()).longValue());
    }

    @Test
    void fetch_rowCountPercentageAtMax_clampsToMax() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(2000); // 2000 * 50 / 100 = 1000 == max -> ">=" branch clamps to max

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        when(commandSession.prepareStatement(contains("select * from"))).thenReturn(readStatement);
        ArgumentCaptor<Object> limitCaptor = ArgumentCaptor.forClass(Object.class);
        when(readStatement.execute(limitCaptor.capture())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        newSnapshot(false).fetch();

        assertEquals(1000L, ((Number) limitCaptor.getValue()).longValue());
    }

    // --- fetch() branch shape: useStorageApi=false builds a literal SQL prepareStatement on commandSession ---

    @Test
    void fetch_nonStorageApi_buildsLiteralSqlAndExecutesOnCommandSession() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(500); // limit = 250

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        String expectedSql = String.format("select * from `%s.%s.%s` limit ?", PROJECT_ID, SCHEMA_NAME, TABLE);
        when(commandSession.prepareStatement(expectedSql)).thenReturn(readStatement);
        ArgumentCaptor<Object> limitCaptor = ArgumentCaptor.forClass(Object.class);
        when(readStatement.execute(limitCaptor.capture())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        var result = newSnapshot(false).fetch();

        assertNotNull(result);
        verify(commandSession).prepareStatement(expectedSql);
        assertEquals(250L, ((Number) limitCaptor.getValue()).longValue());
        verify(readSession, never()).statement();
    }

    // --- fetch() branch shape: useStorageApi=true executes via readSession.statement() with INPUT_* params ---

    @Test
    void fetch_storageApi_executesViaReadSessionWithExpectedParams() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(500); // limit = 250

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        when(readSession.statement()).thenReturn(readStatement);
        ArgumentCaptor<Object> paramsCaptor = ArgumentCaptor.forClass(Object.class);
        when(readStatement.execute(paramsCaptor.capture())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        var result = newSnapshot(true).fetch();

        assertNotNull(result);
        verify(readSession).statement();
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
        assertEquals(SCHEMA_NAME, params.get(BigQueryReadIoSession.INPUT_DATASET));
        assertEquals(TABLE, params.get(BigQueryReadIoSession.INPUT_TABLE));
        assertEquals(250L, ((Number) params.get(BigQueryReadIoSession.INPUT_LIMIT)).longValue());
        // the literal-SQL branch must not have been taken
        verify(commandSession, never()).prepareStatement(contains("select * from"));
    }

    // --- BUG HUNT: fetch()'s try/catch swallows exceptions from building/executing the read statement ---

    /**
     * BigQuerySnapshot.java:44-64 wraps the entire "build+execute the snapshot read statement" logic in
     * {@code try { ... } catch (Exception e) { log.error(...); return null; }}. This test proves that when
     * the read statement's execute(...) throws, fetch() swallows the exception and returns null instead of
     * propagating it - even though fetch()'s own signature is `throws Exception` and callers have no way to
     * distinguish "legitimately empty result" from "something failed".
     */
    @Test
    void fetch_readStatementExecuteThrows_swallowsExceptionAndReturnsNullInsteadOfPropagating() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(500);

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        when(commandSession.prepareStatement(contains("select * from"))).thenReturn(readStatement);
        when(readStatement.execute(any())).thenThrow(new RuntimeException("simulated BigQuery read failure"));

        var result = newSnapshot(false).fetch();

        // Confirmed: the informative RuntimeException is logged and discarded; fetch() returns null.
        // A caller doing `for (Map<String,Object> row : someWrapper(fetch()))` or `fetch().hasNext()`
        // would get a NullPointerException far from the real cause, instead of the original exception.
        assertNull(result);
    }

    /**
     * Contrast case: getLimit()/getNumberOfRows() run BEFORE the try block (BigQuerySnapshot.java:40, with
     * the try starting at :44), so a failure there is NOT swallowed - it propagates normally. This is the
     * asymmetry called out in the bug report: identical-looking failures ("can't read from BigQuery") are
     * handled completely differently depending on which side of one line they occur.
     */
    @Test
    void fetch_getNumberOfRowsThrows_propagatesExceptionRatherThanSwallowingIt() throws Exception {
        stubSampleSize(10, 1000, 50);
        IoCommand.Statement countStatement = mock(IoCommand.Statement.class);
        RuntimeException failure = new RuntimeException("simulated row-count failure");
        when(commandSession.prepareStatement(contains("__TABLES__"))).thenReturn(countStatement);
        when(countStatement.execute(any())).thenThrow(failure);

        BigQuerySnapshot snapshot = newSnapshot(false);
        RuntimeException thrown = assertThrows(RuntimeException.class, snapshot::fetch);
        assertEquals(failure, thrown);
    }

    // --- close() ---

    @Test
    void close_withNoPriorFetch_doesNotThrow() {
        assertDoesNotThrow(() -> newSnapshot(false).close());
    }

    @Test
    void close_afterSuccessfulFetch_closesStatementAndResultThenToleratesRepeatedClose() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(500);

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        when(commandSession.prepareStatement(contains("select * from"))).thenReturn(readStatement);
        when(readStatement.execute(any())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        BigQuerySnapshot snapshot = newSnapshot(false);
        snapshot.fetch();

        snapshot.close();
        verify(readStatement, times(1)).close();
        verify(readResult, times(1)).close();

        // second close() must be tolerant of the now-nulled fields (no further close() calls, no exception)
        assertDoesNotThrow(snapshot::close);
        verify(readStatement, times(1)).close();
        verify(readResult, times(1)).close();
    }

    @Test
    void fetch_calledTwice_defensivelyClosesStalePriorStatementAndResultBeforeRebuilding() throws Exception {
        stubSampleSize(10, 1000, 50);
        stubRowCount(500);

        IoCommand.Statement readStatement = mock(IoCommand.Statement.class);
        IoCommand.Result readResult = mock(IoCommand.Result.class);
        when(commandSession.prepareStatement(contains("select * from"))).thenReturn(readStatement);
        when(readStatement.execute(any())).thenReturn(readResult);
        when(readResult.iterator()).thenReturn(Collections.emptyIterator());

        BigQuerySnapshot snapshot = newSnapshot(false);
        snapshot.fetch();
        snapshot.fetch();

        InOrder inOrder = inOrder(commandSession, readStatement);
        inOrder.verify(commandSession).prepareStatement(contains("select * from")); // 1st fetch builds the statement
        inOrder.verify(readStatement).close(); // 2nd fetch defensively closes the stale statement from call #1
        inOrder.verify(commandSession).prepareStatement(contains("select * from")); // 2nd fetch rebuilds it
    }
}
