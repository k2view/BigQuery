package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.k2view.broadway.actors.builtin.AbstractIoSession;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.tx.TxManager;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

import static com.k2view.fabric.common.io.basic.IoSimpleResultSet.ONE_ROW_AFFECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
 * BigQueryWrite.session/iface are declared `protected`/`private` on the superclass
 * com.k2view.broadway.actors.builtin.AbstractIoSession, which lives in a different package.
 * Since this test class is not itself a subclass of AbstractIoSession, those fields are not
 * directly reachable and are set/read via reflection, exactly like the sibling
 * BigQuerySessionTest does for BigQuerySession's own private fields.
 *
 * Pre-seeding both `session` (protected) and `iface` (private) lets action() take the
 * "reuse existing session" branch inside AbstractIoSession.openSession(...) (the interface
 * name comparison short-circuits to true), so action()/close() can be exercised without ever
 * building a real BigQueryWriteIoSession or touching Google Cloud infrastructure.
 */
class BigQueryWriteTest {

    private static void setSession(BigQueryWrite write, IoSession session) throws Exception {
        java.lang.reflect.Field f = AbstractIoSession.class.getDeclaredField("session");
        f.setAccessible(true);
        f.set(write, session);
    }

    private static void setIface(BigQueryWrite write, String iface) throws Exception {
        java.lang.reflect.Field f = AbstractIoSession.class.getDeclaredField("iface");
        f.setAccessible(true);
        f.set(write, iface);
    }

    private static IoSession getSession(BigQueryWrite write) throws Exception {
        java.lang.reflect.Field f = AbstractIoSession.class.getDeclaredField("session");
        f.setAccessible(true);
        return (IoSession) f.get(write);
    }

    private static Context noOpContext() {
        // A plain mock(IoProvider.class) is deliberately NOT an instance of TxManager, so the
        // TxManager.refreshTx(...) branch inside AbstractIoSession.openSession(...) is skipped.
        Context context = mock(Context.class);
        when(context.ioProvider()).thenReturn(mock(IoProvider.class));
        return context;
    }

    // --- execute ---

    @Test
    void execute_firstCall_createsStatementFromSessionExactlyOnce() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        when(session.statement()).thenReturn(statement);
        setSession(write, session);

        Data input = Data.create();
        input.put("foo", "bar");

        write.execute(input);

        verify(session, times(1)).statement();
        verify(statement, times(1)).batch(input.fields());
    }

    @Test
    void execute_secondCall_reusesCachedStatement_doesNotCallSessionStatementAgain() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        when(session.statement()).thenReturn(statement);
        setSession(write, session);

        Data input1 = Data.create();
        input1.put("row", 1);
        Data input2 = Data.create();
        input2.put("row", 2);

        write.execute(input1);
        write.execute(input2);

        // statement() must be created lazily exactly once, no matter how many times execute() runs.
        verify(session, times(1)).statement();
        verify(statement, times(1)).batch(input1.fields());
        verify(statement, times(1)).batch(input2.fields());
    }

    // --- action ---

    @Test
    void action_setsWriteOperationBeforeSuperAction_andPopulatesAffectedRows() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        when(session.statement()).thenReturn(statement);
        setSession(write, session);
        setIface(write, "iface1");

        Data input = Data.create();
        input.put("interface", "iface1");
        Data output = Data.create();

        write.action(input, output, noOpContext());

        assertEquals(BigQueryIoProvider.Operation.WRITE, input.fields().get(BigQueryIoProvider.OPERATION_PARAM_NAME));
        assertEquals(ONE_ROW_AFFECTED.rowsAffected(), output.fields().get("affected"));
        verify(statement, times(1)).batch(input.fields());
        // Same session/statement instance is kept - openSession() took the "reuse" branch.
        assertEquals(session, getSession(write));
    }

    @Test
    void action_secondCall_stillCachesStatement_andReportsOneRowAffectedEachTime() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        when(session.statement()).thenReturn(statement);
        setSession(write, session);
        setIface(write, "iface1");

        Data input1 = Data.create();
        input1.put("interface", "iface1");
        input1.put("row", 1); // distinguishes this call's args from input2's for the verify() below
        Data output1 = Data.create();
        write.action(input1, output1, noOpContext());

        Data input2 = Data.create();
        input2.put("interface", "iface1");
        input2.put("row", 2);
        Data output2 = Data.create();
        write.action(input2, output2, noOpContext());

        verify(session, times(1)).statement();
        verify(statement, times(1)).batch(input1.fields());
        verify(statement, times(1)).batch(input2.fields());
        assertEquals(1, output1.fields().get("affected"));
        assertEquals(1, output2.fields().get("affected"));
    }

    // --- createSessionParams ---

    @Test
    void createSessionParams_injectsWriteSubIdentifier_onTopOfInputFields() {
        BigQueryWrite write = new BigQueryWrite();
        Data input = Data.create();
        input.put("existing", "value");

        Map<String, Object> params = write.createSessionParams(input);

        assertEquals("_write", params.get(TxManager.SUB_IDENTIFIER));
        assertEquals("value", params.get("existing"));
        // createSessionParams returns the SAME live map backing `input` (Data.fields() is not a
        // copy) - this is what makes the OPERATION_PARAM_NAME asymmetry with BigQueryRead safe;
        // see action_operationParamName_isVisibleToCreateSessionParams_dueToSharedFieldsMap below.
        assertEquals(input.fields(), params);
    }

    @Test
    void action_operationParamName_isVisibleToCreateSessionParams_dueToSharedFieldsMap() {
        // BigQueryWrite.action() sets OPERATION_PARAM_NAME directly on `input` BEFORE calling
        // super.action(...), instead of inside createSessionParams(...) like BigQueryRead does.
        // Data.fields() (verified via the broadway Data/DataImp bytecode) returns the live
        // backing map, not a copy, so by the time createSessionParams(input) runs (invoked by
        // AbstractIoSession.openSession() -> mutableSessionParams(), which wraps the result in
        // `new HashMap<>(createSessionParams(input))`), the operation key set by action() is
        // already present. This test proves the asymmetry is cosmetic, not a bug: the write
        // operation param is never missing from the map handed to IoProvider.createSession(...).
        BigQueryWrite write = new BigQueryWrite();
        Data input = Data.create();
        input.put("interface", "iface1");

        // Simulate exactly what action() does before delegating to super.action(...):
        input.put(BigQueryIoProvider.OPERATION_PARAM_NAME, BigQueryIoProvider.Operation.WRITE);

        Map<String, Object> params = write.createSessionParams(input);

        assertEquals(BigQueryIoProvider.Operation.WRITE, params.get(BigQueryIoProvider.OPERATION_PARAM_NAME));
    }

    // --- close ---

    @Test
    void close_safelyClosesAndNullsStatement_thenClosesSessionViaSuper() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        when(session.statement()).thenReturn(statement);
        setSession(write, session);

        Data input = Data.create();
        input.put("k", "v");
        write.execute(input); // lazily creates the cached statement

        write.close();

        verify(statement, times(1)).close();
        verify(session, times(1)).close();
        // super.close() nulls the session field.
        assertNull(getSession(write));
    }

    @Test
    void close_withoutPriorExecute_doesNotThrow_statementNeverTouched() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        setSession(write, session);

        write.close();

        verify(session, times(1)).close();
        verify(session, never()).statement();
    }

    @Test
    void close_thenExecute_recreatesStatementBecauseCloseNulledIt() throws Exception {
        BigQueryWrite write = new BigQueryWrite();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statementA = mock(IoCommand.Statement.class);
        IoCommand.Statement statementB = mock(IoCommand.Statement.class);
        when(session.statement()).thenReturn(statementA, statementB);
        setSession(write, session);

        Data input1 = Data.create();
        input1.put("k", 1);
        write.execute(input1);
        write.close();

        // Re-seed the session field, mimicking a fresh action()/openSession() cycle after close().
        setSession(write, session);
        Data input2 = Data.create();
        input2.put("k", 2);
        write.execute(input2);

        verify(session, times(2)).statement();
        verify(statementA, times(1)).batch(input1.fields());
        verify(statementB, times(1)).batch(input2.fields());
    }
}
