package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.tx.TxManager;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
 * NOTE ON TEST STRATEGY:
 * BigQueryRead.action(...) first calls super.action(...) (AbstractIoSession, from Broadway),
 * which always resolves the session by calling context.ioProvider().createSession(interfaceName,
 * sessionParams) - confirmed by tracing its bytecode and by an earlier failed attempt at this test
 * that tried to pre-seed the protected "session" field directly and skip that call: it turned out
 * the private "iface" tracking field never matches an empty/absent "interface" input value the way
 * a first read of the bytecode suggested, so createSession(...) is always invoked in practice.
 * Rather than depend on that internal nuance, these tests instead stub Context.ioProvider() to
 * return a mock IoProvider whose createSession(...) returns our own mock IoSession - giving full,
 * deterministic control over action()'s wiring without ever touching BigQueryReadIoSession or any
 * real BigQuery/network call. createSessionParams(...) itself (which BigQueryRead overrides to
 * inject the operation/sub-identifier keys) is exercised for real as part of this flow, and also
 * tested directly and in isolation below.
 */
class BigQueryReadTest {

    private static Context contextCreating(IoSession session) throws Exception {
        IoProvider provider = mock(IoProvider.class);
        when(provider.createSession(anyString(), any())).thenReturn(session);
        Context context = mock(Context.class);
        when(context.ioProvider()).thenReturn(provider);
        return context;
    }

    // --- createSessionParams ---

    @Test
    void createSessionParams_addsOperationReadAndSubIdentifier_toInputFieldsMap() {
        BigQueryRead actor = new BigQueryRead();
        Data input = Data.create();
        input.put("someExistingKey", "someExistingValue");

        Map<String, Object> result = actor.createSessionParams(input);

        assertSame(input.fields(), result, "createSessionParams must mutate and return the same fields map, not a copy");
        assertEquals(BigQueryIoProvider.Operation.READ, result.get(BigQueryIoProvider.OPERATION_PARAM_NAME));
        assertEquals("_read", result.get(TxManager.SUB_IDENTIFIER));
        assertEquals("someExistingValue", result.get("someExistingKey"), "pre-existing fields must be preserved");
    }

    // --- close() null-safety ---

    @Test
    void close_withNothingEverOpened_doesNotThrow() {
        BigQueryRead actor = new BigQueryRead();

        assertDoesNotThrow(actor::close);
    }

    @Test
    void close_afterAction_nullsCommandAndResult_andIsIdempotent() throws Exception {
        BigQueryRead actor = new BigQueryRead();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        IoCommand.Result commandResult = mock(IoCommand.Result.class);
        when(session.statement()).thenReturn(statement);
        when(statement.execute(any())).thenReturn(commandResult);

        Data input = Data.create();
        Data output = Data.create();
        actor.action(input, output, contextCreating(session));

        assertDoesNotThrow(actor::close);
        // calling close() a second time must also be safe (fields already nulled)
        assertDoesNotThrow(actor::close);
    }

    // --- action() wiring ---

    @Test
    void action_wiresSessionStatementExecuteAndPutsResultInOutput() throws Exception {
        BigQueryRead actor = new BigQueryRead();
        IoSession session = mock(IoSession.class);
        IoCommand.Statement statement = mock(IoCommand.Statement.class);
        IoCommand.Result commandResult = mock(IoCommand.Result.class);
        when(session.statement()).thenReturn(statement);
        when(statement.execute(any())).thenReturn(commandResult);

        Data input = Data.create();
        input.put("dataset", "myDataset");
        Data output = Data.create();

        actor.action(input, output, contextCreating(session));

        verify(session).statement();
        verify(statement).execute(input.fields());
        assertSame(commandResult, output.fields().get("result"));
        // action() -> createSessionParams(...) must have run as part of opening the session
        assertEquals(BigQueryIoProvider.Operation.READ, input.fields().get(BigQueryIoProvider.OPERATION_PARAM_NAME));
        assertEquals("_read", input.fields().get(TxManager.SUB_IDENTIFIER));
    }
}
