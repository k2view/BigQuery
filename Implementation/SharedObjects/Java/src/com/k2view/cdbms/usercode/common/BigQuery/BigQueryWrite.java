package com.k2view.cdbms.usercode.common.BigQuery;

import com.k2view.broadway.actors.builtin.AbstractIoSession;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.tx.TxManager;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;

import java.util.Map;

import static com.k2view.fabric.common.io.basic.IoSimpleResultSet.ONE_ROW_AFFECTED;

public class BigQueryWrite extends AbstractIoSession {
    private final Log log = Log.a(this.getClass());
    private IoCommand.Statement statement;

    /*
     Used in super.openSession(String, Context, Data).
     Need to override in order to access the operation type (write/read/command)
     in BigQueryWriteIoProvider.
    */
    @Override
    protected Map<String, Object> createSessionParams(Data input) {
        Map<String, Object> fields = input.fields();
        fields.put(TxManager.SUB_IDENTIFIER, "_write");
        return fields;
    }

    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        // The BigQueryIoProvider must know which type of session
        // to create.
        input.put(BigQueryIoProvider.OPERATION_PARAM_NAME, BigQueryIoProvider.Operation.WRITE);

        // Create the session if needed (in the first run)
        super.action(input, output, context);

        this.execute(input);

        // Return 1 row affected
        output.put("affected", ONE_ROW_AFFECTED.rowsAffected());
    }

    protected void execute(Data input) throws Exception {
        // Define the statement in the first run
        if (statement == null) {
            statement = this.session.statement();
        }

        statement.batch(input.fields());
    }

    @Override
    public void close() {
        // Close all resources
        log.debug("CLOSING ACTOR {}", this);
        log.debug("SESSION={}", session != null ? session.unwrap() : null);
        log.debug("STATEMENT={}", statement);
        Util.safeClose(statement);
        statement=null;
        super.close();
    }
}
