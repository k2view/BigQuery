package com.k2view.cdbms.usercode.common.BigQuery;

import com.k2view.broadway.actors.builtin.AbstractIoSession;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.tx.TxManager;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;

import java.util.Map;

public class BigQueryRead extends AbstractIoSession {
    private IoCommand.Result result;
    private IoCommand.Statement command;

    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        super.action(input, output, context);
        this.result = this.execute(input);
        // Put the iterator rows
        output.put("result", this.result);
    }

    /*
     Used in super.openSession(String, Context, Data).
     Need to override in order to access the operation type (write/read) in the IoProvider
    */
    @Override
    protected Map<String, Object> createSessionParams(Data input) {
        // The BigQueryIoProvider must know which type of session
        // to create - read or write
        Map<String, Object> fields = input.fields();
        fields.put(BigQueryIoProvider.OPERATION_PARAM_NAME, BigQueryIoProvider.Operation.READ);
        fields.put(TxManager.SUB_IDENTIFIER, "_read");
        return fields;
    }

    private IoCommand.Result execute(Data input) throws Exception {
        // Define the statement and execute the statement "execute" function
        this.command = this.session.statement();
        return this.command.execute(input.fields());
    }

    @Override
    public void close() {
        // Close all resources
        Util.safeClose(this.result);
        this.result = null;
        Util.safeClose(this.command);
        this.command = null;
        super.close();
    }
}


