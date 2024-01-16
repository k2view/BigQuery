package com.k2view.cdbms.usercode.common.BigQuery;

import com.k2view.broadway.actors.builtin.DbCommand;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.tx.TxManager;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.basic.IoSimpleResultSet;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;


public class BigQueryCommand extends DbCommand {
    private final Log log = Log.a(this.getClass());
    private IoCommand.Statement command;
    private String prevSql;

    /*
    Used in super.openSession(String, Context, Data).
    Need to override in order to access the operation type (write/read/command)
    in BigQueryWriteIoProvider.
    */
    @Override
    protected Map<String, Object> createSessionParams(Data input) {
        Map<String, Object> fields = input.fields();
        fields.put(TxManager.SUB_IDENTIFIER, "_command");
        return fields;
    }

    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        // The BigQueryIoProvider must know which type of session
        // to create.
        input.put(BigQueryIoProvider.OPERATION_PARAM_NAME, BigQueryIoProvider.Operation.COMMAND);
        super.action(input, output, context);
    }

    @Override
    protected IoCommand.Result execute(boolean batch, String sql, Object... params) throws Exception {
        try {
            if (this.command == null || !Util.eq(this.prevSql, sql)) {
                Util.safeClose(this.command);
                this.command = this.session.statement();
                this.prevSql = sql;
            }

            params = Stream.concat(Arrays.stream(new Object[]{sql}), Arrays.stream(params)).toArray(Object[]::new);

            if (batch) {
                this.command.batch(params);
                return new IoSimpleResultSet(1);
            } else {
                return this.command.execute(params);
            }
        } catch (Exception e) {
            throw new BigQueryCommandSqlException(e, sql);
        }
    }

    @Override
    public void close() {
        // Close all resources
        log.debug("CLOSING ACTOR {}", this);
        log.debug("SESSION={}", session != null ? session.unwrap() : null);
        log.debug("STATEMENT={}", command);
        Util.safeClose(command);
        command=null;
        prevSql=null;
        super.close();
    }

    public static class BigQueryCommandSqlException extends SQLException {
        private final String sql;

        public BigQueryCommandSqlException(SQLException e, String sql) {
            super(reason(e, sql), e.getSQLState(), e.getErrorCode(), e);
            this.sql = sql;
        }

        public BigQueryCommandSqlException(Exception e, String sql) {
            super(reason(e, sql), e);
            this.sql = sql;
        }

        private static String reason(Exception e, String sql) {
            String message = Util.def(e.getMessage(), e.toString());
            return message + "\n" + sql;
        }

        public String getSql() {
            return this.sql;
        }
    }
}