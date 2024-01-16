package com.k2view.cdbms.usercode.common.BigQuery;

import org.json.JSONArray;

public interface WriteStream extends AutoCloseable {
    void write(String dataset, String table, JSONArray rows) throws Exception;
    default void abort() throws Exception {
        this.close();
    }
}
