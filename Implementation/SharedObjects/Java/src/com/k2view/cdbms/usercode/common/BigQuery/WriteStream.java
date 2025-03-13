package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.IOException;

import org.json.JSONArray;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.k2view.fabric.common.Log;

public interface WriteStream extends AutoCloseable {
    static final Log log = Log.a(WriteStream.class);

    void write(String dataset, String table, JSONArray rows) throws Exception;

    default void abort() throws Exception {
        this.close();
    }

    public static WriteStream createWriteStream(Type type, String projectId, Credentials credentials)
            throws IOException {
        switch (type) {
            case PENDING:
                log.debug("Creating a stream of type PENDING");
                return new PendingWriteStream(projectId, credentials);
            case BUFFERED:
            case COMMITTED:
                throw new IllegalArgumentException("Unimplemented type of stream " + type + ".");
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognized type of stream.");
            case TYPE_UNSPECIFIED:
            default:
                log.debug("Creating a default stream");
                return new DefaultWriteStream(projectId, credentials);
        }
    }

    public static WriteStream createWriteStream(String projectId, Credentials credentials) throws Exception {
        return createWriteStream(Type.TYPE_UNSPECIFIED, projectId, credentials);
    }
}
