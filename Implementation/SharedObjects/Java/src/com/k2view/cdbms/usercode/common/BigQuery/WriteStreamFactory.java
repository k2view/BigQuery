package com.k2view.cdbms.usercode.common.BigQuery;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.k2view.fabric.common.Log;

import java.io.IOException;

public class WriteStreamFactory {
    private final Log log = Log.a(this.getClass());
    public WriteStream createWriteStream(Type type, String projectId, String credentialFilePath) throws IOException {
        switch (type) {
            case PENDING:
                log.debug("Creating a stream of type PENDING");
                return new PendingWriteStream(projectId, credentialFilePath);
            case BUFFERED:
            case COMMITTED:
                throw new IllegalArgumentException("Unimplemented type of stream " + type + ".");
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognized type of stream.");
            case TYPE_UNSPECIFIED:
            default:
                log.debug("Creating a default stream");
                return new DefaultWriteStream(projectId, credentialFilePath);
        }
    }
    public WriteStream createWriteStream(String projectId, String credentialFilePath) throws Exception {
        return createWriteStream(Type.TYPE_UNSPECIFIED, projectId, credentialFilePath);
    }
}
