package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;

@ThreadSafe
class BigQuerySession extends AbstractIoSession {
    private static enum AuthMethod {
        DEFAULT,
        FILE,
        JSON
    }

    private final AuthMethod authMethod;
    private final String credentialsFilePath;
    private final String credentialsJSON;
    final String interfaceName;
    final String userProjectId;
    final String datasetsProjectId;
    final boolean snapshotViaStorageApi;

    private volatile BigQuery bqClient; // Ensures visibility across threads
    private final Object lock = new Object(); // Synchronization lock

    protected volatile boolean aborted;

    BigQuerySession(Map<String, Object> props) {
        this.userProjectId = (String) props.get(BigQueryIoProvider.SESSION_PROP_USER_PROJECT);
        this.credentialsFilePath = (String) props.get(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_FILE);
        this.credentialsJSON = (String) props.get(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON);
        this.interfaceName = (String) props.get(BigQueryIoProvider.SESSION_PROP_INTERFACE);
        this.snapshotViaStorageApi = ParamConvertor
                .toBool(props.get(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE));
        this.datasetsProjectId = (String) props.get(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT);
        this.authMethod = AuthMethod.valueOf(props.get(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD).toString().toUpperCase());
    }

    @Override
    public void testConnection() {
        Util.rte(() -> client().listDatasets());
    }

    @Override
    public void abort() throws Exception {
        this.aborted = true;
    }

    BigQuery client() throws Exception {
        if (bqClient == null) { // First check (without locking)
            synchronized (lock) {
                if (bqClient == null) { // Second check (within lock)
                    bqClient = BigQueryOptions.newBuilder()
                            .setCredentials(credentials())
                            .setProjectId(userProjectId)
                            .build()
                            .getService();
                }
            }
        }
        return bqClient;
    }

    GoogleCredentials credentials() throws IOException {
        switch (authMethod) {
            case FILE:
                try (FileInputStream credentialsStream = new FileInputStream(credentialsFilePath)) {
                    return GoogleCredentials.fromStream(credentialsStream);
                }
            case JSON:
                return GoogleCredentials
                        .fromStream(new ByteArrayInputStream(credentialsJSON.getBytes(StandardCharsets.UTF_8)));
            default:
                return GoogleCredentials.getApplicationDefault();
        }
    }
}
