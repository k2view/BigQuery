package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.k2view.broadway.exception.AbortException;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;

@ThreadSafe
class BigQuerySession extends AbstractIoSession {
    private static final String DEFAULT_AUTH_METHOD = "default";

    private final boolean defaultAuthenticationMethod;
    private final String credentialsFilePath;
    final String interfaceName;
    final String projectId;
    final String queryJobsProjectId;
    final boolean snapshotViaStorageApi;
    
    private volatile BigQuery bqClient; // Ensures visibility across threads
    private final Object lock = new Object(); // Synchronization lock

    protected volatile boolean aborted;

    BigQuerySession(Map<String, Object> props) {
        this.projectId = (String) props.get(BigQueryIoProvider.SESSION_PROP_PROJECT);
        this.credentialsFilePath = (String) props.get(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_FILE);
        this.interfaceName = (String) props.get(BigQueryIoProvider.SESSION_PROP_INTERFACE);
        this.snapshotViaStorageApi = ParamConvertor
                .toBool(props.get(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE));
        this.defaultAuthenticationMethod = DEFAULT_AUTH_METHOD
                .equalsIgnoreCase((String) props.get(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD));
        this.queryJobsProjectId = (String) props.get(BigQueryIoProvider.SESSION_PROP_JOB_PROJECT);
    }

    BigQuery client() throws Exception {
        if (bqClient == null) { // First check (without locking)
            synchronized (lock) {
                if (bqClient == null) { // Second check (within lock)
                    bqClient = BigQueryOptions.newBuilder()
                            .setCredentials(credentials())
                            .setProjectId(projectId)
                            .build()
                            .getService();
                }
            }
        }
        return bqClient;
    }

    GoogleCredentials credentials() throws IOException {
        if (!defaultAuthenticationMethod) {
            try (FileInputStream credentialsStream = new FileInputStream(credentialsFilePath)) {
                return GoogleCredentials.fromStream(credentialsStream);
            }
        }
        return GoogleCredentials.getApplicationDefault();
    }

    @Override
    public void testConnection() {
        Util.rte(() -> client().listDatasets());
    }

    @Override
    public void abort() throws Exception {
        this.aborted = true;
    }
}
