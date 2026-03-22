package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;

@ThreadSafe
class BigQuerySession extends AbstractIoSession {
    private enum AuthMethod {
        DEFAULT,
        FILE,
        JSON
    }

    private record ClientKey(AuthMethod authMethod, String userProjectId, String credentialsId) {}
    private static final ConcurrentHashMap<ClientKey, BigQuery> CLIENT_CACHE = new ConcurrentHashMap<>();

    private final Log log = Log.a(this.getClass());

    private final AuthMethod authMethod;
    private final String credentialsJSON; // FILE auth: file contents read at construction; JSON auth: raw JSON string
    final String interfaceName;
    final String userProjectId;
    final String datasetsProjectId;
    final boolean snapshotViaStorageApi;

    private BigQuery bqClient;

    protected volatile boolean aborted;

    BigQuerySession(Map<String, Object> props) {
        this.userProjectId = (String) props.get(BigQueryIoProvider.SESSION_PROP_USER_PROJECT);
        this.interfaceName = (String) props.get(BigQueryIoProvider.SESSION_PROP_INTERFACE);
        this.authMethod = AuthMethod.valueOf(props.get(BigQueryIoProvider.SESSION_PROP_AUTHENTICATION_METHOD).toString().toUpperCase());
        String filePath = (String) props.get(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_FILE);
        this.credentialsJSON = authMethod == AuthMethod.FILE
                ? Util.rte(() -> Files.readString(Path.of(filePath)))
                : (String) props.get(BigQueryIoProvider.SESSION_PROP_CREDENTIALS_JSON);
        this.snapshotViaStorageApi = ParamConvertor
                .toBool(props.get(BigQueryIoProvider.SESSION_PROP_SNAPSHOT_VIA_STORAGE));
        this.datasetsProjectId = (String) props.get(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT);
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
        if (bqClient == null) {
            bqClient = CLIENT_CACHE.computeIfAbsent(clientKey(), k -> buildClient());
            log.debug("BigQuery client resolved for interface={}, authMethod={}, cacheSize={}", interfaceName, authMethod, CLIENT_CACHE.size());
        }
        return bqClient;
    }

    GoogleCredentials credentials() throws IOException {
        if (authMethod == AuthMethod.DEFAULT) {
            return GoogleCredentials.getApplicationDefault();
        }
        return GoogleCredentials.fromStream(new ByteArrayInputStream(credentialsJSON.getBytes(StandardCharsets.UTF_8)));
    }

    private ClientKey clientKey() {
        String credId = authMethod == AuthMethod.DEFAULT ? "" : credentialsJSON;
        return new ClientKey(authMethod, userProjectId, credId);
    }

    private BigQuery buildClient() {
        log.debug("Building new BigQuery client (cache miss): interface={}, authMethod={}, userProjectId={}", interfaceName, authMethod, userProjectId);
        return Util.rte(
            () -> BigQueryOptions.newBuilder()
                    .setCredentials(credentials())
                    .setProjectId(userProjectId)
                    .build()
                    .getService()
        );
    }
}
