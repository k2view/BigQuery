package com.k2view.cdbms.usercode.common.BigQuery;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

// The IoProvider is used to handle Big Query Read/Write
public class BigQueryIoProvider implements IoProvider {
    static final String SESSION_PROP_SNAPSHOT_VIA_STORAGE = "snapshotViaStorageApi";
    static final String SESSION_PROP_CREDENTIALS_FILE = "credentialsFilePath";
    static final String SESSION_PROP_PROJECT = "project";
    static final String SESSION_PROP_INTERFACE = "interface";
    static final String SESSION_PROP_AUTHENTICATION_METHOD = "authenticationMethod";

    public enum Operation {
        WRITE,
        READ,
        COMMAND
    }

    public static final Operation DEFAULT_OPERATION = Operation.COMMAND;
    public static final String OPERATION_PARAM_NAME = "operation";

    private final Log log = Log.a(this.getClass());

    @Override
    public IoSession createSession(String ioProviderFunc, Map<String, Object> map)
            throws FileNotFoundException, IOException {
        String interfaceName = (String) map.get(SESSION_PROP_INTERFACE);
        String projectId = ParamConvertor.toString(map.get("ProjectId"));
        String credentialFilePath = ParamConvertor.toString(map.get("OAuthPvtKeyPath"));
        boolean snapshotViaStorageApi = ParamConvertor.toBool(map.get(SESSION_PROP_SNAPSHOT_VIA_STORAGE));
        String authenticationMethod = ParamConvertor.toString(map.get(SESSION_PROP_AUTHENTICATION_METHOD));

        map.putIfAbsent(OPERATION_PARAM_NAME, DEFAULT_OPERATION);
        // Validate that Operation is READ/WRITE/COMMAND
        // Not taking operation as is because of different class loader issue
        Operation operation = Operation.valueOf(map.get(OPERATION_PARAM_NAME).toString());
        // Open READ/WRITE session based on Operation input, and pass the params
        // extracted from Data property set in the interface
        Map<String, Object> sessionProps = Util.map(SESSION_PROP_AUTHENTICATION_METHOD, authenticationMethod,
                SESSION_PROP_INTERFACE, interfaceName, SESSION_PROP_PROJECT,
                projectId, SESSION_PROP_CREDENTIALS_FILE, credentialFilePath, SESSION_PROP_SNAPSHOT_VIA_STORAGE,
                snapshotViaStorageApi);
        if (Operation.READ == operation) {
            return new BigQueryReadIoSession(sessionProps);
        } else if (Operation.WRITE == operation) {
            log.debug("Creating a BQ Write IoSession");
            return new BigQueryWriteIoSession(sessionProps);
        } else if (Operation.COMMAND == operation) {
            log.debug("Creating a BQ Command IoSession");
            return new BigQueryCommandIoSession(sessionProps);
        } else {
            throw new IllegalArgumentException("Unsupported operation");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IoProvider> T unwrap(Class<T> aClass) {
        return (T) this;
    }
}
