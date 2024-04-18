package com.k2view.cdbms.usercode.common.BigQuery;

import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

import java.util.Map;

// The IoProvider is used to handle Big Query Read/Write
public class BigQueryIoProvider implements IoProvider {
    enum Operation {
        WRITE,
        READ,
        COMMAND
    }

    public static final Operation DEFAULT_OPERATION = Operation.COMMAND;
    public static final String OPERATION_PARAM_NAME = "operation";

    private final Log log = Log.a(this.getClass());
    @Override
    public IoSession createSession(String interfaceName, Map<String, Object> map) {
        // Get Data property set in the interface from input map
        String projectId = ParamConvertor.toString(map.get("ProjectId"));
        String credentialFilePath = ParamConvertor.toString(map.get("OAuthPvtKeyPath"));;
        map.putIfAbsent(OPERATION_PARAM_NAME, DEFAULT_OPERATION);
        // Validate that Operation is READ/WRITE/COMMAND
        if (!(map.get(OPERATION_PARAM_NAME) instanceof Operation)) {
                throw new IllegalArgumentException(String.format("'%s' param must be of type %s", OPERATION_PARAM_NAME, Operation.class.getName()));
        }
        Operation operation = (Operation) map.get(OPERATION_PARAM_NAME);
        // Open READ/WRITE session based on Operation input, and pass the params extracted from Data property set in the interface
        if(Operation.READ == operation) {
            return new BigQueryReadIoSession(interfaceName, projectId,credentialFilePath);
        } else if(Operation.WRITE == operation){
            log.debug("Creating a BQ Write IoSession");
            return new BigQueryWriteIoSession(interfaceName, projectId, credentialFilePath);
        } else if (Operation.COMMAND == operation) {
            log.debug("Creating a BQ Command IoSession");
            return new BigQueryCommandIoSession(interfaceName, credentialFilePath, projectId);
        }
        else {
            throw new IllegalArgumentException("Unsupported operation");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IoProvider> T unwrap(Class<T> aClass) {
        return (T) this;
    }
}
