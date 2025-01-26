package com.k2view.cdbms.usercode.common.BigQuery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.QueryJobConfiguration.Builder;
import com.k2view.cdbms.usercode.common.BigQuery.BigQueryMetadata;
import com.k2view.fabric.common.*;
import com.k2view.fabric.common.io.AbstractIoSession;
import com.k2view.fabric.common.io.basic.IoSimpleRow;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.*;

public class BigQueryCommandIoSession extends AbstractIoSession {
    private final Log log = Log.a(this.getClass());
    private final AtomicReference<String> credentialsFilePath = new AtomicReference<>();
    private final String interfaceName;
    private final String projectId;
    final BigQuery bigquery;
    final boolean snapshotViaStorageApi;

    public BigQueryCommandIoSession(String interfaceName, String credentialsFilePath, String projectId, boolean snapshotViaStorageApi) {
        this.interfaceName = interfaceName;
        this.credentialsFilePath.set(credentialsFilePath);
        this.projectId = projectId;
        this.snapshotViaStorageApi = snapshotViaStorageApi;

        try (FileInputStream credentialsStream = new FileInputStream(credentialsFilePath)) { 
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(credentialsStream);
            this.bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
        } catch (Exception e) {
            log.error("Failed to initialize BigQuery client", e);
            throw new RuntimeException("Failed to initialize BigQuery client", e);
        }
    }

    @Override
    public void close() {}

    @Override
    public void abort() {
        this.close();
    }

    @Override
    public Statement prepareStatement(String command) {
        return new BigQueryCommandStatement(command);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(Map<String, Object> params) throws Exception {
        return (T) new BigQueryMetadata(credentialsFilePath.get(), interfaceName, this, null, projectId, params);
    }

    public class BigQueryCommandStatement implements Statement {
        private final String command;
        private final Builder queryJobBuilder;

        public BigQueryCommandStatement(String command) {
            this.command = command;
            this.queryJobBuilder = QueryJobConfiguration.newBuilder(command);
        }

        @Override
        public Result execute(Object... params) throws Exception {
            log.debug("Executing command with sql={}", command);

            Iterable<QueryParameterValue> values = params == null || params.length == 0 ? null : Arrays.asList(params).stream().map(BigQueryParamParser::parseToBqParam).toList();
            queryJobBuilder.setPositionalParameters(values);

            // Create the query job configuration
            QueryJobConfiguration queryConfig = queryJobBuilder.build();

            // Create the query job and wait for it to finish
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
            queryJob = queryJob.waitFor();

            BigQueryError error = queryJob.getStatus().getError();
            if (queryJob.isDone() && error == null) {
                log.debug("Command executed successfully with sql={}", command);

                // Save the query results
                TableResult queryResults = queryJob.getQueryResults();
                // Return the result
                return new BigQueryCommandResult(queryResults);
            } else {
                throw new RuntimeException(error != null ? error.getMessage() : String.format("Failed to execute sql='%s'", command));
            }
        }

        private class BigQueryCommandResult implements Result {
            private final Iterator<FieldValueList> iterator;
            private final TableResult queryResult;
            private final FieldList schemaFields; 

            public BigQueryCommandResult(TableResult queryResult) {
                this.queryResult = queryResult;
                this.iterator = queryResult.iterateAll().iterator();
                this.schemaFields = queryResult.getSchema() != null ? queryResult.getSchema().getFields() : null;
            }
        
            @NotNull
            @Override
            public Iterator<Row> iterator() {
                if (queryResult.getSchema() == null) {
                    // DDL or DML
                    return Collections.emptyIterator();
                }
                return new Iterator<Row>() {
                    private final Function<Object[], Row> simpleRowFactory = IoSimpleRow.factory(schemaFields.stream().map(Field::getName).toList());
        
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
        
                    @Override
                    public Row next() {
                        FieldValueList record = iterator.next();
                        Object[] values = new Object[schemaFields.size()];
                        int fieldValIndex = 0;
        
                        for (FieldValue fieldValue : record) {
                            Field field = schemaFields.get(fieldValIndex);
                            try {
                                values[fieldValIndex++] = parseBqValue(field, fieldValue, false);
                            } catch (Exception e) {
                                log.error("Unable to parse BigQuery value '{}'' for field '{}'", fieldValue, field.getName(), e);
                                return null;
                            }
                        }
                        return simpleRowFactory.apply(values);
                    }
                };
            }
        
            @Override
            public String[] labels() {
                return schemaFields != null ? schemaFields.stream().map(Field::getName).toArray(String[]::new) : new String[0];
            }
        
            @Override
            public Type[] types() {
                return schemaFields != null ? schemaFields.stream()
                        .map(field -> getJavaTypeFromBQType(field.getType().getStandardType()))
                        .toArray(Type[]::new) : new Type[0];
            }
        
            @Override
            public int rowsAffected() {
                return -1; // Not applicable for queries
            }
        }
        

    }
}
