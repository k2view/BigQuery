package com.k2view.cdbms.usercode.common.BigQuery;

import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.getJavaTypeFromBQType;
import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.parseBqValue;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Builder;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.basic.IoSimpleRow;

public class BigQueryCommandIoSession extends BigQuerySession {
    private final Log log = Log.a(this.getClass());

    public BigQueryCommandIoSession(Map<String, Object> props) {
        super(props);
    }

    @Override
    public void close() {
    }

    @Override
    public void abort() {
        this.close();
    }

    @Override
    public Statement prepareStatement(String command) {
        return new BigQueryCommandStatement(command);
    }

    @Override
    public IoSessionCompartment compartment() {
        return IoSessionCompartment.SHARED;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(Map<String, Object> params) throws Exception {
        return (T) new BigQueryMetadata(interfaceName, this, null, client(), datasetsProjectId, snapshotViaStorageApi,
                params);
    }

    public class BigQueryCommandStatement implements Statement {
        private final String command;
        private final Builder queryJobBuilder;

        public BigQueryCommandStatement(String command) {
            this.command = replaceProjectId(command, datasetsProjectId);
            this.queryJobBuilder = QueryJobConfiguration.newBuilder(this.command);
        }

        private static String replaceProjectId(String sql, String projectId) {
            StringBuilder result = new StringBuilder();
            boolean inString = false;
            int length = sql.length();

            for (int i = 0; i < length;) {
                char c = sql.charAt(i);

                if (!inString && sql.startsWith("$projectId.", i)) {
                    result.append(projectId).append(".");
                    i += "$projectId.".length();
                } else {
                    if (c == '\'') {
                        inString = !inString;
                    } 
                    result.append(c);
                    i++;
                }
            }

            return result.toString();
        }

        @Override
        public Result execute(Object... params) throws Exception {
            log.debug("Executing command with sql={}", command);

            Iterable<QueryParameterValue> values = params == null || params.length == 0 ? null
                    : Arrays.asList(params).stream().map(BigQueryParamParser::parseToBqParam).toList();
            queryJobBuilder.setPositionalParameters(values);

            // Create the query job configuration
            QueryJobConfiguration queryConfig = queryJobBuilder.build();

            // Create the query job and wait for it to finish
            JobId jobId = JobId.newBuilder().setProject(userProjectId).build();
            Job queryJob = client().create(
                    JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            long numDmlAffectedRows = -1;
            queryJob = queryJob.waitFor();

            if (queryJob.getStatistics() instanceof QueryStatistics qs && qs.getNumDmlAffectedRows() != null) {
                numDmlAffectedRows = qs.getNumDmlAffectedRows();
            }

            BigQueryError error = queryJob.getStatus().getError();
            if (queryJob.isDone() && error == null) {
                log.debug("Command executed successfully with sql={}", command);

                // Save the query results
                TableResult queryResults = queryJob.getQueryResults();
                // Return the result
                return new BigQueryCommandResult(queryResults, numDmlAffectedRows);
            } else {
                throw new RuntimeException(
                        error != null ? error.getMessage() : String.format("Failed to execute sql='%s'", command));
            }
        }

        private class BigQueryCommandResult implements Result {
            private final Iterator<FieldValueList> iterator;
            private final TableResult queryResult;
            private final FieldList schemaFields;
            private final long numDmlAffectedRows;

            public BigQueryCommandResult(TableResult queryResult, long numDmlAffectedRows) {
                this.queryResult = queryResult;
                this.iterator = queryResult.iterateAll().iterator();
                this.schemaFields = queryResult.getSchema() != null ? queryResult.getSchema().getFields() : null;
                this.numDmlAffectedRows = numDmlAffectedRows;
            }

            @Override
            public Iterator<Row> iterator() {
                if (queryResult.getSchema() == null) {
                    // DDL or DML
                    return Collections.emptyIterator();
                }
                return new Iterator<Row>() {
                    private final Function<Object[], Row> simpleRowFactory = IoSimpleRow
                            .factory(schemaFields.stream().map(Field::getName).toList());

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
                                log.error("Unable to parse BigQuery value '{}' for field '{}'", fieldValue,
                                        field.getName(), e);
                                values = new Object[] {};
                                break;
                            }
                        }
                        return simpleRowFactory.apply(values);
                    }
                };
            }

            @Override
            public String[] labels() {
                return schemaFields != null ? schemaFields.stream().map(Field::getName).toArray(String[]::new)
                        : new String[0];
            }

            @Override
            public Type[] types() {
                return schemaFields != null ? schemaFields.stream()
                        .map(field -> getJavaTypeFromBQType(field.getType().getStandardType()))
                        .toArray(Type[]::new) : new Type[0];
            }

            @Override
            public int rowsAffected() {
                return (int) numDmlAffectedRows;
            }
        }

    }
}
