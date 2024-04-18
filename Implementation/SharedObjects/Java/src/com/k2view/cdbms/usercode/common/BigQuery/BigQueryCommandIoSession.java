package com.k2view.cdbms.usercode.common.BigQuery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.k2view.cdbms.usercode.common.BigQuery.metadata.BigQueryMetadata;
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

import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.*;

@SuppressWarnings("all")
public class BigQueryCommandIoSession extends AbstractIoSession {
	private final Log log = Log.a(this.getClass());
	private final AtomicReference<String> credentialsFilePath = new AtomicReference<>();
	private final String interfaceName;
	private final String projectId;

	public BigQueryCommandIoSession(String interfaceName, String credentialsFilePath, String projectId) {
		this.interfaceName = interfaceName;
		this.credentialsFilePath.set(credentialsFilePath);
		this.projectId = projectId;
	}

	@Override
	public void close() {}

	@Override
	public void abort() {
		this.close();
	}

	@Override
	public Statement statement() {
		return new BigQueryCommandStatement();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getMetadata(Map<String, Object> params) {
		return (T) new BigQueryMetadata(credentialsFilePath.get(), interfaceName, this, null, projectId, params);
	}

	public class BigQueryCommandStatement implements Statement {
		private FieldList schemaFields;
		private Iterable<FieldValueList> queryResults;

		/*
		 Should be called with an argument of type Data.
		 Expected inputs in objects[0] (Data arg):
			 interface (Mandatory, string),
			 statement (Mandatory, string),
			 BigQueryIoProvider.OPERATION_PARAM_NAME=BigQueryIoProvider.Operation.COMMAND
		 */

		@Override
		public Result execute(Object... params) throws Exception {
			if (params.length < 1) {
				throw new IllegalArgumentException("A Statement needs at least one parameter with the command syntax");
			}
			String command = String.valueOf(params[0]);
			log.debug("Executing command with sql={}", command);

			Path path = Paths.get(credentialsFilePath.get());
			log.debug("path={}", path);

			ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsFilePath.get()));

			BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

			QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(command);
			for (int i = 1; i < params.length; i++) {
				builder.addPositionalParameter(parseToBqParam(params[i]));
			}

			// Create the query job configuration
			QueryJobConfiguration queryConfig = builder.build();

			// Create a job ID so that we can safely retry.
			JobId jobId = JobId.of(UUID.randomUUID().toString());

			// Create the query job and wait for it to finish
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
			queryJob = queryJob.waitFor();

			BigQueryError error = queryJob.getStatus().getError();
			if (queryJob.isDone() && error == null) {
				log.debug("Command executed successfully with sql={}", command);

				// save the query results
				this.queryResults = queryJob.getQueryResults().getValues();
				this.schemaFields = queryJob.getQueryResults().getSchema() != null ? queryJob.getQueryResults().getSchema().getFields() : null;
				// return the result
				return this.schemaFields != null ? new BigQueryCommandResult() : new Result(){};
			} else {
				log.error("BigQuery was unable to execute sql={}: {}", command, error);
				throw new RuntimeException(error != null ? error.getMessage() : "");
			}
		}

		@Override
		public void close() {
			// clean up resources
			schemaFields = null;
			queryResults = null;
		}

		private class BigQueryCommandResult implements Result {
			@NotNull
			@Override
			public Iterator<Row> iterator() {
				Iterator<FieldValueList> backingIterator = queryResults.iterator();
				// Build the IoSimpleRow keys map from schemaFields
				Map<String, Integer> keys = new LinkedHashMap<>();
				for (int i = 0; i < schemaFields.size(); i++) {
					keys.put(schemaFields.get(i).getName(), i);
				}

				// Translate the iterator from Iterator<FieldValueList> to Iterator<IoSimpleRow>
				return new IteratorTranslate<>(backingIterator, rec -> {
					Object[] values = new Object[keys.size()];
					int fieldValIndex = 0;
					for (FieldValue fieldValue : rec) {
						Field field = schemaFields.get(fieldValIndex);
						try {
							values[fieldValIndex++] = parseBqValue(field, fieldValue, false);
						} catch (Exception e) {
							log.error("Unable to parse BigQuery value {}={}", field.getName(), fieldValue);
							log.error(e);
							return null;
						}
					}
					return new IoSimpleRow(values, keys);
				});
			}

			@Override
			public String[] labels() {
				return schemaFields.stream().map(Field::getName).toArray(String[]::new);
			}

			@Override
			public Type[] types() {
				return schemaFields.stream().map(field -> getJavaTypeFromBQType(field.getType().getStandardType())).toArray(Type[]::new);
			}

			@Override
			public int rowsAffected() {
				// TO-DO Find a way to calc num of effected rows
				return -1;
			}
		}
	}

}