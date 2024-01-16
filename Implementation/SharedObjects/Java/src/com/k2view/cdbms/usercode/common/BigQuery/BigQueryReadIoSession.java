package com.k2view.cdbms.usercode.common.BigQuery;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import com.k2view.cdbms.usercode.common.BigQuery.metadata.BigQueryMetadata;
import com.k2view.fabric.common.IteratorTranslate;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.basic.IoSimpleRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.parseAvroValue;

public class BigQueryReadIoSession extends AbstractIoSession{
	public static final String INPUT_PROJECT_ID = "projectId";
	public static final String INPUT_CREDENTIALS_PATH = "credentialsPath";
	public static final String INPUT_DATASET = "dataset";
	public static final String INPUT_TABLE = "table";
	public static final String INPUT_FILTERS = "filter";
	public static final String INPUT_FIELDS = "fields";
	public static final String INPUT_LIMIT = "limit";
	public static final String INPUT_OPERATION = BigQueryIoProvider.OPERATION_PARAM_NAME;


	private final String projectId;
	private final String credentialsFilePath;
	private final String interfaceName;

	private IoCommand.Statement statement;

	public BigQueryReadIoSession(String interfaceName, String projectId, String credentialsFilePath) {
		this.projectId = projectId;
		this.credentialsFilePath = credentialsFilePath;
		// Initialize IoCommand statement
		this.statement = new BigQueryReadStatement();
		this.interfaceName=interfaceName;
	}

	@Override
	public IoCommand.Statement statement() {
		return this.statement;
	}

	private class BigQueryReadStatement implements IoCommand.Statement {
		private BigQueryIterator bigQueryIterator;
		private BigQueryReadClient client;

		@Override
		public IoCommand.Result execute(Object... objects) throws IOException {
			@SuppressWarnings("unchecked") Map<String, Object> input = (Map<String, Object>) objects[0];
			// add all relevant inputs for bigQueryRead function
			input.put(INPUT_CREDENTIALS_PATH, credentialsFilePath);
			input.put(INPUT_PROJECT_ID, projectId);
			return bigQueryRead(input);
		}

		@Override
		public void close() {
			if (bigQueryIterator != null) {
				bigQueryIterator = null;
			}
			Util.safeClose(client);
			client = null;
		}

		/**
		 *
		 * @param input contains projectId, dataset, table, filter and credentialsPath of the key file.
		 * @return IoCommand result - an iterator of BigQuery records
		 * @throws IOException
		 */
		private IoCommand.Result bigQueryRead(Map<String, Object> input) throws IOException {
			String projectIdInput = (String) input.get(INPUT_PROJECT_ID);
			String dataset = (String) input.get(INPUT_DATASET);
			String table = (String) input.get(INPUT_TABLE);
			String filter = input.get(INPUT_FILTERS) != null ? (String) input.get(INPUT_FILTERS) : "";
			String credentialsPath = (String) input.get(INPUT_CREDENTIALS_PATH);
			long limit = input.get(INPUT_LIMIT) != null ? (long) input.get(INPUT_LIMIT) : 0;
			@SuppressWarnings("unchecked") ArrayList<String> fields = input.get(INPUT_FIELDS) != null ? (ArrayList<String>) input.get(INPUT_FIELDS) : new ArrayList<>();
			// create credentials based on the authentication key file
			GoogleCredentials credentials = ServiceAccountCredentials.fromStream(
					new FileInputStream(credentialsPath));
			BigQueryReadSettings settings = BigQueryReadSettings.newBuilder()
					.setCredentialsProvider(FixedCredentialsProvider.create(credentials))
					.build();
			Util.safeClose(this.client);
			// Set up the BigQueryReadClient with credentials
			this.client = BigQueryReadClient.create(settings);
			ReadSession session = createBqReadSession(client, projectIdInput, dataset, table, filter,fields);
			String streamName = session.getStreamsCount() > 0 ? session.getStreams(0).getName() : null;
			String avroSchemaString = session.getAvroSchema().getSchema();
			org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaString);
			GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
			//Create Big Query Iterator to read and iterate over the records.
			this.bigQueryIterator = new BigQueryIterator(client, streamName, reader, limit);
			return new BigQueryReadResult();
		}

		private ReadSession createBqReadSession(BigQueryReadClient client, String projectId, String dataset, String table, String filter, ArrayList<String> fields) {
			String parent = String.format("projects/%s", projectId);
			// Format table name to connect to
			String srcTable =
					String.format(
							"projects/%s/datasets/%s/tables/%s",
							projectId, dataset, table);
			// Options to utilize the filter input for the read
			ReadSession.TableReadOptions options =
					ReadSession.TableReadOptions.newBuilder().addAllSelectedFields(fields).setRowRestriction(filter)
							.build();
			// The data format is AVRO, easier to read and utilize
			ReadSession.Builder sessionBuilder =
					ReadSession.newBuilder()
							.setTable(srcTable)
							.setDataFormat(DataFormat.AVRO)
							.setReadOptions(options);
			CreateReadSessionRequest.Builder builder =
					CreateReadSessionRequest.newBuilder()
							.setParent(parent)
							.setReadSession(sessionBuilder)
							.setMaxStreamCount(1);
			// Return the Read Session
			return client.createReadSession(builder.build());
		}

		private class BigQueryReadResult implements Result{
			@NotNull
			@Override
			public Iterator<IoCommand.Row> iterator() {
				//Translator that translates each Generic Record read by the bq itr into IoSimpleRow
				//The translator class the "next" function of the bq itr, then runs the translation function on the record to convert it to IoSimpleRow
				return new IteratorTranslate<>(bigQueryIterator,rec -> {
					List<Schema.Field> schemaFields = rec.getSchema().getFields();
					Map<String, Integer> keys = new LinkedHashMap<>();
					for (int i = 0; i < schemaFields.size(); i++) {
						keys.put(schemaFields.get(i).name(), i);
					}
					Object[] values = new Object[keys.size()];
					for (int i = 0; i < schemaFields.size(); i++) {
						values[i] = parseAvroValue(rec.get(i), schemaFields.get(i));
					}
					return new IoSimpleRow(values, keys);
				});
			}
		}

	}

	@Override
	public void abort() {
		Util.safeClose(this.statement);
		this.statement = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getMetadata(Map<String, Object> params) {
		return (T) new BigQueryMetadata(credentialsFilePath, interfaceName, null, this, projectId, params);
	}
}
