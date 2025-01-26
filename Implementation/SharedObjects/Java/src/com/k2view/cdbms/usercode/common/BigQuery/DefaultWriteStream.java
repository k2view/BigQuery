package com.k2view.cdbms.usercode.common.BigQuery;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.k2view.fabric.common.Log;
import io.grpc.Status;
import io.grpc.Status.Code;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;
import javax.annotation.concurrent.GuardedBy;
import org.json.JSONArray;

/*
    For debugging info, add the XML lines below on logback.xml.
    Don't forget to remove the comment delimiters to make them effective.

    <!--<logger name="com.k2view.com.k2view.cdbms.usercode.common.BigQuery" level="DEBUG" />-->
    <!--<logger name="com.google" level="DEBUG" />-->
    <!--<logger name="io.grpc" level="DEBUG" />-->
*/

/*
This class can open/close a BigQuery write stream and write to it. It has two nested classes
AppendContext, DataWriter
* */
public class DefaultWriteStream implements WriteStream {
    private final Log log = Log.a(this.getClass());
    private final String projectId;
    private final BigQuery bigQuery;
    private final BigQueryWriteClient bigQueryWriteClient;
    private final Map<String, DataWriter> dataWriters = new HashMap<>();

    public DefaultWriteStream(String projectId, String credentialFilePath) throws IOException {
        this.projectId = projectId;
        log.debug("projectId={}", this.projectId);

        // If you don't specify credentials when constructing the client, the client library will
        // look for credentials via the environment variable GOOGLE_APPLICATION_CREDENTIALS.
        Path path = Paths.get(credentialFilePath);
        log.debug("path={}", path);

        try (FileInputStream credentialsStream = new FileInputStream(credentialFilePath)) {
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(credentialsStream);
            this.bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();

            BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
                    .newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();

            this.bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);
        }

        
    }

    public void write(String dataset, String table, JSONArray rows) throws DescriptorValidationException, InterruptedException, IOException {
        log.debug("dataset={}", dataset);
        log.debug("table={}", table);

        TableName parentTable = TableName.of(this.projectId, dataset, table);
        log.debug("parentTable={}", parentTable);

        // If we already have a dataWriter for parentTable, we use it, otherwise create a new one
        // and save it in dataWriters
        DataWriter dataWriter = dataWriters.get(parentTable.toString());
        if (dataWriter == null) {
            log.debug("Instantiating data writer for {}", parentTable);
            dataWriter = new DataWriter();
            dataWriter.initialize(parentTable, this.bigQuery, this.bigQueryWriteClient);
            this.dataWriters.put(parentTable.toString(), dataWriter);
        }
        AppendContext appendContext = new AppendContext(rows, 0);

        dataWriter.append(appendContext);
    }

    @Override
    public void close() {
        // Close all dataWriters
        this.dataWriters.forEach((tn,dw) -> dw.cleanup());
        bigQueryWriteClient.shutdown();
        bigQueryWriteClient.close();
    }

    private static class AppendContext {
        JSONArray data;
        int retryCount;

        AppendContext(JSONArray data, int retryCount) {
            this.data = data;
            this.retryCount = retryCount;
        }
    }

    private static class DataWriter {
        private final Log log = Log.a(this.getClass());
        private static final int MAX_RETRY_COUNT = 2;
        private static final List<Code> RETRY_ERROR_CODES = ImmutableList.of(Code.INTERNAL, Code.ABORTED, Code.CANCELLED);

        // Track the number of in-flight requests to wait for all responses before shutting down.
        private final Phaser inFlightRequestCount = new Phaser(1);
        private final Object lock = new Object();
        private JsonStreamWriter streamWriter;

        @GuardedBy("lock")
        private RuntimeException error = null;

        public void initialize(TableName parentTable, BigQuery bigQuery, BigQueryWriteClient bigQueryWriteClient)
                throws DescriptorValidationException, IOException, InterruptedException {
            Table table = bigQuery.getTable(parentTable.getDataset(), parentTable.getTable());
            log.debug("table={}", table);

            Schema schema = table.getDefinition().getSchema();
            if (schema == null) {
                throw new AssertionError();
            }
            log.debug("schema={}", schema);

            // Retrieve table schema information.
            TableSchema tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema);
            log.debug("tableSchema={}", tableSchema);

            // Use the JSON stream writer to send records in JSON format.
            // Specify the table name to write to the default stream.
            // For more information about JsonStreamWriter, see:
            // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
            this.streamWriter = JsonStreamWriter
                    .newBuilder(parentTable.toString(), tableSchema, bigQueryWriteClient)
                    .setIgnoreUnknownFields(true)
                    .build();
        }

        public void append(AppendContext appendContext) throws DescriptorValidationException, IOException {
            synchronized (this.lock) {
                if (this.error != null) {
                    log.debug("Earlier appends have failed. Need to reset before continuing.");
                    throw this.error;
                }
            }

            // Append asynchronously for increased throughput.
            ApiFuture<AppendRowsResponse> future = this.streamWriter.append(appendContext.data);

            ApiFutures.addCallback(future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

            this.inFlightRequestCount.register();
            log.debug("In-flight requests counter increased. registered={}", this.inFlightRequestCount.getRegisteredParties());
        }

        public void cleanup() {
            log.debug("Waiting for in-flight requests to complete. unarrived={}", this.inFlightRequestCount.getUnarrivedParties());
            this.inFlightRequestCount.arriveAndAwaitAdvance();

            this.streamWriter.close();

            synchronized (this.lock) {
                if (this.error != null) {
                    log.debug("An error occurred in the stream.");
                    throw this.error;
                }
            }
        }

        static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
            private final Log log = Log.a(this.getClass());
            private final DataWriter parent;
            private final AppendContext appendContext;

            public AppendCompleteCallback(DataWriter parent, AppendContext appendContext) {
                this.parent = parent;
                log.debug("parent={}", this.parent);

                this.appendContext = appendContext;
            }

            @Override
            public void onSuccess(AppendRowsResponse response) {
                log.debug("onSuccess");
                done();
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.debug("onFailure");

                if (throwable instanceof Exceptions.AppendSerializtionError) {
                    Exceptions.AppendSerializtionError appendSerializationError = (Exceptions.AppendSerializtionError) throwable;
                    log.debug("onFailure failedRows={}", appendSerializationError.getRowIndexToErrorMessage());
                }
                // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
                // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
                // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
                Status status = Status.fromThrowable(throwable);
                log.debug("onFailure status={}", status);

                log.debug("onFailure appendContext.retryCount={}", this.appendContext.retryCount);
                log.debug("onFailure status.getCode={}", status.getCode());
                if (this.appendContext.retryCount < MAX_RETRY_COUNT && RETRY_ERROR_CODES.contains(status.getCode())) {
                    this.appendContext.retryCount++;
                    try {
                        log.debug("onFailure - Retrying the append.");
                        // Since default stream appends are not ordered, we can simply retry the appends.
                        // Retrying with exclusive streams requires more careful consideration.
                        this.parent.append(this.appendContext);
                        // Mark the existing attempt as done since it's being retried.
                        done();
                        return;
                    } catch (Exception e) {
                        // Fall through to return error.
                        log.error("onFailure - Failed to retry append", e);
                    }
                }

                log.debug("onFailure - Verifying that no error occurred in the stream.");
                synchronized (this.parent.lock) {
                    if (this.parent.error == null) {
                        StorageException storageException = Exceptions.toStorageException(throwable);
                        this.parent.error = (storageException != null) ? storageException : new RuntimeException(throwable);
                    }
                }

                log.error("onFailure - Error", throwable);
                done();
            }

            private void done() {
                log.debug("De-registering in-flight requests. unarrived={}", this.parent.inFlightRequestCount.getUnarrivedParties());
                this.parent.inFlightRequestCount.arriveAndDeregister();
                log.debug("Done! arrived={}", this.parent.inFlightRequestCount.getArrivedParties());
            }
        }
    }
}
