package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoSession;

public class BigQuerySnapshot implements SnapshotDataset {
    private final IoSession commandSession;
    private final IoSession readSession;
    private final String table;
    private final String schema;
    private final SampleSize size;
    private final Log log = Log.a(this.getClass());
    private IoCommand.Statement readStatement;
    private IoCommand.Result readResult;
    private final boolean useStorageApi;
    private final String datasetsProjectId;

    public BigQuerySnapshot(IoSession commandSession, IoSession readSession, String table, String schema,
            String datasetsProjectId, SampleSize size, boolean useStorageApi) {
        this.commandSession = commandSession;
        this.readSession = readSession;
        this.table = table;
        this.schema = schema;
        this.datasetsProjectId = datasetsProjectId;
        this.size = size;
        this.useStorageApi = useStorageApi;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Map<String, Object>> fetch() throws Exception {
        final long limit = getLimit();

        Util.safeClose(readResult);
        Util.safeClose(readStatement);
        try {
            if (useStorageApi) {
                Map<String, Object> executeParams = new HashMap<>();
                executeParams.put(BigQueryReadIoSession.INPUT_DATASET, schema);
                executeParams.put(BigQueryReadIoSession.INPUT_TABLE, table);
                executeParams.put(BigQueryReadIoSession.INPUT_LIMIT, limit);

                log.debug("Fetching BigQuery snapshot via Storage Read API with params={}", executeParams);
                this.readStatement = readSession.statement();
                this.readResult = readStatement.execute(executeParams);
            } else {
                this.readStatement = commandSession
                        .prepareStatement(String.format("select * from `%s.%s.%s` limit ?", datasetsProjectId, schema, table));
                this.readResult = readStatement.execute(limit);
            }
            Iterator<IoCommand.Row> iterator = readResult.iterator();
            return (Iterator<Map<String, Object>>) (Iterator<?>) iterator;
        } catch (Exception e) {
            log.error("Unable to fetch snapshot for schema='{}', table='{}'", schema, table, e);
            return null;
        }

    }

    @Override
    public void close() throws Exception {
        Util.safeClose(readResult);
        Util.safeClose(readStatement);
        readStatement = null;
        readResult = null;
    }

    private int getLimit() throws Exception {
        int limit;
        long count = getNumberOfRows();
        int countPercentage = Math.toIntExact(count * size.getPercentage() / 100);
        if (countPercentage < size.getMin()) {
            limit = Math.toIntExact(size.getMin());
        } else if (countPercentage >= size.getMax()) {
            limit = Math.toIntExact(size.getMax());
        } else {
            limit = countPercentage;
        }
        return limit;
    }

    private long getNumberOfRows() throws Exception {
        try (
                IoCommand.Statement statement = commandSession.prepareStatement(
                        String.format("SELECT row_count FROM %s.%s.__TABLES__ WHERE table_id = ?", datasetsProjectId, schema));
                IoCommand.Result result = statement.execute(table)) {
            return (Long) result.firstValue();
        }
    }
}
