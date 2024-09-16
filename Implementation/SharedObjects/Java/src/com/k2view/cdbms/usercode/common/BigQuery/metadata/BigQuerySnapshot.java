package com.k2view.cdbms.usercode.common.BigQuery.metadata;

import com.k2view.cdbms.usercode.common.BigQuery.BigQueryCommandIoSession;
import com.k2view.cdbms.usercode.common.BigQuery.BigQueryReadIoSession;
import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.model.impl.DatasetEntry;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BigQuerySnapshot implements SnapshotDataset {
    private final BigQueryCommandIoSession commandSession;
    private final String table;
    private final String schema;
    private final SampleSize size;
    private final Log log = Log.a(this.getClass());
    private IoCommand.Statement readStatement;
    private IoCommand.Result readResult;

    public BigQuerySnapshot(BigQueryCommandIoSession commandSession, BigQueryReadIoSession readSession, String table, String schema, SampleSize size) {
        this.commandSession = commandSession;
        this.table = table;
        this.schema = schema;
        this.size = size;
        this.readStatement = readSession.statement();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Map<String, Object>> fetch() throws Exception {
        final long limit = getLimit();
        Map<String, Object> executeParams = new HashMap<>();
        executeParams.put(BigQueryReadIoSession.INPUT_DATASET, schema);
        executeParams.put(BigQueryReadIoSession.INPUT_TABLE, table);
        executeParams.put(BigQueryReadIoSession.INPUT_LIMIT, limit);
        log.debug("Fetching BigQuery snapshot with params={}", executeParams);
        Util.safeClose(readResult);
        this.readResult = readStatement.execute(executeParams);
        Iterator<IoCommand.Row> iterator = readResult.iterator();
        return (Iterator<Map<String, Object>>)(Iterator<?>) iterator;
    }

    @Override
    public void close() throws Exception {
        Util.safeClose(readStatement);
        Util.safeClose(readResult);
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
        try (IoCommand.Statement statement = commandSession.statement()){
            IoCommand.Result result = statement.execute(String.format("SELECT row_count FROM %s.__TABLES__ WHERE table_id = '%s'", schema, table));
            return (Long) result.firstValue();
        }
    }
}
