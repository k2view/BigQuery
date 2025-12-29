package com.k2view.cdbms.usercode.common.BigQuery;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.k2view.broadway.metadata.Any;
import com.k2view.broadway.metadata.ArrayType;
import com.k2view.broadway.metadata.ObjectType;
import com.k2view.broadway.metadata.Primitive;
import com.k2view.broadway.metadata.Properties;
import com.k2view.broadway.metadata.Schema;
import com.k2view.broadway.metadata.Type;
import com.k2view.cdbms.lut.InterfacesManager;
import com.k2view.cdbms.usercode.common.BigQuery.DatasetFieldsBuilder.SchemaPropertyContext;
import com.k2view.discovery.MonitorStatusUpdater;
import com.k2view.discovery.rules.CrawlerRules;
import com.k2view.discovery.rules.DataPlatformMetaDataInfo;
import com.k2view.discovery.schema.io.CrawlerAbortedException;
import com.k2view.discovery.schema.io.IoMetadata;
import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.model.Category;
import com.k2view.discovery.schema.model.DataPlatform;
import com.k2view.discovery.schema.model.impl.ConcreteClassNode;
import com.k2view.discovery.schema.model.impl.ConcreteDataPlatform;
import com.k2view.discovery.schema.model.impl.ConcreteDataset;
import com.k2view.discovery.schema.model.impl.ConcreteField;
import com.k2view.discovery.schema.model.impl.ConcreteNode;
import com.k2view.discovery.schema.model.impl.ConcreteSchemaNode;
import com.k2view.discovery.schema.model.impl.PrimitiveClass;
import com.k2view.discovery.schema.model.types.BooleanClass;
import com.k2view.discovery.schema.model.types.BytesClass;
import com.k2view.discovery.schema.model.types.DateClass;
import com.k2view.discovery.schema.model.types.DateTimeClass;
import com.k2view.discovery.schema.model.types.IntegerClass;
import com.k2view.discovery.schema.model.types.RealClass;
import com.k2view.discovery.schema.model.types.StringClass;
import com.k2view.discovery.schema.model.types.TimeClass;
import com.k2view.discovery.schema.model.types.UnknownClass;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoCommand.Row;
import com.k2view.fabric.common.io.IoSession;

public class BigQueryMetadata implements IoMetadata {
    private static final String STATUS_CRAWLER = "crawler";
    private static final String SCHEMA = "schema";
    private static final String DATASET = "dataset";
    private static final String CLASS = "class";
    // private static final String FIELD = "field";
    private static final String ENTITY_NAME = "entityName";
    private static final String CRAWLER = "Crawler";
    // private static final String PROPERTY = "property";
    // private static final String ORDINAL_POSITION = "ordinal_position";
    // private static final String POSITION_IN_UNIQUE_CONSTRAINT =
    // "position_in_unique_constraint";
    // private static final String COLUMN_NAME = "column_name";
    // private static final String CONSTRAINT_NAME = "constraint_name";
    private static final String TABLE_NAME = "table_name";
    private static final Map<StandardSQLTypeName, Integer> SQL_TYPE_MAPPING = createBQToSQLTypeMap();
    private static final Map<StandardSQLTypeName, PrimitiveClass> DEFINED_BY_MAPPING = createBQToDefinedByMap();

    private final Log log = Log.a(this.getClass());
    private IoSession commandSession;
    private IoSession readSession;
    private boolean selfCreatedCommandSession;
    private boolean selfCreatedReadSession;
    private final String interfaceName;

    private Set<String> schemasExclude = new HashSet<>();
    private Map<String, List<String>> tablesExclude = new HashMap<>();
    private Set<String> schemasInclude = new HashSet<>();
    private Map<String, List<String>> tablesInclude = new HashMap<>();
    private final String datasetsProjectId;
    private final String jobUid;
    private final DataPlatformMetaDataInfo dataPlatformMetaDataInfo;
    private int totalFields;
    private boolean aborted;
    private final boolean snapshotViaStorage;
    private final BigQuery bqClient;

    private static Map<StandardSQLTypeName, Integer> createBQToSQLTypeMap() {
        Map<StandardSQLTypeName, Integer> map = new HashMap<>();
        map.put(StandardSQLTypeName.ARRAY, Types.VARCHAR);
        map.put(StandardSQLTypeName.BIGNUMERIC, Types.DECIMAL);
        map.put(StandardSQLTypeName.BOOL, Types.BOOLEAN);
        map.put(StandardSQLTypeName.BYTES, Types.BINARY);
        map.put(StandardSQLTypeName.DATE, Types.DATE);
        map.put(StandardSQLTypeName.DATETIME, Types.TIMESTAMP);
        map.put(StandardSQLTypeName.FLOAT64, Types.DOUBLE);
        map.put(StandardSQLTypeName.GEOGRAPHY, Types.VARCHAR);
        map.put(StandardSQLTypeName.INT64, Types.BIGINT);
        map.put(StandardSQLTypeName.INTERVAL, Types.VARCHAR);
        map.put(StandardSQLTypeName.JSON, Types.VARCHAR);
        map.put(StandardSQLTypeName.NUMERIC, Types.NUMERIC);
        map.put(StandardSQLTypeName.STRING, Types.VARCHAR);
        map.put(StandardSQLTypeName.STRUCT, Types.VARCHAR);
        map.put(StandardSQLTypeName.TIME, Types.TIME);
        map.put(StandardSQLTypeName.TIMESTAMP, Types.TIMESTAMP);
        map.put(StandardSQLTypeName.RANGE, Types.VARCHAR);

        return map;
    }

    private static Map<StandardSQLTypeName, PrimitiveClass> createBQToDefinedByMap() {
        Map<StandardSQLTypeName, PrimitiveClass> map = new HashMap<>();
        map.put(StandardSQLTypeName.BIGNUMERIC, RealClass.REAL);
        map.put(StandardSQLTypeName.BOOL, BooleanClass.BOOLEAN);
        map.put(StandardSQLTypeName.BYTES, BytesClass.BYTES);
        map.put(StandardSQLTypeName.DATE, DateClass.DATE);
        map.put(StandardSQLTypeName.DATETIME, DateTimeClass.DATETIME);
        map.put(StandardSQLTypeName.FLOAT64, RealClass.REAL);
        map.put(StandardSQLTypeName.GEOGRAPHY, UnknownClass.UNKNOWN);
        map.put(StandardSQLTypeName.INT64, IntegerClass.INTEGER);
        map.put(StandardSQLTypeName.INTERVAL, UnknownClass.UNKNOWN);
        map.put(StandardSQLTypeName.JSON, StringClass.STRING);
        map.put(StandardSQLTypeName.NUMERIC, RealClass.REAL);
        map.put(StandardSQLTypeName.STRING, StringClass.STRING);
        map.put(StandardSQLTypeName.STRUCT, UnknownClass.UNKNOWN);
        map.put(StandardSQLTypeName.TIME, TimeClass.TIME);
        map.put(StandardSQLTypeName.TIMESTAMP, DateTimeClass.DATETIME);
        map.put(StandardSQLTypeName.RANGE, UnknownClass.UNKNOWN);

        return map;
    }

    public BigQueryMetadata(String interfaceName, IoSession commandIoSession, IoSession readSession, BigQuery bqClient,
            String datasetsProjectId,
            boolean snapshotViaStorage, Map<String, Object> props) throws Exception {
        this.interfaceName = interfaceName;
        this.datasetsProjectId = datasetsProjectId;
        this.commandSession = commandIoSession;
        this.readSession = readSession;
        this.snapshotViaStorage = snapshotViaStorage;
        this.bqClient = bqClient;
        if (commandSession == null) {
            this.commandSession = InterfacesManager.getInstance().getInterface(interfaceName).getIoSession(null);
            this.selfCreatedCommandSession = true;
        }
        if (readSession == null && snapshotViaStorage) {
            this.readSession = InterfacesManager.getInstance().getInterface(interfaceName).getIoSession(
                    Map.of(BigQueryIoProvider.OPERATION_PARAM_NAME, BigQueryIoProvider.Operation.READ));
            this.selfCreatedReadSession = true;
        }
        this.jobUid = ParamConvertor.toString(props.get("uuid"));
        this.dataPlatformMetaDataInfo = ((CrawlerRules) props.get("rules")).getMetaData(interfaceName);
        if (this.dataPlatformMetaDataInfo == null) {
            return;
        }
        setIncludeSchema();
    }

    private void assertAborted() {
        if (aborted) {
            throw new CrawlerAbortedException(String.format("Crawler for dataPlatform '%s' is aborted", interfaceName));
        }
    }

    @SuppressWarnings("deprecation")
    private void setIncludeSchema() {
        Set<String> schemaSet = this.dataPlatformMetaDataInfo.getSchemaMetadata().getSet();
        if (!Util.isEmpty(schemaSet) && this.dataPlatformMetaDataInfo.getSchemaMetadata().isIncludeOrExcludeList) {
            schemasInclude = schemaSet;
            populateTableLists(schemasInclude);
        } else {
            schemasExclude = schemaSet;
        }
        populateTableLists(this.dataPlatformMetaDataInfo.getTableSetPerSchema());
    }

    private void populateTableLists(Set<String> schemas) {
        if (schemas.isEmpty()) {
            handleSchema("");
        } else {
            for (String schema : schemas) {
                handleSchema(schema);
            }
        }
    }

    private void handleSchema(String schema) {
        DataPlatformMetaDataInfo.MetaDataListInfo tableListInfo = this.dataPlatformMetaDataInfo
                .getTableListPerSchema(schema);
        if (tableListInfo != null) {
            if (tableListInfo.isIncludeOrExcludeList) {
                tablesInclude.put(schema, new ArrayList<>(tableListInfo.getSet()));
            } else {
                tablesExclude.put(schema, new ArrayList<>(tableListInfo.getSet()));
            }
        }
    }

    @Override
    public DataPlatform getDataPlatform() throws Exception {
        MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid, 0);
        MonitorStatusUpdater.getInstance().registerDuration(STATUS_CRAWLER, jobUid);
        MonitorStatusUpdater.getInstance().updateProgress(STATUS_CRAWLER, jobUid, 0);
        ConcreteDataPlatform dataPlatform = addPlatformNode(this.interfaceName);
        addSchemaNodes(dataPlatform);
        MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid, totalFields);
        return dataPlatform;
    }

    private ConcreteDataPlatform addPlatformNode(String platform) {
        ConcreteDataPlatform dataPlatform = new ConcreteDataPlatform(platform, 1.0, CRAWLER, "", "Data platform",
                platform);
        String idPrefix = "dataPlatform:" + dataPlatform.getId();
        dataPlatform.addProperty(idPrefix, ENTITY_NAME, "Data Platform Name", dataPlatform.getName(), 1.0, CRAWLER, "");
        dataPlatform.addProperty(idPrefix, "type", "Data Platform Type", "BigQuery", 1.0, CRAWLER, "");
        return dataPlatform;
    }

    @Override
    public void abort() throws Exception {
        this.aborted = true;
    }

    private void addSchemaNodes(ConcreteDataPlatform dataPlatform) throws Exception {
        String query = String.format("SELECT * EXCEPT (schema_owner) FROM %s.INFORMATION_SCHEMA.SCHEMATA",
                datasetsProjectId);
        List<Object> statementParams = new LinkedList<>();
        Set<String> include = schemasInclude.stream().filter(schema -> !schemasExclude.contains(schema))
                .collect(Collectors.toSet());
        if (!Util.isEmpty(include)) {
            query = query.concat(" WHERE schema_name IN UNNEST (?)");
            statementParams.add(include);
        } else if (!Util.isEmpty(schemasExclude)) {
            query = query.concat(" WHERE schema_name NOT IN UNNEST (?)");
            statementParams.add(schemasExclude);
        }

        try (
                IoCommand.Statement statement = this.commandSession.prepareStatement(query);
                IoCommand.Result schemas = statement.execute(statementParams.toArray())) {
            for (Row schema : schemas) {
                assertAborted();
                String schemaName = schema.get("schema_name").toString();
                ConcreteSchemaNode schemaNode = new ConcreteSchemaNode(schemaName, 1.0, CRAWLER, "",
                        "Name of the schema", schemaName);
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), ENTITY_NAME, "Name of the schema (/BQ-Dataset)",
                        schemaName, 1.0, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "projectId",
                        "The name of the catalog (/BQ-Project) that contains the schema(/BQ-Dataset)",
                        schema.get("catalog_name"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "creationTime", "The BQ dataset's creation time",
                        schema.get("creation_time"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "lastModifiedTime",
                        "The BQ dataset's last modified time", schema.get("last_modified_time"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "location", "The BQ dataset's geographic location",
                        schema.get("location"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "ddl",
                        "The CREATE SCHEMA DDL statement that can be used to create the BQ dataset", schema.get("ddl"),
                        1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "defaultCollationName",
                        "The name of the default collation specification if it exists; otherwise, NULL.",
                        String.valueOf(schema.get("default_collation_name")), 1, CRAWLER, "");
                assertAborted();
                addDatasetNodes(schemaNode);
                dataPlatform.contains(schemaNode, 1.0, CRAWLER, "");
            }
        }
    }

    private void addDatasetNodes(ConcreteSchemaNode schemaNode) throws Exception {
        String schemaName = schemaNode.getName();
        QueryAndParams tablesQueryAndParams = getTablesQueryAndParams(schemaName);
        // QueryAndParams keyColUsageQueryAndParams =
        // getKeyColumnUsageQueryAndParams(schemaName);
        // QueryAndParams constraintColUsageQueryAndParams =
        // getConstraintColumnUsageQueryAndParams(schemaName);

        // try (AutoCloseableStatementsResults closeableResults = execQueriesInParallel(
        // tablesQueryAndParams,
        // keyColUsageQueryAndParams,
        // constraintColUsageQueryAndParams)) {
        try (AutoCloseableStatementsResults closeableResults = execQueriesInParallel(
                tablesQueryAndParams)) {
            IoCommand.Result tables = closeableResults.getResult(0);
            // IoCommand.Result keyColumnUsage = closeableResults.getResult(1);
            // IoCommand.Result constraintColumnUsage = closeableResults.getResult(2);
            // Map<String, List<IoCommand.Row>> tableKeyColumnUsageMap = new HashMap<>();
            Map<String, FieldList> tableFields = new HashMap<>();
            // keyColumnUsage.forEach(usage -> {
            // String tableName = (String) usage.get(TABLE_NAME);
            // tableKeyColumnUsageMap.computeIfAbsent(tableName, key -> new LinkedList<>());
            // tableKeyColumnUsageMap.get(tableName).add(usage);
            // });
            assertAborted();
            processTables(schemaNode, tables, tableFields);
            // processTables(schemaNode, tables, tableKeyColumnUsageMap, tableFields);
            // Add refersTo to every table referenced by a Foreign Key constraint in another
            // one
            // addForeignKeys(schemaNode,
            // Lists.newArrayList(keyColumnUsage),
            // Lists.newArrayList(constraintColumnUsage));
        }
    }

    private static PrimitiveClass definedBy(String sourceDataType) {
        if (sourceDataType == null) return UnknownClass.UNKNOWN;
        sourceDataType = sourceDataType.toUpperCase();
        return DEFINED_BY_MAPPING.getOrDefault(sourceDataType.startsWith("REPEATED") ? ""
                : StandardSQLTypeName.valueOf(sourceDataType), UnknownClass.UNKNOWN);
    }

    private void processTables(ConcreteSchemaNode schemaNode, IoCommand.Result tables,
            Map<String, FieldList> tableFields) throws Exception {
        for (IoCommand.Row row : tables) {
            assertAborted();
            String tableName = row.get(TABLE_NAME).toString();
            FieldList fields = bqClient.getTable(TableId.of(datasetsProjectId, schemaNode.getName(), tableName))
                    .getDefinition().getSchema()
                    .getFields();
            tableFields.put(tableName, fields);
            // MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid,
            // fields.size());
            ConcreteClassNode datasetClassNode = new ConcreteClassNode(tableName, 1.0, CRAWLER, "", "Table name",
                    tableName);
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), ENTITY_NAME, "Name of the table", tableName,
                    1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "tableType",
                    "The table type (BASE TABLE/CLONE/SNAPSHOT/VIEW/MATERIALIZED VIEW/EXTERNAL)", row.get("table_type"),
                    1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "creationTime", "The table's creation time",
                    row.get("creation_time"), 1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "ddl",
                    "The DDL statement that can be used to recreate the table, such as CREATE TABLE or CREATE VIEW",
                    row.get("ddl"), 1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "defaultCollationName",
                    "The name of the default collation specification if it exists; otherwise, NULL",
                    String.valueOf(row.get("default_collation_name")), 1.0, CRAWLER, "");

            ConcreteDataset datasetNode = new ConcreteDataset(tableName, 1.0, CRAWLER, "", "Table name", tableName);
            datasetNode.definedBy(datasetClassNode, 1.0, CRAWLER, "");
            datasetNode.addProperty(this.idPrefix(DATASET, datasetNode), ENTITY_NAME, "Name of the table", tableName,
                    1.0, CRAWLER, "");
            schemaNode.contains(datasetNode, 1.0, CRAWLER, "");

            DatasetFieldsBuilder.fromObjectSchema(datasetClassNode, convertFieldListToObjectType(fields, null),
                    this::schemaContextConsumer,
                    BigQueryMetadata::definedBy);
            int progress = fields.size();
            totalFields += progress;
            MonitorStatusUpdater.getInstance().updateProgress(STATUS_CRAWLER, jobUid, progress);
        }
    }

    private void schemaContextConsumer(SchemaPropertyContext context) {
        ConcreteField fieldNode = context.field();
        int ordinalPosition = context.ordinalPosition();
        Schema schema = context.schema();
        fieldNode.addProperty(context.idPrefix(), Category.ordinalPosition.name(), "Ordinal position",
                ordinalPosition, 1.0, CRAWLER, "");
        String sourceDataType = schema.description() != null ? schema.description() : "";
        if (context.isTopLevel()) {
            fieldNode.addProperty(context.idPrefix(), Category.sourceDataType.name(), "Column type",
                    sourceDataType, 1.0, CRAWLER, "");
            fieldNode.addProperty(context.idPrefix(), Category.sourceNullable.name(),
                    "Nullability of the field 1 or 0",
                    true, 1.0,
                    CRAWLER, "");
            fieldNode.addProperty(context.idPrefix(), Category.sourceEntityType.name(), "Role",
                    "Column", 1.0,
                    CRAWLER, "");
            // fieldNode.addProperty(context.idPrefix(), Category.columnSize.name(),
            // "Max column size in bytes",
            // 0, 1.0, CRAWLER, "");
            fieldNode.addProperty(context.idPrefix(), Category.sqlDataType.name(), "SQL data type",
                    SQL_TYPE_MAPPING.getOrDefault(
                            sourceDataType.startsWith("REPEATED") ? ""
                                    : StandardSQLTypeName.valueOf(sourceDataType.toUpperCase()),
                            Types.VARCHAR),
                    1.0, CRAWLER, "");
        }
        if (schema.equals(Any.ANY)) {
            fieldNode.addProperty(context.idPrefix(), Category.definedBy.name(), "Data type for field",
                    UnknownClass.UNKNOWN.getClassName(), 1.0, CRAWLER, "");
        } else if (schema.type().isPrimitive()) {
            fieldNode.addProperty(context.idPrefix(), Category.definedBy.name(), "Data type for field",
                    definedBy(sourceDataType).getName().toUpperCase(), 1.0, CRAWLER, "");
        }

        // List<Row> uniqueConstraints = tableKeyColumnUsageMap.getOrDefault(tableName,
        // new LinkedList<>())
        // .stream()
        // .filter(constraint -> constraint.get(POSITION_IN_UNIQUE_CONSTRAINT) == null)
        // .collect(Collectors.toList());

        // if (!Util.isEmpty(uniqueConstraints)) {
        // uniqueConstraints
        // .stream()
        // .filter(constraint -> constraint.get(COLUMN_NAME).equals(field.getName()))
        // .findFirst()
        // .ifPresent(constraint -> fieldNode.addProperty(
        // this.idPrefix(FIELD, fieldNode),
        // "pk",
        // "Primary Key (Unenforced)",
        // true,
        // 1.0,
        // CRAWLER,
        // ""));
        // }
    }

    private AutoCloseableStatementsResults execQueriesInParallel(QueryAndParams... queries)
            throws InterruptedException {
        List<IoCommand.Statement> statements = new ArrayList<>(Collections.nCopies(queries.length, null));
        List<IoCommand.Result> results = new ArrayList<>(Collections.nCopies(queries.length, null));
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < queries.length; i++) {
            QueryAndParams query = queries[i];
            int index = i;

            Thread thread = Util.thread(() -> {
                try {
                    IoCommand.Statement statement = commandSession.prepareStatement(query.query());
                    statements.set(index, statement);
                    IoCommand.Result result = statement.execute(query.params().toArray());
                    results.set(index, result);
                } catch (Exception e) {
                    log.error("Failed to execute query: {}", query.query(), e);
                }
            });
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        return new AutoCloseableStatementsResults(statements, results);
    }

    private QueryAndParams getTablesQueryAndParams(String schemaName) {
        String query = String.format(
                "SELECT * FROM %s.%s.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE!='VIEW'",
                datasetsProjectId,
                schemaName);
        List<Object> params = new LinkedList<>();
        if (tablesInclude.containsKey(schemaName)) {
            query = query.concat(" AND table_name IN UNNEST (?)");
            params.add(tablesInclude.get(schemaName));
        } else if (tablesExclude.containsKey(schemaName)) {
            query = query.concat(" AND table_name NOT IN UNNEST (?)");
            params.add(tablesExclude.get(schemaName));
        }
        return new QueryAndParams(query, params);
    }

    // private QueryAndParams getKeyColumnUsageQueryAndParams(String schemaName) {
    // String query = String.format(
    // "SELECT * FROM %s.%s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
    // projectId,
    // schemaName);
    // return new QueryAndParams(query, Collections.emptyList());
    // }

    // private QueryAndParams getConstraintColumnUsageQueryAndParams(String
    // schemaName) {
    // String query = String.format(
    // "SELECT * FROM %s.%s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE",
    // projectId,
    // schemaName);
    // return new QueryAndParams(query, Collections.emptyList());
    // }

    private record QueryAndParams(String query, List<Object> params) {
    }

    private static class AutoCloseableStatementsResults implements AutoCloseable {
        private final List<IoCommand.Statement> statements;
        private final List<IoCommand.Result> results;

        public AutoCloseableStatementsResults(List<IoCommand.Statement> statements, List<IoCommand.Result> results) {
            this.statements = statements;
            this.results = results;
        }

        public IoCommand.Result getResult(int index) {
            return results.get(index);
        }

        @Override
        public void close() {
            for (int i = 0; i < statements.size(); i++) {
                try {
                    results.get(i).close();
                    statements.get(i).close();
                } catch (Exception e) {
                    Log.a(this.getClass()).error("Failed to close result/statement", e);
                }
            }
        }
    }

    // private void addForeignKeys(ConcreteSchemaNode schemaNode,
    // List<IoCommand.Row> keyColumnUsage,
    // List<IoCommand.Row> constraintColumnUsage) {
    // if (!Util.isEmpty(keyColumnUsage)) {

    // Map<String, List<IoCommand.Row>> foreignKeysByConstraintName = keyColumnUsage
    // .stream()
    // .filter(constraint -> constraint.get(POSITION_IN_UNIQUE_CONSTRAINT) != null)
    // .collect(Collectors.groupingBy(c -> (String) c.get(CONSTRAINT_NAME)));
    // foreignKeysByConstraintName.keySet().forEach(constraintName -> {
    // AtomicReference<String> fkTableName = new AtomicReference<>();
    // foreignKeysByConstraintName.get(constraintName)
    // .stream()
    // .filter(r -> r.get(POSITION_IN_UNIQUE_CONSTRAINT) != null)
    // .findFirst()
    // .ifPresent(r -> fkTableName.set((String) r.get(TABLE_NAME)));
    // Map<String, Property> properties = new HashMap<>();
    // List<IoCommand.Row> constraintUsage = constraintColumnUsage
    // .stream()
    // .filter(c -> c.get(CONSTRAINT_NAME).equals(constraintName))
    // .collect(Collectors.toList());
    // String pkTableName = (String) constraintUsage.get(0).get(TABLE_NAME);
    // String fkColumns = foreignKeysByConstraintName.get(constraintName)
    // .stream()
    // .map(r -> (String) r.get(COLUMN_NAME))
    // .collect(Collectors.joining(";"));
    // String pkColumns = keyColumnUsage.stream()
    // .filter(r -> r.get(TABLE_NAME).equals(pkTableName) &&
    // r.get(POSITION_IN_UNIQUE_CONSTRAINT) == null &&
    // foreignKeysByConstraintName.get(constraintName)
    // .stream()
    // .anyMatch(o -> o.get(POSITION_IN_UNIQUE_CONSTRAINT) ==
    // r.get(ORDINAL_POSITION)))
    // .sorted((o1, o2) -> {
    // Long o1OrdinalPositionInFk = getOrdinalPositionInFk(keyColumnUsage,
    // fkTableName.get(), o1);
    // Long o2OrdinalPositionInFk = getOrdinalPositionInFk(keyColumnUsage,
    // fkTableName.get(), o2);
    // return o1OrdinalPositionInFk.compareTo(o2OrdinalPositionInFk);
    // })
    // .map(o -> (String) o.get(COLUMN_NAME))
    // .collect(Collectors.joining(";"));
    // properties.put(ConcreteRefersToRelation.FkCategory.fkTableName.name(),
    // new PropertyImpl(PROPERTY + ":" +
    // ConcreteRefersToRelation.FkCategory.fkTableName.name(),
    // ConcreteRefersToRelation.FkCategory.fkTableName.name(), fkTableName.get(),
    // ConcreteRefersToRelation.FkCategory.fkTableName.name(), 1.0D, CRAWLER, ""));
    // properties.put(ConcreteRefersToRelation.FkCategory.pkTableName.name(),
    // new PropertyImpl(PROPERTY + ":" +
    // ConcreteRefersToRelation.FkCategory.pkTableName.name(),
    // ConcreteRefersToRelation.FkCategory.pkTableName.name(), pkTableName,
    // ConcreteRefersToRelation.FkCategory.pkTableName.name(), 1.0D, CRAWLER, ""));
    // properties.put(ConcreteRefersToRelation.FkCategory.fkColumnName.name(),
    // new PropertyImpl(PROPERTY + ":" +
    // ConcreteRefersToRelation.FkCategory.fkColumnName.name(),
    // ConcreteRefersToRelation.FkCategory.fkColumnName.name(), fkColumns,
    // ConcreteRefersToRelation.FkCategory.fkColumnName.name(), 1.0D, CRAWLER, ""));
    // properties.put(ConcreteRefersToRelation.FkCategory.pkColumnName.name(),
    // new PropertyImpl(PROPERTY + ":" +
    // ConcreteRefersToRelation.FkCategory.pkColumnName.name(),
    // ConcreteRefersToRelation.FkCategory.pkColumnName.name(), pkColumns,
    // ConcreteRefersToRelation.FkCategory.pkColumnName.name(), 1.0D, CRAWLER, ""));
    // schemaNode
    // .dataset(fkTableName.get())
    // .flatMap(contains -> contains.getNode().classNode(fkTableName.get()))
    // .ifPresent(fkTableClassNode -> schemaNode.dataset(pkTableName)
    // .flatMap(dataset -> dataset.getNode().classNode(pkTableName))
    // .ifPresent(pkTableClassNode -> ((ConcreteClassNode) pkTableClassNode)
    // .refersTo(fkTableClassNode, fkColumns, pkColumns, 1.0D, CRAWLER, "",
    // constraintName, properties)));
    // });
    // }
    // }

    // private long getOrdinalPositionInFk(List<IoCommand.Row> keyColumnUsage,
    // String tableName, IoCommand.Row pkRow) {
    // return ParamConvertor.toInteger(keyColumnUsage
    // .stream()
    // .filter(r -> r.get(TABLE_NAME).equals(tableName) &&
    // r.get(POSITION_IN_UNIQUE_CONSTRAINT) == pkRow.get(ORDINAL_POSITION))
    // .map(o -> o.get(ORDINAL_POSITION))
    // .collect(Collectors.toList())
    // .get(0));
    // }

    private static ObjectType convertFieldListToObjectType(FieldList fieldList, Field parent) {
        Properties properties = new Properties();
        for (Field field : fieldList) {
            String fieldName = field.getName();
            Schema fieldSchema = convertFieldToSchema(field);
            properties.put(fieldName, fieldSchema);
        }
        return new ObjectType(properties, parent == null ? null : parent.getType().getStandardType().name());
    }

    private static Schema convertFieldToSchema(Field field) {
        StandardSQLTypeName bqType = field.getType().getStandardType();
        Primitive asPrimitive = mapBigQueryTypeToPrimitive(bqType);

        if (field.getMode() == Field.Mode.REPEATED) {
            Schema itemSchema;
            if (bqType == StandardSQLTypeName.STRUCT) {
                itemSchema = convertFieldListToObjectType(field.getSubFields(), field);
            } else {
                itemSchema = asPrimitive;
            }
            return new ArrayType(itemSchema, "REPEATED " + bqType.name());
        } else if (bqType == StandardSQLTypeName.STRUCT) {
            return convertFieldListToObjectType(field.getSubFields(), field);
        } else {
            return asPrimitive;
        }
    }

    private static Primitive mapBigQueryTypeToPrimitive(StandardSQLTypeName bqType) {
        switch (bqType) {
            case ARRAY:
                return new Primitive(Type.array, bqType.name(), null);
            case BIGNUMERIC:
            case NUMERIC:
            case FLOAT64:
                return new Primitive(Type.real, bqType.name(), null);
            case BOOL:
                return new Primitive(Type.bool, bqType.name(), null);
            case BYTES:
                return new Primitive(Type.blob, bqType.name(), null);
            case DATE:
                return new Primitive(Type.date, bqType.name(), null);
            case INT64:
                return new Primitive(Type.integer, bqType.name(), null);
            case STRING:
            case DATETIME:
            case GEOGRAPHY:
            case INTERVAL:
            case JSON:
            case TIME:
            case TIMESTAMP:
            case RANGE:
                return new Primitive(Type.string, bqType.name(), null);
            default:
                return null;
        }
    }

    private String idPrefix(String prefix, ConcreteNode node) {
        return prefix + ":" + node.getId();
    }

    @Override
    public BigQuerySnapshot snapshotDataset(String dataset, String schema, SampleSize size, Map<String, Object> map) {
        return new BigQuerySnapshot(commandSession, readSession, dataset, schema, datasetsProjectId, size,
                snapshotViaStorage);
    }

    @Override
    public void close() throws Exception {
        if (this.selfCreatedCommandSession) {
            Util.safeClose(this.commandSession);
        }
        if (this.selfCreatedReadSession) {
            Util.safeClose(this.readSession);
        }
    }

    // @Override in 8.3
    public SnapshotDataset snapshotDataset(String arg0, String arg1, String arg2, SampleSize arg3,
            Map<String, Object> arg4) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'snapshotDataset'");
    }
}
