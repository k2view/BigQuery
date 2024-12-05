package com.k2view.cdbms.usercode.common.BigQuery.metadata;

import static com.k2view.discovery.crawl.JdbcIoMetadata.EXCLUDE_LIST;
import static com.k2view.discovery.crawl.JdbcIoMetadata.INCLUDE_LIST;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Lists;
import com.k2view.cdbms.usercode.common.BigQuery.BigQueryCommandIoSession;
import com.k2view.cdbms.usercode.common.BigQuery.BigQueryReadIoSession;
import com.k2view.discovery.Action;
import com.k2view.discovery.DiscoveryStatusUpdater;
import com.k2view.discovery.pipeline.PluginsPipeline;
import com.k2view.discovery.schema.io.IoMetadata;
import com.k2view.discovery.schema.model.Category;
import com.k2view.discovery.schema.model.DataPlatform;
import com.k2view.discovery.schema.model.Property;
import com.k2view.discovery.schema.model.impl.ConcreteClassNode;
import com.k2view.discovery.schema.model.impl.ConcreteDataPlatform;
import com.k2view.discovery.schema.model.impl.ConcreteDataset;
import com.k2view.discovery.schema.model.impl.ConcreteField;
import com.k2view.discovery.schema.model.impl.ConcreteNode;
import com.k2view.discovery.schema.model.impl.ConcreteRefersToRelation;
import com.k2view.discovery.schema.model.impl.ConcreteSchemaNode;
import com.k2view.discovery.schema.model.impl.PropertyImpl;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Time;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoCommand.Row;

public class BigQueryMetadata implements IoMetadata {
    private static final String SCHEMA = "schema";
    private static final String DATASET = "dataset";
    private static final String CLASS = "class";
    private static final String FIELD = "field";
    private static final String ENTITY_NAME = "entityName";
    private static final String CRAWLER = "Crawler";
    private static final String PROPERTY = "property";
    private static final String ORDINAL_POSITION = "ordinal_position";
    private static final String POSITION_IN_UNIQUE_CONSTRAINT = "position_in_unique_constraint";
    private static final String COLUMN_NAME = "column_name";
    private static final String CONSTRAINT_NAME = "constraint_name";
    private static final String TABLE_NAME = "table_name";
    private static final long MB = 1024 * 1024;
    private static final Map<String, Long> TYPE_TO_SIZE = createTypeToSizeMap();
    private static final Map<String, String> TYPE_MAPPING = javaToBqTypeMapping();
    private static final Map<String, Integer> BQ_TO_SQL_TYPES = createBQToSQLTypeMap();

    private final Log log = Log.a(this.getClass());
    private BigQueryCommandIoSession commandSession;
    private BigQueryReadIoSession readSession;
    private boolean selfCreatedCommandSession;
    private boolean selfCreatedReadSession;
    private final String interfaceName;

    private Set<String> schemasExclude = new HashSet<>();
    private Map<String, List<String>> tablesExclude = new HashMap<>();
    private Set<String> schemasInclude = new HashSet<>();
    private Map<String, List<String>> tablesInclude = new HashMap<>();
    private final String projectId;
    private final String jobUid;
    private AtomicInteger fieldsProgress = new AtomicInteger(0);

    // TO-DO replace with Util
    private Set<String> initSchemaWBList(List<String> items) {
        return items.stream()
                .filter(i -> !i.contains("."))
                .map(i -> i.split("\\.")[0])
                .collect(Collectors.toSet());
    }

    private static Map<String, Integer> createBQToSQLTypeMap() {
        Map<String, Integer> map = new HashMap<>();
        map.put("array", Types.ARRAY);
        map.put("bignumeric", Types.DECIMAL);
        map.put("bool", Types.BOOLEAN);
        map.put("bytes", Types.BINARY);
        map.put("date", Types.DATE);
        map.put("datetime", Types.TIMESTAMP);
        map.put("float64", Types.DOUBLE);
        map.put("geography", Types.VARCHAR);
        map.put("int64", Types.BIGINT);
        map.put("interval", Types.VARCHAR);
        map.put("json", Types.VARCHAR);
        map.put("numeric", Types.NUMERIC);
        map.put("string", Types.VARCHAR);
        map.put("struct", Types.STRUCT);
        map.put("time", Types.TIME);
        map.put("timestamp", Types.TIMESTAMP);
        return map;
    }

    // TO-DO replace with Util
    private Map<String, List<String>> initTableWBList(List<String> items) {
        return items.stream()
                .filter(i -> i.contains(".") && !i.endsWith(".*"))
                .collect(Collectors.groupingBy(schema -> {
                            String s = schema.split("\\.")[0];
                            return s.equals("*") ? "" : s;
                        },
                        Collectors.mapping(schema -> schema.split("\\.")[1], Collectors.toList())));
    }

    private Optional<List<String>> getNestedListFromProps(Map<String, Object> props, String parentKey, String nestedKey, String childKey) {
        return Optional.ofNullable(props.get(parentKey))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .map(parentMap -> parentMap.get(nestedKey))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .map(nestedMap -> nestedMap.get(childKey))
                .filter(List.class::isInstance)
                .map(List.class::cast);
    }

    public BigQueryMetadata(String credentialsFilePath, String interfaceName, BigQueryCommandIoSession commandIoSession, BigQueryReadIoSession readSession, String projectId, Map<String, Object> props) {
        this.interfaceName = interfaceName;
        this.projectId = projectId;
        Optional<List<String>> excludeListOptional = getNestedListFromProps(props, "data_platforms", this.interfaceName, EXCLUDE_LIST);
        Optional<List<String>> includeListOptional = getNestedListFromProps(props, "data_platforms", this.interfaceName, INCLUDE_LIST);
        includeListOptional.ifPresent(list -> {
            schemasInclude = initSchemaWBList(list);
            tablesInclude = initTableWBList(list);
        });
        excludeListOptional.ifPresent(list -> {
            schemasExclude = initSchemaWBList(list);
            tablesExclude = initTableWBList(list);
        });

        this.commandSession = commandIoSession;
        this.readSession = readSession;
        if (commandSession == null) {
            this.commandSession = new BigQueryCommandIoSession(interfaceName, credentialsFilePath, projectId);
            this.selfCreatedCommandSession = true;
        }
        if (readSession == null) {
            this.readSession = new BigQueryReadIoSession(interfaceName, projectId, credentialsFilePath);
            this.selfCreatedReadSession = true;
        }
        this.jobUid = ParamConvertor.toString(props.get("uuid"));
    }

    @Override
    public DataPlatform getDataPlatform() throws Exception {
        long startTime = Time.now();
        DiscoveryStatusUpdater.getInstance().setDiscoveryStatus("crawler", DiscoveryStatusUpdater.ALL, jobUid, Action.CREATE, PluginsPipeline.DURATION, 0);
        ConcreteDataPlatform dataPlatform = addPlatformNode(this.interfaceName);
        DiscoveryStatusUpdater.getInstance().setDiscoveryStatus("crawler", "", jobUid, Action.CREATE, PluginsPipeline.PROGRESS, 0);
        addSchemaNodes(dataPlatform);
        long endTime = Time.now();
        DiscoveryStatusUpdater.getInstance().setDiscoveryStatus("crawler", DiscoveryStatusUpdater.ALL, jobUid, Action.UPDATE, PluginsPipeline.DURATION, endTime - startTime);
        return dataPlatform;
    }

    private ConcreteDataPlatform addPlatformNode(String platform) {
        ConcreteDataPlatform dataPlatform = new ConcreteDataPlatform(platform);
        String idPrefix ="dataPlatform:" + dataPlatform.getId();
        dataPlatform.addProperty(idPrefix, ENTITY_NAME, "Data Platform Name", dataPlatform.getName(), 1.0, CRAWLER, "");
        dataPlatform.addProperty(idPrefix, "type", "Data Platform Type", "BigQuery", 1.0, CRAWLER, "");
        return dataPlatform;
    }

    private void addSchemaNodes(ConcreteDataPlatform dataPlatform) throws Exception {
        List<Object> statementParams = new LinkedList<>();
        statementParams.add(String.format("SELECT * EXCEPT (schema_owner) FROM %s.INFORMATION_SCHEMA.SCHEMATA", projectId));
        Set<String> schemasIncludeAllOrPartial =  Stream.concat(schemasInclude.stream(), tablesInclude.keySet().stream())
                .collect(Collectors.toSet());
        if (!Util.isEmpty(schemasIncludeAllOrPartial)) {
            statementParams.set(0, ((String)statementParams.get(0)).concat(" WHERE schema_name IN UNNEST (?)"));
            statementParams.add(schemasIncludeAllOrPartial);
        } else if(!Util.isEmpty(schemasExclude)){
            statementParams.set(0, ((String)statementParams.get(0)).concat(" WHERE schema_name NOT IN UNNEST (?)"));
            statementParams.add(schemasExclude);
        }
        try (IoCommand.Statement statement = this.commandSession.statement()) {
            IoCommand.Result schemas = statement.execute(statementParams.toArray());
            StreamSupport.stream(schemas.spliterator(), true).forEach(schema -> {
                String schemaName = schema.get("schema_name").toString();
                ConcreteSchemaNode schemaNode = new ConcreteSchemaNode(schemaName);
                dataPlatform.contains(schemaNode, 1.0, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), ENTITY_NAME, "Name of the schema (/BQ-Dataset)", schemaName, 1.0, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "projectId", "The name of the catalog (/BQ-Project) that contains the schema(/BQ-Dataset)", schema.get("catalog_name"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "creationTime", "The BQ dataset's creation time", schema.get("creation_time"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "lastModifiedTime", "The BQ dataset's last modified time", schema.get("last_modified_time"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "location", "The BQ dataset's geographic location", schema.get("location"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "ddl", "The CREATE SCHEMA DDL statement that can be used to create the BQ dataset", schema.get("ddl"), 1, CRAWLER, "");
                schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), "defaultCollationName", "The name of the default collation specification if it exists; otherwise, NULL.", String.valueOf(schema.get("default_collation_name")), 1, CRAWLER, "");
                try {
                    addDatasetNodes(schemaNode);
                } catch (Exception e) {
                    log.error("Failed to add dataset nodes for schema={}. Exception caught:", schemaNode.getName());
                    log.error(e);
                }
            });
        }
    }

    private void addDatasetNodes(ConcreteSchemaNode schemaNode) throws Exception {
        IoCommand.Statement tablesStatement = commandSession.statement();
        IoCommand.Statement columnsStatement = commandSession.statement();
        IoCommand.Statement keyColUsageStatement = commandSession.statement();
        IoCommand.Statement constraintColUsageStatement = commandSession.statement();

        IoCommand.Result[] results = this.execSchemaQueries(schemaNode, tablesStatement, columnsStatement, keyColUsageStatement, constraintColUsageStatement);
        IoCommand.Result tables = results[0];
        IoCommand.Result tablesColumns = results[1];
        IoCommand.Result keyColumnUsage = results[2];
        IoCommand.Result constraintColumnUsage = results[3];
        Map<String, List<IoCommand.Row>> tableKeyColumnUsageMap = new HashMap<>();
        Map<String, List<IoCommand.Row>> tableColumnsMap = new HashMap<>();
        keyColumnUsage.forEach(usage -> {
            String tableName = (String) usage.get(TABLE_NAME);
            tableKeyColumnUsageMap.computeIfAbsent(tableName, key -> new LinkedList<>());
            tableKeyColumnUsageMap.get(tableName).add(usage);
        });
        int fieldsCnt = 0;
        for (Row r : tablesColumns) {
            String tableName = (String) r.get(TABLE_NAME);
            tableColumnsMap.computeIfAbsent(tableName, key -> new LinkedList<>());
            tableColumnsMap.get(tableName).add(r);
            fieldsCnt++;
        }
        DiscoveryStatusUpdater.getInstance().setDiscoveryStatus("crawler", DiscoveryStatusUpdater.ALL, jobUid, Action.CREATE, PluginsPipeline.TOTAL, fieldsCnt);

        for (IoCommand.Row row : tables) {
            String tableName = row.get(TABLE_NAME).toString();
            ConcreteClassNode datasetClassNode = new ConcreteClassNode(tableName);
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), ENTITY_NAME, "Name of the table", tableName, 1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "tableType", "The table type (BASE TABLE/CLONE/SNAPSHOT/VIEW/MATERIALIZED VIEW/EXTERNAL)", row.get("table_type"), 1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "creationTime", "The table's creation time", row.get("creation_time"), 1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "ddl", "The DDL statement that can be used to recreate the table, such as CREATE TABLE or CREATE VIEW", row.get("ddl"), 1.0, CRAWLER, "");
            datasetClassNode.addProperty(idPrefix(CLASS, datasetClassNode), "defaultCollationName", "The name of the default collation specification if it exists; otherwise, NULL", String.valueOf(row.get("default_collation_name")), 1.0, CRAWLER, "");

            ConcreteDataset datasetNode = new ConcreteDataset(tableName);
            datasetNode.definedBy(datasetClassNode, 1.0, CRAWLER, "");
            datasetNode.addProperty(this.idPrefix(DATASET, datasetNode), ENTITY_NAME, "Name of the table", tableName, 1.0, CRAWLER, "");
            schemaNode.contains(datasetNode, 1.0, CRAWLER, "");

            addFieldNodes(datasetClassNode,
                    tableColumnsMap.get(tableName),
                    tableKeyColumnUsageMap.getOrDefault(tableName, new LinkedList<>())
                            .stream()
                            .filter(constraint -> constraint.get(POSITION_IN_UNIQUE_CONSTRAINT) == null)
                            .collect(Collectors.toList()));
        }
        // Add refersTo to every table referenced by a Foreign Key constraint in another one
        addForeignKeys(schemaNode,
                Lists.newArrayList(keyColumnUsage),
                Lists.newArrayList(constraintColumnUsage));
        columnsStatement.close();
        constraintColUsageStatement.close();
        keyColUsageStatement.close();
        tablesStatement.close();
    }

    private IoCommand.Result[] execSchemaQueries(ConcreteSchemaNode schemaNode, IoCommand.Statement tablesStatement, IoCommand.Statement columnsStatement, IoCommand.Statement keyColUsageStatement, IoCommand.Statement constraintColUsageStatement) throws InterruptedException {
        AtomicReference<IoCommand.Result> tables = new AtomicReference<>();
        AtomicReference<IoCommand.Result> tablesColumns = new AtomicReference<>();
        AtomicReference<IoCommand.Result> keyColumnUsage = new AtomicReference<>();
        AtomicReference<IoCommand.Result> constraintColumnUsage = new AtomicReference<>();
        Thread tablesTh = Util.thread(() -> {
            List<Object> statementParams = new LinkedList<>();
            statementParams.add(String.format(
                    "SELECT * FROM %s.%s.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE!='VIEW'",
                    projectId,
                    schemaNode.getName()));
            String schemaName = schemaNode.getName();
            if (tablesInclude.containsKey(schemaName)) {
                statementParams.set(0, ((String)statementParams.get(0)).concat(" AND table_name IN UNNEST (?)"));
                statementParams.add(tablesInclude.get(schemaName));
            } else if (tablesExclude.containsKey(schemaName)) {
                statementParams.set(0, ((String)statementParams.get(0)).concat(" AND table_name NOT IN UNNEST (?)"));
                statementParams.add(tablesExclude.get(schemaName));
            }
            try {
                tables.set(tablesStatement.execute(statementParams.toArray()));
            } catch (Exception e) {
                log.error(e);
            }
        });
        Thread columnsTh = Util.thread(() -> {
            try {
                tablesColumns.set(columnsStatement.execute(
                        String.format(
                                "SELECT * EXCEPT(is_generated, generation_expression, is_stored, is_updatable) FROM %s.%s.INFORMATION_SCHEMA.COLUMNS",
                                projectId,
                                schemaNode.getName())));
            } catch (Exception e) {
                log.error(e);
            }
        });
        Thread keyColUsageTh = Util.thread(() -> {
            // Contains info about PKs and partial info about FKs
            try {
                keyColumnUsage.set(keyColUsageStatement.execute(
                        String.format(
                                "SELECT * FROM %s.%s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
                                projectId,
                                schemaNode.getName())));
            } catch (Exception e) {
                log.error(e);
            }
        });
        Thread colUsageTh = Util.thread(() -> {
            try {
                // To get the referenced tables in FKs
                constraintColumnUsage.set(constraintColUsageStatement.execute(String.format(
                        "SELECT * FROM %s.%s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE",
                        projectId,
                        schemaNode.getName())));
            } catch (Exception e) {
                log.error(e);
            }
        });
        tablesTh.join();
        columnsTh.join();
        keyColUsageTh.join();
        colUsageTh.join();
        return new IoCommand.Result[]{tables.get(), tablesColumns.get(), keyColumnUsage.get(), constraintColumnUsage.get()};
    }

    private void addForeignKeys(ConcreteSchemaNode schemaNode, List<IoCommand.Row> keyColumnUsage, List<IoCommand.Row> constraintColumnUsage) {
        if (!Util.isEmpty(keyColumnUsage)) {

            Map<String, List<IoCommand.Row>> foreignKeysByConstraintName =
                    keyColumnUsage
                            .stream()
                            .filter(constraint -> constraint.get(POSITION_IN_UNIQUE_CONSTRAINT) != null)
                            .collect(Collectors.groupingBy(c -> (String) c.get(CONSTRAINT_NAME)));
            foreignKeysByConstraintName.keySet().forEach(constraintName -> {
                AtomicReference<String> fkTableName = new AtomicReference<>();
                foreignKeysByConstraintName.get(constraintName)
                        .stream()
                        .filter(r -> r.get(POSITION_IN_UNIQUE_CONSTRAINT) != null)
                        .findFirst()
                        .ifPresent(r -> fkTableName.set((String) r.get(TABLE_NAME)));
                Map<String, Property> properties = new HashMap<>();
                List<IoCommand.Row> constraintUsage = constraintColumnUsage
                        .stream()
                        .filter(c -> c.get(CONSTRAINT_NAME).equals(constraintName))
                        .collect(Collectors.toList());
                String pkTableName = (String) constraintUsage.get(0).get(TABLE_NAME);
                String fkColumns = foreignKeysByConstraintName.get(constraintName)
                        .stream()
                        .map(r -> (String) r.get(COLUMN_NAME))
                        .collect(Collectors.joining(";"));
                String pkColumns = keyColumnUsage.stream()
                        .filter(r -> r.get(TABLE_NAME).equals(pkTableName) &&
                                        r.get(POSITION_IN_UNIQUE_CONSTRAINT) == null &&
                                        foreignKeysByConstraintName.get(constraintName)
                                            .stream()
                                            .anyMatch(o ->
                                                    o.get(POSITION_IN_UNIQUE_CONSTRAINT) == r.get(ORDINAL_POSITION)
                                            )
                        )
                        .sorted((o1, o2) -> {
                            Long o1OrdinalPositionInFk = getOrdinalPositionInFk(keyColumnUsage, fkTableName.get(), o1);
                            Long o2OrdinalPositionInFk = getOrdinalPositionInFk(keyColumnUsage, fkTableName.get(), o2);
                            return o1OrdinalPositionInFk.compareTo(o2OrdinalPositionInFk);
                        })
                        .map(o -> (String) o.get(COLUMN_NAME))
                        .collect(Collectors.joining(";"));
                properties.put(ConcreteRefersToRelation.FkCategory.fkTableName.name(), new PropertyImpl(PROPERTY + ":" + ConcreteRefersToRelation.FkCategory.fkTableName.name(), ConcreteRefersToRelation.FkCategory.fkTableName.name(), fkTableName.get(), ConcreteRefersToRelation.FkCategory.fkTableName.name(), 1.0D, CRAWLER, ""));
                properties.put(ConcreteRefersToRelation.FkCategory.pkTableName.name(), new PropertyImpl(PROPERTY + ":" + ConcreteRefersToRelation.FkCategory.pkTableName.name(), ConcreteRefersToRelation.FkCategory.pkTableName.name(), pkTableName, ConcreteRefersToRelation.FkCategory.pkTableName.name(), 1.0D, CRAWLER, ""));
                properties.put(ConcreteRefersToRelation.FkCategory.fkColumnName.name(), new PropertyImpl(PROPERTY + ":" + ConcreteRefersToRelation.FkCategory.fkColumnName.name(), ConcreteRefersToRelation.FkCategory.fkColumnName.name(), fkColumns, ConcreteRefersToRelation.FkCategory.fkColumnName.name(), 1.0D, CRAWLER, ""));
                properties.put(ConcreteRefersToRelation.FkCategory.pkColumnName.name(), new PropertyImpl(PROPERTY + ":" + ConcreteRefersToRelation.FkCategory.pkColumnName.name(), ConcreteRefersToRelation.FkCategory.pkColumnName.name(), pkColumns, ConcreteRefersToRelation.FkCategory.pkColumnName.name(), 1.0D, CRAWLER, ""));
                schemaNode
                        .dataset(fkTableName.get())
                        .flatMap(contains -> contains.getNode().classNode(fkTableName.get()))
                        .ifPresent(fkTableClassNode ->
                                schemaNode.dataset(pkTableName)
                                        .flatMap(dataset -> dataset.getNode().classNode(pkTableName))
                                        .ifPresent(pkTableClassNode -> ((ConcreteClassNode) pkTableClassNode)
                                                .refersTo(fkTableClassNode, fkColumns, pkColumns, 1.0D, CRAWLER, "", constraintName, properties)
                                        ));
            });
        }
    }

    private long getOrdinalPositionInFk(List<IoCommand.Row> keyColumnUsage, String tableName, IoCommand.Row pkRow) {
        return ParamConvertor.toInteger(keyColumnUsage
                .stream()
                .filter(r ->
                        r.get(TABLE_NAME).equals(tableName) &&
                                r.get(POSITION_IN_UNIQUE_CONSTRAINT) == pkRow.get(ORDINAL_POSITION))
                .map(o -> o.get(ORDINAL_POSITION))
                .collect(Collectors.toList())
                .get(0));
    }

    private void addFieldNodes(ConcreteClassNode tableClassNode, List<IoCommand.Row> tableColumns, List<IoCommand.Row> uniqueConstraints) {
        tableColumns.forEach(column -> {
            String dataType = (String) column.get("data_type");
            ConcreteField fieldNode = new ConcreteField(column.get(COLUMN_NAME).toString());
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sourceDataType.name(), "Column type", dataType , 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sourceNullable.name(), "Nullability of the field 1 or 0", column.get("is_nullable"), 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sourceEntityType.name(), "Role", "Column", 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.definedBy.name(), "Data type for field", TYPE_MAPPING.getOrDefault(dataType.toLowerCase(), "Unknown"),1.0, CRAWLER,"");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.columnSize.name(), "Max column size in bytes", TYPE_TO_SIZE.getOrDefault(dataType.toLowerCase(), Long.MAX_VALUE), 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.ordinalPosition.name(), "Ordinal position", column.get("ordinal_position"), 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sqlDataType.name(), "SQL data type", BQ_TO_SQL_TYPES.getOrDefault(dataType, Types.VARCHAR), 1.0, CRAWLER, "");

            if (!Util.isEmpty(uniqueConstraints)) {
                uniqueConstraints
                    .stream()
                    .filter(constraint ->
                            constraint.get(COLUMN_NAME).equals(column.get(COLUMN_NAME)))
                    .findFirst()
                    .ifPresent(constraint -> fieldNode.addProperty(
                            this.idPrefix(FIELD, fieldNode),
                            "pk",
                            "Primary Key (Unenforced)",
                            true,
                            1.0,
                            CRAWLER,
                            ""));
            }
            tableClassNode.contains(fieldNode, 1.0, CRAWLER, "");
        });
        Util.rte(() -> DiscoveryStatusUpdater.getInstance().setDiscoveryStatus("crawler", "", jobUid, Action.UPDATE, PluginsPipeline.PROGRESS, fieldsProgress.addAndGet(tableColumns.size())));
    }

    private static Map<String, String> javaToBqTypeMapping() {
        Map<String, String> typeMapping = new HashMap<>();
        typeMapping.put("array", "Iterable");
        typeMapping.put("bignumeric", "BigDecimal");
        typeMapping.put("bool", "Boolean");
        typeMapping.put("bytes", "byte[]");
        typeMapping.put("date", "LocalDate");
        typeMapping.put("datetime", "LocalDateTime");
        typeMapping.put("float64", "Float");
        typeMapping.put("geography", "String");
        typeMapping.put("int64", "Integer");
        typeMapping.put("interval", "String");
        typeMapping.put("json", "JsonObject");
        typeMapping.put("numeric", "BigDecimal");
        typeMapping.put("string", "String");
        typeMapping.put("struct", "Map");
        typeMapping.put("time", "LocalTime");
        typeMapping.put("timestamp", "Instant");
        return typeMapping;
    }

    private static Map<String, Long> createTypeToSizeMap() {
        Map<String, Long> map = new HashMap<>();
        map.put("array", 16L * MB);
        map.put("bignumeric", 16L);
        map.put("bool", 1L);
        map.put("bytes", 16L * MB);
        map.put("date", 3L);
        map.put("datetime", 8L);
        map.put("float64", 8L);
        map.put("geography", 1L * MB);
        map.put("int64", 8L);
        map.put("interval", 16L);
        map.put("json", 16L * MB);
        map.put("numeric", 16L);
        map.put("string", 10L * MB);
        map.put("struct", 16L * MB);
        map.put("time", 6L);
        map.put("timestamp", 6L);
        return map;
    }

    private String idPrefix(String prefix, ConcreteNode node) {
        return prefix + ":" + node.getId();
    }


    @Override
    public BigQuerySnapshot snapshotDataset(String dataset, String schema, SampleSize size, Map<String, Object> map) {
        return new BigQuerySnapshot(commandSession, readSession, dataset, schema, size);
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
}
