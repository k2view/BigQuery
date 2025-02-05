package com.k2view.cdbms.usercode.common.BigQuery;

import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.k2view.discovery.plugins.complexfield.ComplexFieldPlugin;
import com.k2view.discovery.schema.model.Category;
import com.k2view.discovery.schema.model.Property;
import com.k2view.discovery.schema.model.impl.ConcreteClassNode;
import com.k2view.discovery.schema.model.impl.ConcreteField;
import com.k2view.discovery.schema.model.impl.ConcreteNode;
import com.k2view.discovery.schema.model.types.UnknownClass;
import com.k2view.discovery.schema.model.impl.PropertyImpl;
import com.k2view.broadway.metadata.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class DatasetFieldsBuilder {
    private static final String CRAWLER = "Crawler";
    private static final String FIELD = "field";
    private static final String ENTITY_NAME = "entityName";
    private static final String INNER_CLASS_DELIMITER = ";";

    /**
     * Builds nodes by processing the provided ObjectType schema and mapping it to the given ConcreteClassNode.
     *
     * @param datasetClass the target {@link ConcreteClassNode} to populate with fields and properties.
     * @param objSchema the {@link ObjectType} schema to process and map into nodes.
     * @param schemaConsumer a {@link Consumer} that allows customization of how properties are added
     *                       to the {@link ConcreteField} or {@link ConcreteClassNode}.
     *                       This is invoked for each schema property to allow external logic to determine
     *                       how fields are handled.
     */
    public static void fromObjectSchema(ConcreteClassNode datasetClass, ObjectType objSchema, Consumer<SchemaPropertyContext> schemaContextConsumer) {
        processObject(datasetClass, objSchema, true, 0, schemaContextConsumer);
    }

    private static void processObject(
            ConcreteClassNode classNode,
            ObjectType objType,
            boolean isTopLevel,
            int collectionDepth,
            Consumer<SchemaPropertyContext> schemaConsumer) {
        final AtomicInteger i = new AtomicInteger(0);
        objType.properties().forEach((innerFieldName, innerFieldSchema) -> {
            ConcreteField innerField = new ConcreteField(innerFieldName, 1.0, CRAWLER, "", "Field name", innerFieldName);
            String idPrefix = FIELD + ":" + innerField.getId();
            Type type = innerFieldSchema.type();

            // Delegate property customization to schemaConsumer
            SchemaPropertyContext context = new SchemaPropertyContext(innerField, innerFieldSchema, idPrefix, isTopLevel, i.getAndIncrement());
            schemaConsumer.accept(context);

            if (type == Type.array) {
                processArray(innerField, (ArrayType) innerFieldSchema, isTopLevel, collectionDepth + 1, classNode.getId(), schemaConsumer);
            } else if (type == Type.object) {
                String fieldClass = ComplexFieldPlugin.createClassName(innerFieldName);
                String innerClassId = isTopLevel ? fieldClass : classNode.getId() + INNER_CLASS_DELIMITER + fieldClass;
                ConcreteClassNode innerClassNode = new ConcreteClassNode(innerClassId, 1.0, CRAWLER, "", "Field", innerClassId);
                innerClassNode.addProperty(innerClassId, ENTITY_NAME, "", innerClassId, 1.0, CRAWLER, "");
                innerField.definedBy(innerClassNode, 1.0, CRAWLER, "", definedByProperties(innerFieldName, innerClassId));
                processObject(innerClassNode, (ObjectType) innerFieldSchema, false, collectionDepth, schemaConsumer);
            }

            classNode.contains(innerField, 1.0, CRAWLER, "");
        });
    }

    private static void processArray(
            ConcreteField field,
            Schema fieldSchema,
            boolean isTopLevel,
            int collectionDepth,
            String fieldParentId,
            Consumer<SchemaPropertyContext> schemaConsumer) {
        Schema itemsSchema = fieldSchema.items();
        Type itemsType = itemsSchema.type();

        if (itemsType.isPrimitive()) {
            String fieldType = "Collection(".repeat(collectionDepth) + itemsType.name() + ")".repeat(collectionDepth);
            field.addProperty(idPrefix(FIELD, field), Category.definedBy.name(), "Data type for field", fieldType,1.0, CRAWLER,"");
        } else if (itemsType == Type.array) {
            processArray(field, itemsSchema.items(), isTopLevel, collectionDepth + 1, fieldParentId, schemaConsumer);
        } else if (itemsType == Type.object) {
            String fieldClass = ComplexFieldPlugin.createClassName(field.getId());
            String innerClassId = isTopLevel ? fieldClass : fieldParentId + INNER_CLASS_DELIMITER + fieldClass;
            innerClassId = "Collection(".repeat(collectionDepth) + innerClassId + ")".repeat(collectionDepth);
            ConcreteClassNode innerClassNode = new ConcreteClassNode(innerClassId, 1.0, CRAWLER, "", "Field", innerClassId);
            innerClassNode.addProperty(innerClassId, ENTITY_NAME, "", innerClassId, 1.0, CRAWLER, "");
            field.definedBy(innerClassNode, 1.0, CRAWLER, "", definedByProperties(field.getName(), innerClassId));
            processObject(innerClassNode, (ObjectType) itemsSchema, false, 0, schemaConsumer);
        }
    }

    private static Map<String, Property> definedByProperties(String fieldName, String className) {
        return Map.of(
                "fieldName",
                new PropertyImpl(
                        "property:fieldName",
                        "",
                        fieldName,
                        "fieldName",
                        1.0,
                        CRAWLER,
                        ""
                ),
                "className",
                new PropertyImpl(
                        "property:className",
                        "",
                        className,
                        "className",
                        1.0,
                        CRAWLER,
                        ""
                )
        );
    }

    private static String idPrefix(String prefix, ConcreteNode node) {
        return prefix + ":" + node.getId();
    }

    public static record SchemaPropertyContext(
            ConcreteField field,
            Schema schema,
            String idPrefix,
            boolean isTopLevel,
            int ordinalPosition
    ) {}
}
