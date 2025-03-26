/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.BigQuery;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.*;
import java.math.*;
import java.io.*;

import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.shared.user.UserCode;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.cdbms.func.oracle.OracleToDate;
import com.k2view.cdbms.interfaces.GenericInterface;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValue;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.fabricdb.datachange.TableDataChange;

import static com.k2view.cdbms.shared.user.ProductFunctions.*;
import static com.k2view.cdbms.shared.user.UserCode.*;
import static com.k2view.cdbms.shared.utils.UserCodeDescribe.FunctionType.*;
import static com.k2view.cdbms.usercode.common.BigQuery.BigQueryParamParser.parseBqValue;

@SuppressWarnings({"all"})
public class SharedLogic {
	@type(CustomIoProvider)
	@out(name = "ioProvider", type = IoProvider.class, desc = "")
	public static IoProvider bigQueryIoProvider() throws Exception {
		return new BigQueryIoProvider();
	}

    public static Object bqParseTdmQueryParam(String fieldName, String value, String type) {
        // Assuming value is primitive for the sake of select queries built by TDM
        Field field = Field.of(fieldName, mapToStandardSQLType(type));
        FieldValue fieldValue = FieldValue.of(Attribute.PRIMITIVE, value);
        return parseBqValue(field, fieldValue, false);
    }

    public static String bqReplaceFilterPlaceholders(String filter, Iterable<Object> values) {
        if (filter == null || values == null) {
            throw new IllegalArgumentException("Filter and values cannot be null");
        }

        Iterator<Object> iterator = values.iterator();
        StringBuilder result = new StringBuilder();
        Matcher matcher = Pattern.compile("\\?").matcher(filter);

        while (matcher.find()) {
            if (!iterator.hasNext()) {
                throw new IllegalArgumentException("Not enough values provided for placeholders");
            }

            Object value = iterator.next();
            String replacement;

            if (value == null) {
                replacement = "NULL";
            } else if (value instanceof String) {
                replacement = "'" + value.toString().replace("'", "''") + "'"; // Escape single quotes
            } else {
                replacement = value.toString();
            }
            matcher.appendReplacement(result, replacement);
        }

        matcher.appendTail(result);

        if (iterator.hasNext()) {
            throw new IllegalArgumentException("Too many values provided for placeholders");
        }

        return result.toString();
    }

    /**
     * Converts INFORMATION_SCHEMA `data_type` into StandardSQLTypeName.
     */
    private static StandardSQLTypeName mapToStandardSQLType(String type) {
        // Remove precision/length specifiers, e.g., "STRING(10)" → "STRING"
        String baseType = type.replaceAll("\\(.*\\)", "").toUpperCase();
    
        // Handle special case: ARRAY<TYPE>
        if (baseType.startsWith("ARRAY<")) {
            return StandardSQLTypeName.ARRAY;
        }
    
        return switch (baseType) {
            case "STRING" -> StandardSQLTypeName.STRING;
            case "INT64", "INTEGER" -> StandardSQLTypeName.INT64;
            case "FLOAT64", "FLOAT" -> StandardSQLTypeName.FLOAT64;
            case "BOOLEAN", "BOOL" -> StandardSQLTypeName.BOOL;
            case "DATETIME" -> StandardSQLTypeName.DATETIME;
            case "TIMESTAMP" -> StandardSQLTypeName.TIMESTAMP;
            case "DATE" -> StandardSQLTypeName.DATE;
            case "NUMERIC" -> StandardSQLTypeName.NUMERIC;
            case "BIGNUMERIC" -> StandardSQLTypeName.BIGNUMERIC;
            case "BYTES" -> StandardSQLTypeName.BYTES;
            default -> throw new IllegalArgumentException("Unsupported BigQuery type: " + type);
        };
    }

    public static Iterable<Map<String, Object>> bqParentRowsMapper(String lu, String table, Iterable<Map<String, Object>> parentRows) {
        LUType luType = LUType.getTypeByName(lu);
        if (!table.equalsIgnoreCase(luType.rootObjectName)) {
            return parentRows;
        }
        LudbColumn ludbEntityIDColumnObject = luType.ludbEntityIDColumnObject;
        String iidColName = ludbEntityIDColumnObject.originColumnName;
        Map<String, Object> row = parentRows.iterator().next();
        Object oldVal = row.get(iidColName);
        Object newVal;
        switch (ludbEntityIDColumnObject.originalColumnDataType.toUpperCase()) {
            case "INTEGER" -> newVal = ParamConvertor.toInteger(oldVal);
            case "REAL" -> newVal = ParamConvertor.toReal(oldVal);
            case "DATETIME", "DATE", "TIME" -> newVal = ParamConvertor.toDate(oldVal);
            case "BLOB" -> newVal = ParamConvertor.toBuffer(oldVal);
            case "TEXT" -> newVal = ParamConvertor.toString(oldVal);
            default -> newVal = oldVal;
        }
        row.replace(iidColName, newVal);
        return List.of(row);
    }

    public static String bqGetDatasetsProject(String interfaceName, String env) {
        return ((GenericInterface) InterfacesManager.getInstance().getTypedInterface(interfaceName, env)).getProperty(BigQueryIoProvider.SESSION_PROP_DATASETS_PROJECT);
    }
    

}
