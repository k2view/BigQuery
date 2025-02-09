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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValue;
import com.k2view.cdbms.func.oracle.OracleRownum;
import com.k2view.cdbms.shared.utils.UserCodeDescribe.*;
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
        Field field = Field.of(fieldName, StandardSQLTypeName.valueOf(type));
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
}
