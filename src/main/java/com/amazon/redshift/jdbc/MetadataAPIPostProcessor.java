/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.*;
import com.amazon.redshift.logger.RedshiftLogger;

import java.sql.*;
import java.util.*;

public class MetadataAPIPostProcessor extends MetadataAPIHelper {

    public MetadataAPIPostProcessor(RedshiftConnectionImpl connection) {
        super(connection);
        this.connection = connection;
    }

    protected final RedshiftConnectionImpl connection; // The connection association


    // Post-processing for metadata API getCatalogs()
    protected ResultSet getCatalogsPostProcessing(List<String> serverResults) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getCatalogsPostProcessing");
        }
        if(serverResults == null){
            return createRs(GET_CATALOGS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> catalogTuples = new ArrayList<>();

        try {
            for (String database_name : serverResults) {
                byte[][] tuple = getEmptyTuple(GET_CATALOGS_COLS, GET_CATALOGS_COLS.length);

                // Apply the post-processing
                tuple[GetCatalogs_Metadata.TABLE_CAT.getIndex()] = encodeStr(database_name);

                // Add the data row into the tuple list
                catalogTuples.add(new Tuple(tuple));
            }

            return createRs(GET_CATALOGS_COLS, catalogTuples);

        } catch (SQLException e) {
            if (RedshiftLogger.isEnable()) {
                RedshiftLogger.getDriverLogger().logError(e);
            }
            throw e;
        }
    }

    // Post-processing for metadata API getSchemas()
    protected ResultSet getSchemasPostProcessing(List<ShowSchemasInfo> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getSchemasPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_SCHEMAS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> schemaTuples = new ArrayList<>();


        // Loop through all the ResultSet received from Server API call and apply post-processing to match the JDBC metadata API spec
        for (ShowSchemasInfo resultSet : serverResultSets) {
            byte[][] tuple = getEmptyTuple(GET_SCHEMAS_COLS, GET_SCHEMAS_COLS.length);

            // Apply the post-processing
            tuple[GetSchemas_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(resultSet.getSchemaName());
            tuple[GetSchemas_Metadata.TABLE_CATALOG.getIndex()] = encodeStr(resultSet.getDatabaseName());

            // Add the data row into the tuple list
            schemaTuples.add(new Tuple(tuple));
        }
        return createRs(GET_SCHEMAS_COLS, schemaTuples);
    }

    // Post-processing for metadata API getTables()
    protected ResultSet getTablesPostProcessing(List<ShowTablesInfo> serverResultSets, String[] types) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getTablesPostProcessing");
        }
        // Directly return empty ResultSet
        if (serverResultSets == null || serverResultSets.isEmpty()) {
            return createRs(GET_TABLES_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> final_data = new ArrayList<>();

        // Create table types filter
        HashSet<String> typeSet = null;
        if (types != null) {
            typeSet = new HashSet<>(Arrays.asList((types)));
        }

        // Loop through all the ResultSet received from Server API call and apply post-processing to match the JDBC metadata API spec
        for (ShowTablesInfo resultSet : serverResultSets) {
            if (types == null || typeSet.contains(resultSet.getTableType())) {
                byte[][] tuple = getEmptyTuple(GET_TABLES_COLS, GET_TABLES_COLS.length);

                // Apply the post-processing
                tuple[GetTables_Metadata.TABLE_CAT.getIndex()] = encodeStr(resultSet.getDatabaseName());
                tuple[GetTables_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(resultSet.getSchemaName());
                tuple[GetTables_Metadata.TABLE_NAME.getIndex()] = encodeStr(resultSet.getTableName());
                tuple[GetTables_Metadata.TABLE_TYPE.getIndex()] = encodeStr(resultSet.getTableType());
                tuple[GetTables_Metadata.REMARKS.getIndex()] = encodeStr(resultSet.getRemarks());
                tuple[GetTables_Metadata.OWNER.getIndex()] = encodeStr(resultSet.getOwner());
                tuple[GetTables_Metadata.LAST_ALTERED_TIME.getIndex()] = encodeStr(resultSet.getLastAlteredTime());
                tuple[GetTables_Metadata.LAST_MODIFIED_TIME.getIndex()] = encodeStr(resultSet.getLastModifiedTime());
                tuple[GetTables_Metadata.DIST_STYLE.getIndex()] = encodeStr(resultSet.getDistStyle());
                tuple[GetTables_Metadata.TABLE_SUBTYPE.getIndex()] = encodeStr(resultSet.getTableSubtype());

                // Add the data row into the tuple list
                final_data.add(new Tuple(tuple));
            }
        }
        return createRs(GET_TABLES_COLS, final_data);
    }

    // Post-processing for metadata API getColumns()
    protected ResultSet getColumnsPostProcessing(List<ShowColumnsInfo> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getColumnsPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_COLUMNS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> columnTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing to match the JDBC metadata API spec
        for (ShowColumnsInfo resultSet : serverResultSets) {
            String dataType = resultSet.getDataType();
            String rsType;

            // Retrieve customize precision from DATETIME/INTERVAL data type
            int precisions = 0;
            boolean dateTimeCustomizePrecision = false;
            if (dataType.matches(DATETIME_PRECISION_PATTERN) || dataType.matches(INTERVAL_PRECISION_PATTERN)) {
                rsType = getRSType(dataType.replaceAll(PRECISION_REMOVAL_PATTERN, "").replaceFirst(TRAILING_SPACES_PATTERN, ""));
                precisions = Integer.parseInt(dataType.replaceAll(PRECISION_EXTRACTION_PATTERN, "$1"));
                dateTimeCustomizePrecision = true;
            } else {
                rsType = getRSType(dataType);
            }

            String sqlType = getSQLType(rsType);
            String autoIncrement = getAutoIncrement(resultSet.getColumnDefault());

            byte[][] tuple = getEmptyTuple(GET_COLUMNS_COLS, GET_COLUMNS_COLS.length);

            // Apply the post-processing
            tuple[GetColumns_Metadata.TABLE_CAT.getIndex()] = encodeStr(resultSet.getDatabaseName());
            tuple[GetColumns_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(resultSet.getSchemaName());
            tuple[GetColumns_Metadata.TABLE_NAME.getIndex()] = encodeStr(resultSet.getTableName());
            tuple[GetColumns_Metadata.COLUMN_NAME.getIndex()] = encodeStr(resultSet.getColumnName());
            tuple[GetColumns_Metadata.DATA_TYPE.getIndex()] = encodeStr(sqlType);
            tuple[GetColumns_Metadata.TYPE_NAME.getIndex()] = encodeStr(rsType);
            tuple[GetColumns_Metadata.COLUMN_SIZE.getIndex()] = encodeStr(getColumnSize(
                    rsType,
                    resultSet.getCharacterMaximumLength(),
                    resultSet.getNumericPrecision()));
            tuple[GetColumns_Metadata.BUFFER_LENGTH.getIndex()] = null;                    // Unused
            tuple[GetColumns_Metadata.DECIMAL_DIGITS.getIndex()] = encodeStr(getDecimalDigit(
                    rsType,
                    resultSet.getNumericScale(),
                    precisions,
                    dateTimeCustomizePrecision));
            tuple[GetColumns_Metadata.NUM_PREC_RADIX.getIndex()] = encodeStr(getNumPrefixRadix(resultSet.getDataType()));
            tuple[GetColumns_Metadata.NULLABLE.getIndex()] = encodeStr(getNullable(resultSet.getIsNullable()));
            tuple[GetColumns_Metadata.REMARKS.getIndex()] = encodeStr(resultSet.getRemarks());
            tuple[GetColumns_Metadata.COLUMN_DEF.getIndex()] = encodeStr(resultSet.getColumnDefault());
            tuple[GetColumns_Metadata.SQL_DATA_TYPE.getIndex()] = encodeStr(sqlType);      // It's unused in the spec, but previously return SQL type
            tuple[GetColumns_Metadata.SQL_DATETIME_SUB.getIndex()] = null;                 // Unused
            tuple[GetColumns_Metadata.CHAR_OCTET_LENGTH.getIndex()] = encodeStr(getColumnSize(
                    rsType,
                    resultSet.getCharacterMaximumLength(),
                    resultSet.getNumericPrecision())); // CHAR_OCTET_LENGTH return same value as COLUMN_SIZE?
            tuple[GetColumns_Metadata.ORDINAL_POSITION.getIndex()] = encodeStr(resultSet.getOrdinalPosition());
            tuple[GetColumns_Metadata.IS_NULLABLE.getIndex()] = encodeStr(resultSet.getIsNullable());
            tuple[GetColumns_Metadata.SCOPE_CATALOG.getIndex()] = null;
            tuple[GetColumns_Metadata.SCOPE_SCHEMA.getIndex()] = null;
            tuple[GetColumns_Metadata.SCOPE_TABLE.getIndex()] = null;
            tuple[GetColumns_Metadata.SOURCE_DATA_TYPE.getIndex()] = null;     // Since Redshift doesn't support either distinct or user-defined reference types, it should return null
            tuple[GetColumns_Metadata.IS_AUTOINCREMENT.getIndex()] = encodeStr(autoIncrement);
            tuple[GetColumns_Metadata.IS_GENERATEDCOLUMN.getIndex()] = encodeStr(autoIncrement);
            tuple[GetColumns_Metadata.SORT_KEY_TYPE.getIndex()] = encodeStr(resultSet.getSortKeyType());
            tuple[GetColumns_Metadata.SORT_KEY.getIndex()] = encodeStr(resultSet.getSortKey());
            tuple[GetColumns_Metadata.DIST_KEY.getIndex()] = encodeStr(resultSet.getDistKey());
            tuple[GetColumns_Metadata.ENCODING.getIndex()] = encodeStr(resultSet.getEncoding());
            tuple[GetColumns_Metadata.COLLATION.getIndex()] = encodeStr(resultSet.getCollation());

            // Add the data row into the tuple list
            columnTuples.add(new Tuple(tuple));
        }
        return createRs(GET_COLUMNS_COLS, columnTuples);
    }

    // Post-processing for metadata API getPrimaryKeys()
    protected ResultSet getPrimaryKeysPostProcessing(List<ShowPrimaryKeysInfo> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getPrimaryKeysPostProcessing");
        }

        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_PRIMARY_KEYS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> primaryKeyTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (ShowPrimaryKeysInfo resultSet : serverResultSets) {
            byte[][] tuple = getEmptyTuple(GET_PRIMARY_KEYS_COLS, GET_PRIMARY_KEYS_COLS.length);

            // Apply the post-processing
            tuple[GetPrimaryKeys_Metadata.TABLE_CAT.getIndex()] = encodeStr(resultSet.getDatabaseName());
            tuple[GetPrimaryKeys_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(resultSet.getSchemaName());
            tuple[GetPrimaryKeys_Metadata.TABLE_NAME.getIndex()] = encodeStr(resultSet.getTableName());
            tuple[GetPrimaryKeys_Metadata.COLUMN_NAME.getIndex()] = encodeStr(resultSet.getColumnName());
            tuple[GetPrimaryKeys_Metadata.KEY_SEQ.getIndex()] = encodeStr(resultSet.getKeySeq());
            tuple[GetPrimaryKeys_Metadata.PK_NAME.getIndex()] = encodeStr(resultSet.getPkName());

            // Add the data row into the tuple list
            primaryKeyTuples.add(new Tuple(tuple));
        }
        return createRs(GET_PRIMARY_KEYS_COLS, primaryKeyTuples);

    }

    // Post-processing for metadata API getImportedKeys() and getExportedKeys()
    protected ResultSet getForeignKeysPostProcessing(List<ShowForeignKeysInfo> serverResultSets, boolean isImported) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getForeignKeysPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_FOREIGN_KEYS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> foreignKeyTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (ShowForeignKeysInfo resultSet : serverResultSets) {
            // Handle default values for rules and deferrability
            String updateRule = resultSet.getUpdateRule() == null ?
                    Short.toString(IMPORTED_KEY_NO_ACTION) : resultSet.getUpdateRule();

            String deleteRule = resultSet.getDeleteRule() == null ?
                    Short.toString(IMPORTED_KEY_NO_ACTION) : resultSet.getDeleteRule();

            String deferrability = resultSet.getDeferrability() == null ?
                    Short.toString(IMPORTED_KEY_NOT_DEFERRABLE) : resultSet.getDeferrability();

            byte[][] tuple = getEmptyTuple(GET_FOREIGN_KEYS_COLS, GET_FOREIGN_KEYS_COLS.length);

            // Apply the post-processing
            tuple[GetForeignKeys_Metadata.PKTABLE_CAT.getIndex()] = encodeStr(resultSet.getPkDatabaseName());
            tuple[GetForeignKeys_Metadata.PKTABLE_SCHEM.getIndex()] = encodeStr(resultSet.getPkSchemaName());
            tuple[GetForeignKeys_Metadata.PKTABLE_NAME.getIndex()] = encodeStr(resultSet.getPkTableName());
            tuple[GetForeignKeys_Metadata.PKCOLUMN_NAME.getIndex()] = encodeStr(resultSet.getPkColumnName());
            tuple[GetForeignKeys_Metadata.FKTABLE_CAT.getIndex()] = encodeStr(resultSet.getFkDatabaseName());
            tuple[GetForeignKeys_Metadata.FKTABLE_SCHEM.getIndex()] = encodeStr(resultSet.getFkSchemaName());
            tuple[GetForeignKeys_Metadata.FKTABLE_NAME.getIndex()] = encodeStr(resultSet.getFkTableName());
            tuple[GetForeignKeys_Metadata.FKCOLUMN_NAME.getIndex()] = encodeStr(resultSet.getFkColumnName());
            tuple[GetForeignKeys_Metadata.KEY_SEQ.getIndex()] = encodeStr(resultSet.getKeySeq());
            tuple[GetForeignKeys_Metadata.UPDATE_RULE.getIndex()] = encodeStr(updateRule);
            tuple[GetForeignKeys_Metadata.DELETE_RULE.getIndex()] = encodeStr(deleteRule);
            tuple[GetForeignKeys_Metadata.FK_NAME.getIndex()] = encodeStr(resultSet.getFkName());
            tuple[GetForeignKeys_Metadata.PK_NAME.getIndex()] = encodeStr(resultSet.getPkName());
            tuple[GetForeignKeys_Metadata.DEFERRABILITY.getIndex()] = encodeStr(deferrability);

            // Add the data row into the tuple list
            foreignKeyTuples.add(new Tuple(tuple));
        }
        sortForeignKeyTuples(foreignKeyTuples, isImported);
        return createRs(GET_FOREIGN_KEYS_COLS, foreignKeyTuples);
    }

    // Post-processing for metadata API getBestRowIdentifier()
    protected ResultSet getBestRowIdentifierPostProcessing(List<BestRowIdenData> serverResultSets, int scope) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getBestRowIdentifierPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_BEST_ROW_IDENTIFIER_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> bestRowIdentifierTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (BestRowIdenData bestRowIdenRs : serverResultSets) {
            if (bestRowIdenRs == null) {
                if (RedshiftLogger.isEnable()) {
                    connection.getLogger().logDebug("Receive null intermediate data hence skipping post-processing");
                }
                continue;
            }
            List<ShowColumnsInfo> curRs = bestRowIdenRs.getResultSet();
            for (ShowColumnsInfo columnRs : curRs) {
                String columnName = columnRs.getColumnName();
                if (!bestRowIdenRs.getPkColumnSet().contains(columnName)) {
                    continue;
                }
                String dataType = columnRs.getDataType();
                String rsType;

                // Retrieve customize precision from DATETIME/INTERVAL data type
                int precisions = 0;
                boolean dateTimeCustomizePrecision = false;
                if (dataType.matches(DATETIME_PRECISION_PATTERN) || dataType.matches(INTERVAL_PRECISION_PATTERN)) {
                    rsType = getRSType(dataType.replaceAll(PRECISION_REMOVAL_PATTERN, "").replaceFirst(TRAILING_SPACES_PATTERN, ""));
                    precisions = Integer.parseInt(dataType.replaceAll(PRECISION_EXTRACTION_PATTERN, "$1"));
                    dateTimeCustomizePrecision = true;
                } else {
                    rsType = getRSType(dataType);
                }

                String sqlType = getSQLType(rsType);

                byte[][] tuple = getEmptyTuple(GET_BEST_ROW_IDENTIFIER_COLS, GET_BEST_ROW_IDENTIFIER_COLS.length);

                // Apply the post-processing
                tuple[GetBestRowIdentifier_Metadata.SCOPE.getIndex()] = encodeStr(Integer.toString(scope));
                tuple[GetBestRowIdentifier_Metadata.COLUMN_NAME.getIndex()] = encodeStr(columnRs.getColumnName());
                tuple[GetBestRowIdentifier_Metadata.DATA_TYPE.getIndex()] = encodeStr(sqlType);
                tuple[GetBestRowIdentifier_Metadata.TYPE_NAME.getIndex()] = encodeStr(rsType);
                tuple[GetBestRowIdentifier_Metadata.COLUMN_SIZE.getIndex()] = encodeStr(getColumnSize(
                        rsType,
                        columnRs.getCharacterMaximumLength(),
                        columnRs.getNumericPrecision()));
                tuple[GetBestRowIdentifier_Metadata.BUFFER_LENGTH.getIndex()] = null;
                tuple[GetBestRowIdentifier_Metadata.DECIMAL_DIGITS.getIndex()] = encodeStr(getDecimalDigit(
                        rsType,
                        columnRs.getNumericScale(),
                        precisions,
                        dateTimeCustomizePrecision));
                tuple[GetBestRowIdentifier_Metadata.PSEUDO_COLUMN.getIndex()] = encodeStr(PSEUDO_COLUMN_VALUE);

                // Add the data row into the tuple list
                bestRowIdentifierTuples.add(new Tuple(tuple));
            }
        }
        return createRs(GET_BEST_ROW_IDENTIFIER_COLS, bestRowIdentifierTuples);
    }

    // Post-processing for metadata API getTableTypes()
    protected ResultSet getTableTypesPostProcessing(ResultSet serverResultSet) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getTableTypesPostProcessing");
        }

        // Directly return empty ResultSet if input is null
        if (serverResultSet == null) {
            return createRs(GET_TABLE_TYPE_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> finalData = new ArrayList<>();

        try (Statement stmt = serverResultSet.getStatement()) {
            while (serverResultSet.next()) {
                byte[][] tuple = getEmptyTuple(GET_TABLE_TYPE_COLS, GET_TABLE_TYPE_COLS.length);

                // Apply the post-processing
                tuple[0] = encodeStr(serverResultSet.getString(1)); // TABLE_TYPE

                // Add the data row into the tuple list
                finalData.add(new Tuple(tuple));
            }
        } catch(SQLException e) {
            if (RedshiftLogger.isEnable()) {
                RedshiftLogger.getDriverLogger().logError(e);
            }
            throw e;
        }

        return createRs(GET_TABLE_TYPE_COLS, finalData);
    }


    // Post-processing for metadata API getColumnPrivileges()
    protected ResultSet getColumnPrivilegesPostProcessing(List<ShowGrantsInfo> serverResultSets, String columnPattern) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getColumnPrivilegesPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_COLUMN_PRIVILEGES_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> columnPrivilegesTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ShowGrantsInfo resultSet : serverResultSets) {
            if(!patternMatch(resultSet.getColumnName(),  columnPattern)){
                continue;
            }
            Map<String, Object> row = new HashMap<>();
            row.put(SHOW_GRANT_DATABASE_NAME, resultSet.getDatabaseName());
            row.put(SHOW_GRANT_SCHEMA_NAME, resultSet.getSchemaName());
            row.put(SHOW_GRANT_TABLE_NAME, resultSet.getTableName());
            row.put(SHOW_GRANT_COLUMN_NAME, resultSet.getColumnName());
            row.put(SHOW_GRANT_GRANTOR, resultSet.getGrantor());
            row.put(SHOW_GRANT_IDENTITY_NAME, resultSet.getIdentityName());
            row.put(SHOW_GRANT_PRIVILEGE_TYPE, resultSet.getPrivilegeType());
            row.put(SHOW_GRANT_ADMIN_OPTION, resultSet.getAdminOption());
            rows.add(row);
        }

        // Sort the rows based on column and privilege type
        Collections.sort(rows, (r1, r2) -> {
            // Compare table names
            String column1 = (String)r1.get(SHOW_GRANT_COLUMN_NAME);
            String column2 = (String)r2.get(SHOW_GRANT_COLUMN_NAME);
            int columnCompare = compareNullableStrings(column1, column2);
            if (columnCompare != 0) return columnCompare;

            // Compare privilege types
            String priv1 = (String)r1.get(SHOW_GRANT_PRIVILEGE_TYPE);
            String priv2 = (String)r2.get(SHOW_GRANT_PRIVILEGE_TYPE);
            return compareNullableStrings(priv1, priv2);
        });

        for (Map<String, Object> row : rows) {
            byte[][] tuple = getEmptyTuple(GET_COLUMN_PRIVILEGES_COLS, GET_COLUMN_PRIVILEGES_COLS.length);

            // Apply the post-processing
            tuple[GetColumnPrivileges_Metadata.TABLE_CAT.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_DATABASE_NAME));
            tuple[GetColumnPrivileges_Metadata.TABLE_SCHEM.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_SCHEMA_NAME));
            tuple[GetColumnPrivileges_Metadata.TABLE_NAME.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_TABLE_NAME));
            tuple[GetColumnPrivileges_Metadata.COLUMN_NAME.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_COLUMN_NAME));
            tuple[GetColumnPrivileges_Metadata.GRANTOR.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_GRANTOR));
            tuple[GetColumnPrivileges_Metadata.GRANTEE.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_IDENTITY_NAME));
            tuple[GetColumnPrivileges_Metadata.PRIVILEGE.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_PRIVILEGE_TYPE));
            tuple[GetColumnPrivileges_Metadata.IS_GRANTABLE.getIndex()] = encodeStr(getIsGrantable((Boolean)row.get(SHOW_GRANT_ADMIN_OPTION)));

            // Add the data row into the tuple list
            columnPrivilegesTuples.add(new Tuple(tuple));
        }
        return createRs(GET_COLUMN_PRIVILEGES_COLS, columnPrivilegesTuples);
    }

    // Post-processing for metadata API getTablePrivileges()
    protected ResultSet getTablePrivilegesPostProcessing(List<ShowGrantsInfo> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getTablePrivilegesPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_TABLE_PRIVILEGES_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> tablePrivilegesTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ShowGrantsInfo resultSet : serverResultSets) {
            Map<String, Object> row = new HashMap<>();
            row.put(SHOW_GRANT_DATABASE_NAME, resultSet.getDatabaseName());
            row.put(SHOW_GRANT_SCHEMA_NAME, resultSet.getSchemaName());
            row.put(SHOW_GRANT_OBJECT_NAME, resultSet.getObjectName());
            row.put(SHOW_GRANT_GRANTOR, resultSet.getGrantor());
            row.put(SHOW_GRANT_IDENTITY_NAME, resultSet.getIdentityName());
            row.put(SHOW_GRANT_PRIVILEGE_TYPE, resultSet.getPrivilegeType());
            row.put(SHOW_GRANT_ADMIN_OPTION, resultSet.getAdminOption());
            rows.add(row);
        }

        // Sort the rows based on catalog, schema, table, and privilege type
        Collections.sort(rows, (r1, r2) -> {
            // Compare database names (catalog)
            String db1 = (String)r1.get(SHOW_GRANT_DATABASE_NAME);
            String db2 = (String)r2.get(SHOW_GRANT_DATABASE_NAME);
            int dbCompare = compareNullableStrings(db1, db2);
            if (dbCompare != 0) return dbCompare;

            // Compare schema names
            String schema1 = (String)r1.get(SHOW_GRANT_SCHEMA_NAME);
            String schema2 = (String)r2.get(SHOW_GRANT_SCHEMA_NAME);
            int schemaCompare = compareNullableStrings(schema1, schema2);
            if (schemaCompare != 0) return schemaCompare;

            // Compare table names
            String table1 = (String)r1.get(SHOW_GRANT_OBJECT_NAME);
            String table2 = (String)r2.get(SHOW_GRANT_OBJECT_NAME);
            int tableCompare = compareNullableStrings(table1, table2);
            if (tableCompare != 0) return tableCompare;

            // Compare privilege types
            String priv1 = (String)r1.get(SHOW_GRANT_PRIVILEGE_TYPE);
            String priv2 = (String)r2.get(SHOW_GRANT_PRIVILEGE_TYPE);
            return compareNullableStrings(priv1, priv2);
        });

        for (Map<String, Object> row : rows) {
            byte[][] tuple = getEmptyTuple(GET_TABLE_PRIVILEGES_COLS, GET_TABLE_PRIVILEGES_COLS.length);

            // Apply the post-processing
            tuple[GetTablePrivileges_Metadata.TABLE_CAT.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_DATABASE_NAME));
            tuple[GetTablePrivileges_Metadata.TABLE_SCHEM.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_SCHEMA_NAME));
            tuple[GetTablePrivileges_Metadata.TABLE_NAME.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_OBJECT_NAME));
            tuple[GetTablePrivileges_Metadata.GRANTOR.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_GRANTOR));
            tuple[GetTablePrivileges_Metadata.GRANTEE.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_IDENTITY_NAME));
            tuple[GetTablePrivileges_Metadata.PRIVILEGE.getIndex()] = encodeStr((String)row.get(SHOW_GRANT_PRIVILEGE_TYPE));
            tuple[GetTablePrivileges_Metadata.IS_GRANTABLE.getIndex()] = encodeStr(getIsGrantable((Boolean)row.get(SHOW_GRANT_ADMIN_OPTION)));

            // Add the data row into the tuple list
            tablePrivilegesTuples.add(new Tuple(tuple));
        }
        return createRs(GET_TABLE_PRIVILEGES_COLS, tablePrivilegesTuples);
    }

    // Helper method for comparing potentially null strings
    private static int compareNullableStrings(String s1, String s2) {
        if (s1 == null) return s2 == null ? 0 : -1;
        if (s2 == null) return 1;
        return s1.compareTo(s2);
    }

    // Post-processing for metadata API getProcedures()
    protected ResultSet getProceduresPostProcessing(List<ShowProceduresInfo> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getProceduresPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_PROCEDURES_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> proceduresTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (ShowProceduresInfo resultSet : serverResultSets) {
            byte[][] tuple = getEmptyTuple(GET_PROCEDURES_COLS, GET_PROCEDURES_COLS.length);

            // Apply the post-processing
            tuple[GetProcedures_Metadata.PROCEDURE_CAT.getIndex()] = encodeStr(resultSet.getDatabaseName());
            tuple[GetProcedures_Metadata.PROCEDURE_SCHEM.getIndex()] = encodeStr(resultSet.getSchemaName());
            tuple[GetProcedures_Metadata.PROCEDURE_NAME.getIndex()] = encodeStr(resultSet.getProcedureName());
            tuple[GetProcedures_Metadata.RESERVE1.getIndex()] = null;
            tuple[GetProcedures_Metadata.RESERVE2.getIndex()] = null;
            tuple[GetProcedures_Metadata.RESERVE3.getIndex()] = null;
            tuple[GetProcedures_Metadata.REMARKS.getIndex()] = encodeStr(EMPTY_REMARKS);
            tuple[GetProcedures_Metadata.PROCEDURE_TYPE.getIndex()] = encodeStr(String.valueOf(getProcedureType(resultSet.getReturnType())));
            tuple[GetProcedures_Metadata.SPECIFIC_NAME.getIndex()] = encodeStr(getSpecificName(
                    resultSet.getProcedureName(),
                    resultSet.getArgumentList()));

            // Add the data row into the tuple list
            proceduresTuples.add(new Tuple(tuple));
        }
        return createRs(GET_PROCEDURES_COLS, proceduresTuples);
    }

    // Post-processing for metadata API getProcedureColumns()
    protected ResultSet getProcedureColumnsPostProcessing(List<ProcedureFunctionColumnData> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getProcedureColumnsPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_PROCEDURES_COLUMNS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> procedureColumnsTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (ProcedureFunctionColumnData procRs : serverResultSets) {
            if (procRs == null) {
                if (RedshiftLogger.isEnable()) {
                    connection.getLogger().logDebug("Receive null intermediate data hence skipping post-processing");
                }
                continue;
            }

            List<ShowParametersInfo> rs = procRs.getResultSet();
            for (ShowParametersInfo paramInfo : rs) {
                String dataType = paramInfo.getDataType();
                String rsType;

                // Retrieve customize precision from DATETIME/INTERVAL data type
                int precisions = 0;
                boolean dateTimeCustomizePrecision = false;
                if (dataType.matches(DATETIME_PRECISION_PATTERN) || dataType.matches(INTERVAL_PRECISION_PATTERN)) {
                    rsType = getRSType(dataType.replaceAll(PRECISION_REMOVAL_PATTERN, "").replaceFirst(TRAILING_SPACES_PATTERN, ""));
                    precisions = Integer.parseInt(dataType.replaceAll(PRECISION_EXTRACTION_PATTERN, "$1"));
                    dateTimeCustomizePrecision = true;
                } else {
                    rsType = getRSType(dataType);
                }

                String sqlType = getSQLType(rsType);

                byte[][] tuple = getEmptyTuple(GET_PROCEDURES_COLUMNS_COLS, GET_PROCEDURES_COLUMNS_COLS.length);

                // Apply the post-processing
                tuple[GetProceduresColumns_Metadata.PROCEDURE_CAT.getIndex()] = encodeStr(paramInfo.getDatabaseName());
                tuple[GetProceduresColumns_Metadata.PROCEDURE_SCHEM.getIndex()] = encodeStr(paramInfo.getSchemaName());
                tuple[GetProceduresColumns_Metadata.PROCEDURE_NAME.getIndex()] = encodeStr(paramInfo.getProcedureName());
                tuple[GetProceduresColumns_Metadata.COLUMN_NAME.getIndex()] = encodeStr(paramInfo.getParameterName());
                tuple[GetProceduresColumns_Metadata.COLUMN_TYPE.getIndex()] = encodeStr(String.valueOf(getProcedureColumnType(
                        paramInfo.getParameterType())));
                tuple[GetProceduresColumns_Metadata.DATA_TYPE.getIndex()] = encodeStr(sqlType);
                tuple[GetProceduresColumns_Metadata.TYPE_NAME.getIndex()] = encodeStr(rsType);
                tuple[GetProceduresColumns_Metadata.PRECISION.getIndex()] = encodeStr(getColumnSize(
                        rsType,
                        paramInfo.getCharacterMaximumLength(),
                        paramInfo.getNumericPrecision()));
                tuple[GetProceduresColumns_Metadata.LENGTH.getIndex()] = encodeStr(getColumnLength(rsType));
                tuple[GetProceduresColumns_Metadata.SCALE.getIndex()] = encodeStr(getDecimalDigit(
                        rsType,
                        paramInfo.getNumericScale(),
                        precisions,
                        dateTimeCustomizePrecision));
                tuple[GetProceduresColumns_Metadata.RADIX.getIndex()] = encodeStr(RADIX_VALUE);
                tuple[GetProceduresColumns_Metadata.NULLABLE.getIndex()] = encodeStr(NULLABLE_UNKNOWN_VALUE);  // procedureNullableUnknown
                tuple[GetProceduresColumns_Metadata.REMARKS.getIndex()] = encodeStr(EMPTY_REMARKS);
                tuple[GetProceduresColumns_Metadata.COLUMN_DEF.getIndex()] = null;
                tuple[GetProceduresColumns_Metadata.SQL_DATA_TYPE.getIndex()] = encodeStr(sqlType);
                tuple[GetProceduresColumns_Metadata.SQL_DATETIME_SUB.getIndex()] = null;
                tuple[GetProceduresColumns_Metadata.CHAR_OCTET_LENGTH.getIndex()] = null;
                tuple[GetProceduresColumns_Metadata.ORDINAL_POSITION.getIndex()] = encodeStr(paramInfo.getOrdinalPosition());
                tuple[GetProceduresColumns_Metadata.IS_NULLABLE.getIndex()] = encodeStr(IS_NULLABLE_VALUE);
                tuple[GetProceduresColumns_Metadata.SPECIFIC_NAME.getIndex()] = encodeStr(procRs.getSpecificName());

                // Add the data row into the tuple list
                procedureColumnsTuples.add(new Tuple(tuple));
            }
        }
        return createRs(GET_PROCEDURES_COLUMNS_COLS, procedureColumnsTuples);
    }


    // Post-processing for metadata API getFunctions()
    protected ResultSet getFunctionsPostProcessing(List<ShowFunctionsInfo> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getFunctionsPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_FUNCTIONS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> functionsTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (ShowFunctionsInfo resultSet : serverResultSets) {
            byte[][] tuple = getEmptyTuple(GET_FUNCTIONS_COLS, GET_FUNCTIONS_COLS.length);

            // Apply the post-processing
            tuple[GetFunctions_Metadata.FUNCTION_CAT.getIndex()] = encodeStr(resultSet.getDatabaseName());
            tuple[GetFunctions_Metadata.FUNCTION_SCHEM.getIndex()] = encodeStr(resultSet.getSchemaName());
            tuple[GetFunctions_Metadata.FUNCTION_NAME.getIndex()] = encodeStr(resultSet.getFunctionName());
            tuple[GetFunctions_Metadata.REMARKS.getIndex()] = encodeStr(EMPTY_REMARKS);
            tuple[GetFunctions_Metadata.FUNCTION_TYPE.getIndex()] = encodeStr(String.valueOf(getFunctionType(resultSet.getReturnType())));
            tuple[GetFunctions_Metadata.SPECIFIC_NAME.getIndex()] = encodeStr(getSpecificName(
                    resultSet.getFunctionName(),
                    resultSet.getArgumentList()));

            // Add the data row into the tuple list
            functionsTuples.add(new Tuple(tuple));
        }
        return createRs(GET_FUNCTIONS_COLS, functionsTuples);
    }

    // Post-processing for metadata API getFunctionColumns()
    protected ResultSet getFunctionColumnsPostProcessing(List<ProcedureFunctionColumnData> serverResultSets) throws SQLException {
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling getFunctionColumnsPostProcessing");
        }
        // Directly return empty ResultSet
        if(serverResultSets == null || serverResultSets.isEmpty()){
            return createRs(GET_FUNCTIONS_COLUMNS_COLS, new ArrayList<>());
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> functionColumnsTuples = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing
        for (ProcedureFunctionColumnData funcRs : serverResultSets) {
            if (funcRs == null) {
                if (RedshiftLogger.isEnable()) {
                    connection.getLogger().logDebug("Receive null intermediate data hence skipping post-processing");
                }
                continue;
            }

            List<ShowParametersInfo> rs = funcRs.getResultSet();
            for (ShowParametersInfo paramInfo : rs) {
                String dataType = paramInfo.getDataType();
                String rsType;

                // Retrieve customize precision from DATETIME/INTERVAL data type
                int precisions = 0;
                boolean dateTimeCustomizePrecision = false;
                if (dataType.matches(DATETIME_PRECISION_PATTERN) || dataType.matches(INTERVAL_PRECISION_PATTERN)) {
                    rsType = getRSType(dataType.replaceAll(PRECISION_REMOVAL_PATTERN, "").replaceFirst(TRAILING_SPACES_PATTERN, ""));
                    precisions = Integer.parseInt(dataType.replaceAll(PRECISION_EXTRACTION_PATTERN, "$1"));
                    dateTimeCustomizePrecision = true;
                } else {
                    rsType = getRSType(dataType);
                }

                String sqlType = getSQLType(rsType);

                byte[][] tuple = getEmptyTuple(GET_FUNCTIONS_COLUMNS_COLS, GET_FUNCTIONS_COLUMNS_COLS.length);

                // Apply the post-processing
                tuple[GetFunctionsColumns_Metadata.FUNCTION_CAT.getIndex()] = encodeStr(paramInfo.getDatabaseName());
                tuple[GetFunctionsColumns_Metadata.FUNCTION_SCHEM.getIndex()] = encodeStr(paramInfo.getSchemaName());
                tuple[GetFunctionsColumns_Metadata.FUNCTION_NAME.getIndex()] = encodeStr(paramInfo.getFunctionName());
                tuple[GetFunctionsColumns_Metadata.COLUMN_NAME.getIndex()] = encodeStr(paramInfo.getParameterName());
                tuple[GetFunctionsColumns_Metadata.COLUMN_TYPE.getIndex()] = encodeStr(String.valueOf(getFunctionColumnType(
                        paramInfo.getParameterType())));
                tuple[GetFunctionsColumns_Metadata.DATA_TYPE.getIndex()] = encodeStr(sqlType);
                tuple[GetFunctionsColumns_Metadata.TYPE_NAME.getIndex()] = encodeStr(rsType);
                tuple[GetFunctionsColumns_Metadata.PRECISION.getIndex()] = encodeStr(getColumnSize(
                        rsType,
                        paramInfo.getCharacterMaximumLength(),
                        paramInfo.getNumericPrecision()));
                tuple[GetFunctionsColumns_Metadata.LENGTH.getIndex()] = encodeStr(getColumnLength(rsType));
                tuple[GetFunctionsColumns_Metadata.SCALE.getIndex()] = encodeStr(getDecimalDigit(
                        rsType,
                        paramInfo.getNumericScale(),
                        precisions,
                        dateTimeCustomizePrecision));
                tuple[GetFunctionsColumns_Metadata.RADIX.getIndex()] = encodeStr(RADIX_VALUE);
                tuple[GetFunctionsColumns_Metadata.NULLABLE.getIndex()] = encodeStr(NULLABLE_UNKNOWN_VALUE);
                tuple[GetFunctionsColumns_Metadata.REMARKS.getIndex()] = encodeStr(EMPTY_REMARKS);
                tuple[GetFunctionsColumns_Metadata.CHAR_OCTET_LENGTH.getIndex()] = null;
                tuple[GetFunctionsColumns_Metadata.ORDINAL_POSITION.getIndex()] = encodeStr(paramInfo.getOrdinalPosition());
                tuple[GetFunctionsColumns_Metadata.IS_NULLABLE.getIndex()] = encodeStr(IS_NULLABLE_VALUE);
                tuple[GetFunctionsColumns_Metadata.SPECIFIC_NAME.getIndex()] = encodeStr(funcRs.getSpecificName());

                // Add the data row into the tuple list
                functionColumnsTuples.add(new Tuple(tuple));
            }
        }
        return createRs(GET_FUNCTIONS_COLUMNS_COLS, functionColumnsTuples);
    }
}
