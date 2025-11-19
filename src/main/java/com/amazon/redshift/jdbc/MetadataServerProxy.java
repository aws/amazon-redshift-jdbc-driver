/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;


import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;

import java.sql.*;
import java.util.*;
import com.amazon.redshift.core.Utils;

public class MetadataServerProxy extends MetadataAPIHelper {

    public MetadataServerProxy(RedshiftConnectionImpl connection) throws SQLException {
        super(connection);
    }

    /**
     * Returns a Result set for SHOW DATABASES
     * @return the result set for SHOW DATABASES
     * @throws SQLException if a database error occurs
     */
    protected List<String> getCatalogs() throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling SHOW DATABASES");
        }
        List<String> intermediateResults = new ArrayList<>();
        try (PreparedStatement stmt = createMetaDataPreparedStatement(SQL_PREP_SHOWDATABASES)) {
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    intermediateResults.add(rs.getString(SHOW_DATABASES_DATABASE_NAME));
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getCatalogs: " + e.getMessage());
        }
        return intermediateResults;
    }

    /**
     * Returns a list of intermediate result set for SHOW SCHEMAS
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW SCHEMAS
     * @throws SQLException if a database error occurs
     */
    protected List<ShowSchemasInfo> getSchemas(String catalog, String schemaPattern, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ShowSchemasInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for (String curCatalog : catalogList) {
                intermediateRs.addAll(callShowSchemas(curCatalog, schemaPattern, true));
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getSchemas: " + e.getMessage());
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Return intermediate result set for catalog: {0}, schemaPattern: {1}", catalog, schemaPattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW TABLES
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW TABLES
     * @throws SQLException if a database error occurs
     */
    protected List<ShowTablesInfo> getTables(String catalog, String schemaPattern, String tableNamePattern, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ShowTablesInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCat : catalogList) {
                // Get Schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCat, schemaPattern, false);
                for (ShowSchemasInfo curSchema : schemaList) {
                    intermediateRs.addAll(callShowTables(
                            curCat,
                            curSchema.getSchemaName(),
                            tableNamePattern,
                            true
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getTables: " + e.getMessage());
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Return intermediate result set for catalog = {0}, schemaPattern = {1}, tableNamePattern = {2}",
                    catalog, schemaPattern, tableNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW COLUMNS
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW COLUMNS
     * @throws SQLException if a database error occurs
     */
    protected List<ShowColumnsInfo> getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                               String columnNamePattern, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ShowColumnsInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCat : catalogList){
                // Get Schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCat, schemaPattern, false);
                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get Table list
                    List<ShowTablesInfo> tableList = callShowTables(curCat, curSchema.getSchemaName(), tableNamePattern, false);
                    for (ShowTablesInfo curTable : tableList) {
                        intermediateRs.addAll(callShowColumns(
                                curCat,
                                curSchema.getSchemaName(),
                                curTable.getTableName(),
                                columnNamePattern
                        ));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getColumns: " + e.getMessage());
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Return intermediate result set for catalog = {0}, schemaPattern = {1}, tableNamePattern = {2}, columnNamePattern = {3}",
                    catalog, schemaPattern, tableNamePattern, columnNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW CONSTRAINTS PRIMARY KEY
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schema a schema name; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param table a table name; must match the table name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return a list of intermediate result set for SHOW CONSTRAINTS PRIMARY KEY
     * @throws SQLException if a database error occurs
     */
    protected List<ShowPrimaryKeysInfo> getPrimaryKeys(String catalog, String schema, String table, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ShowPrimaryKeysInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = new ArrayList<>();
                if (Utils.isNullOrEmpty(schema)) {
                    schemaList = callShowSchemas(curCatalog, schema, false);
                } else {
                    schemaList.add(new ShowSchemasInfo(schema));
                }

                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get table list
                    List<ShowTablesInfo> tableList = new ArrayList<>();
                    if (Utils.isNullOrEmpty(table)) {
                        tableList = callShowTables(curCatalog, curSchema.getSchemaName(), table, false);
                    } else {
                        tableList.add(new ShowTablesInfo(table));
                    }

                    for (ShowTablesInfo curTable : tableList) {
                        intermediateRs.addAll(callShowConstraintsPrimaryKey(
                                curCatalog,
                                curSchema.getSchemaName(),
                                curTable.getTableName()
                        ));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getPrimaryKeys: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW CONSTRAINTS PRIMARY KEYS for catalog = {0}, schema = {1}, table = {2}",
                    catalog, schema, table);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW CONSTRAINTS FOREIGN KEY
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schema a schema name; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param table a table name; must match the table name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @param isImported boolean to determine if we want to retrieve imported keys or exported keys
     * @return a list of intermediate result set for SHOW CONSTRAINTS FOREIGN KEY
     * @throws SQLException if a database error occurs
     */
    protected List<ShowForeignKeysInfo> getForeignKeys(String catalog, String schema, String table, boolean isSingleDatabaseMetaData,
                                                       boolean isImported) throws SQLException {

        List<ShowForeignKeysInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = new ArrayList<>();
                if (Utils.isNullOrEmpty(schema)) {
                    schemaList = callShowSchemas(curCatalog, schema, false);
                } else {
                    schemaList.add(new ShowSchemasInfo(schema));
                }

                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get table list
                    List<ShowTablesInfo> tableList = new ArrayList<>();
                    if (Utils.isNullOrEmpty(table)) {
                        tableList = callShowTables(curCatalog, curSchema.getSchemaName(), table, false);
                    } else {
                        tableList.add(new ShowTablesInfo(table));
                    }

                    for (ShowTablesInfo curTable : tableList) {
                        String sql;
                        if (isImported) {
                            sql = SQL_PREP_SHOWFOREIGNKEYS;
                        } else {
                            sql = SQL_PREP_SHOWFOREIGNEXPORTEDKEYS;
                        }
                        intermediateRs.addAll(callShowConstraintsForeignKey(
                                curCatalog,
                                curSchema.getSchemaName(),
                                curTable.getTableName(),
                                sql
                        ));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getForeignKeys: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW CONSTRAINTS FOREIGN KEYS for catalog = {0}, schema = {1}, table = {2}",
                    catalog, schema, table);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for Best Row Identifier
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schema a schema name; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param table a table name; must match the table name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for Best Row Identifier
     * @throws SQLException if a database error occurs
     */
    public List<BestRowIdenData> getBestRowIdentifier(String catalog, String schema, String table, boolean isSingleDatabaseMetaData) throws SQLException {
        List<BestRowIdenData> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = new ArrayList<>();
                if (Utils.isNullOrEmpty(schema)) {
                    schemaList = callShowSchemas(curCatalog, schema, false);
                } else {
                    schemaList.add(new ShowSchemasInfo(schema));
                }

                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get table list
                    List<ShowTablesInfo> tableList = new ArrayList<>();
                    if (Utils.isNullOrEmpty(table)) {
                        tableList = callShowTables(curCatalog, curSchema.getSchemaName(), table, false);
                    } else {
                        tableList.add(new ShowTablesInfo(table));
                    }

                    for (ShowTablesInfo curTable : tableList) {
                        List<ShowPrimaryKeysInfo> showConstraintRs = callShowConstraintsPrimaryKey(curCatalog, curSchema.getSchemaName(), curTable.getTableName());
                        HashSet<String> pkColumnSet = new HashSet<>();
                        for (ShowPrimaryKeysInfo curPrimaryKey : showConstraintRs) {
                            pkColumnSet.add(curPrimaryKey.getColumnName());
                        }
                        if (!pkColumnSet.isEmpty()) {
                            intermediateRs.add(new BestRowIdenData(
                                    callShowColumns(
                                        curCatalog,
                                        curSchema.getSchemaName(),
                                        curTable.getTableName(),
                                        null),
                                    pkColumnSet
                            ));
                        }

                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getBestRowIdentifier: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed Best Row Identifier for catalog = {0}, schema = {1}, table = {2}",
                    catalog, schema, table);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW GRANTS ON COLUMN
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schema a schema name; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param table a table name; must match the table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for COLUMN PRIVILEGES
     * @throws SQLException if a database error occurs
     */
    public List<ShowGrantsInfo> getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern,
                                                    boolean isSingleDatabaseMetaData) throws SQLException {
        List<ShowGrantsInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = new ArrayList<>();
                if (Utils.isNullOrEmpty(schema)) {
                    schemaList = callShowSchemas(curCatalog, schema, false);
                } else {
                    schemaList.add(new ShowSchemasInfo(schema));
                }

                for (ShowSchemasInfo curSchema : schemaList) {
                    try (PreparedStatement stmt = createMetaDataPreparedStatement(SQL_PREP_SHOWGRANTSCOLUMN)) {
                        stmt.setString(1, curCatalog);
                        stmt.setString(2, curSchema.getSchemaName());
                        stmt.setString(3, table);

                        stmt.execute();
                        try (ResultSet rs = stmt.getResultSet()) {
                            while (rs.next()) {
                                intermediateRs.add(new ShowGrantsInfo(
                                        rs.getString(SHOW_GRANT_DATABASE_NAME),
                                        rs.getString(SHOW_GRANT_SCHEMA_NAME),
                                        null,
                                        rs.getString(SHOW_GRANT_TABLE_NAME),
                                        rs.getString(SHOW_GRANT_COLUMN_NAME),
                                        rs.getString(SHOW_GRANT_GRANTOR),
                                        rs.getString(SHOW_GRANT_IDENTITY_NAME),
                                        rs.getString(SHOW_GRANT_PRIVILEGE_TYPE),
                                        rs.getBoolean(SHOW_GRANT_ADMIN_OPTION)
                                ));
                            }
                        } catch (SQLException e) {
                            throw new RedshiftException("MetadataServerProxy.getColumnPrivileges: " + e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getColumnPrivileges: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW GRANTS ON COLUMN for catalog = {0}, schema = {1}, table = {2}, columnNamePattern = {3}",
                    catalog, schema, table, columnNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW GRANTS ON TABLE
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for TABLE PRIVILEGES
     * @throws SQLException if a database error occurs
     */
    public List<ShowGrantsInfo> getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern,
                                                   boolean isSingleDatabaseMetaData) throws SQLException {
        List<ShowGrantsInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCatalog, schemaPattern, false);

                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get table list
                    List<ShowTablesInfo> tableList = callShowTables(curCatalog, curSchema.getSchemaName(), tableNamePattern, false);

                    for (ShowTablesInfo curTable : tableList) {
                        try (PreparedStatement stmt = createMetaDataPreparedStatement(SQL_PREP_SHOWGRANTSTABLE)) {
                            stmt.setString(1, curCatalog);
                            stmt.setString(2, curSchema.getSchemaName());
                            stmt.setString(3, curTable.getTableName());

                            stmt.execute();
                            try (ResultSet rs = stmt.getResultSet()) {
                                while (rs.next()) {
                                    intermediateRs.add(new ShowGrantsInfo(
                                            rs.getString(SHOW_GRANT_DATABASE_NAME),
                                            rs.getString(SHOW_GRANT_SCHEMA_NAME),
                                            rs.getString(SHOW_GRANT_OBJECT_NAME),
                                            null,
                                            null,
                                            rs.getString(SHOW_GRANT_GRANTOR),
                                            rs.getString(SHOW_GRANT_IDENTITY_NAME),
                                            rs.getString(SHOW_GRANT_PRIVILEGE_TYPE),
                                            rs.getBoolean(SHOW_GRANT_ADMIN_OPTION)
                                    ));
                                }
                            } catch (SQLException e) {
                                throw new RedshiftException("MetadataServerProxy.getTablePrivileges: " + e.getMessage(), e);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getTablePrivileges: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW GRANTS ON TABLE for catalog = {0}, schemaPattern = {1}, tableNamePattern = {2}",
                    catalog, schemaPattern, tableNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW PROCEDURES
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param procedureNamePattern a procedure name pattern; must match the procedure name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW PROCEDURES
     * @throws SQLException if a database error occurs
     */
    public List<ShowProceduresInfo> getProcedures(String catalog, String schemaPattern, String procedureNamePattern, boolean isSingleDatabaseMetaData) throws SQLException {
        List<ShowProceduresInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCatalog, schemaPattern, false);

                for (ShowSchemasInfo curSchema : schemaList) {
                    intermediateRs.addAll(callShowProcedures(curCatalog, curSchema.getSchemaName(), procedureNamePattern, true));
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getProcedures: " + e.getMessage(), e);
        }
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW PROCEDURES for catalog = {0}, schemaPattern = {1}, procedureNamePattern = {2}",
                    catalog, schemaPattern, procedureNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW COLUMNS FROM PROCEDURE
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param procedureNamePattern a procedure name pattern; must match the procedure name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for PROCEDURE COLUMNS
     * @throws SQLException if a database error occurs
     */
    protected List<ProcedureFunctionColumnData> getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                                  String columnNamePattern, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ProcedureFunctionColumnData> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCatalog, schemaPattern, false);

                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get procedure list
                    List<ShowProceduresInfo> procedureList = callShowProcedures(curCatalog, curSchema.getSchemaName(), procedureNamePattern, false);

                    // For each procedure, get its columns
                    for (ShowProceduresInfo curProcedurePair : procedureList) {
                        if (curProcedurePair == null) {
                            continue;
                        }
                        String procedureName = curProcedurePair.getProcedureName();
                        String argumentList = curProcedurePair.getArgumentList();
                        Map.Entry<String, List<String>> queryData = createParameterizedQueryString(argumentList, SQL_PREP_SHOWPARAMETERSPROCEDURE, columnNamePattern);

                        try (PreparedStatement stmt = createMetaDataPreparedStatement(queryData.getKey())) {
                            stmt.setString(1, curCatalog);
                            stmt.setString(2, curSchema.getSchemaName());
                            stmt.setString(3, procedureName);
                            int paramIndex = 4;
                            for (String argument : queryData.getValue()) {
                                stmt.setString(paramIndex++, argument);
                            }
                            if (!Utils.isNullOrEmpty(columnNamePattern)) {
                                stmt.setString(paramIndex, columnNamePattern);
                            }

                            stmt.execute();
                            String specificName = getSpecificName(procedureName, argumentList);
                            List<ShowParametersInfo> showParamResult = new ArrayList<>();
                            try (ResultSet rs = stmt.getResultSet()) {
                                while (rs.next()) {
                                    showParamResult.add(new ShowParametersInfo(
                                            rs.getString(SHOW_PARAMETERS_DATABASE_NAME),
                                            rs.getString(SHOW_PARAMETERS_SCHEMA_NAME),
                                            rs.getString(SHOW_PARAMETERS_PROCEDURE_NAME),
                                            null,
                                            rs.getString(SHOW_PARAMETERS_PARAMETER_NAME),
                                            rs.getString(SHOW_PARAMETERS_ORDINAL_POSITION),
                                            rs.getString(SHOW_PARAMETERS_PARAMETER_TYPE),
                                            rs.getString(SHOW_PARAMETERS_DATA_TYPE),
                                            rs.getString(SHOW_PARAMETERS_CHARACTER_MAXIMUM_LENGTH),
                                            rs.getString(SHOW_PARAMETERS_NUMERIC_PRECISION),
                                            rs.getString(SHOW_PARAMETERS_NUMERIC_SCALE)
                                    ));
                                }
                            }
                            intermediateRs.add(new ProcedureFunctionColumnData(specificName, showParamResult));
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getProcedureColumns: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW COLUMNS FROM PROCEDURE for catalog = {0}, schemaPattern = {1}, procedureNamePattern = {2}, columnNamePattern = {3}",
                    catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW FUNCTIONS
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param functionNamePattern a function name pattern; must match the function name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW FUNCTIONS
     * @throws SQLException if a database error occurs
     */
    protected List<ShowFunctionsInfo> getFunctions(String catalog, String schemaPattern, String functionNamePattern,
                                                   boolean isSingleDatabaseMetaData) throws SQLException {

        List<ShowFunctionsInfo> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCatalog, schemaPattern, false);

                for (ShowSchemasInfo curSchema : schemaList) {
                    intermediateRs.addAll(callShowFunctions(curCatalog, curSchema.getSchemaName(), functionNamePattern, true));
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getFunctions: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW FUNCTIONS for catalog = {0}, schemaPattern = {1}, functionNamePattern = {2}",
                    catalog, schemaPattern, functionNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW COLUMNS FROM FUNCTION
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param functionNamePattern a function name pattern; must match the function name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for FUNCTION COLUMNS
     * @throws SQLException if a database error occurs
     */
    protected List<ProcedureFunctionColumnData> getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                                 String columnNamePattern, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ProcedureFunctionColumnData> intermediateRs = new ArrayList<>();

        try {
            // Get Catalog list
            List<String> catalogList = fetchCatalogNames(catalog, isSingleDatabaseMetaData);

            for(String curCatalog : catalogList) {
                // Get schema list
                List<ShowSchemasInfo> schemaList = callShowSchemas(curCatalog, schemaPattern, false);

                for (ShowSchemasInfo curSchema : schemaList) {
                    // Get function list
                    List<ShowFunctionsInfo> functionsList = callShowFunctions(curCatalog, curSchema.getSchemaName(), functionNamePattern, false);

                    // For each function, get its columns
                    for (ShowFunctionsInfo curFunctionPair : functionsList) {
                        if (curFunctionPair == null) {
                            continue;
                        }
                        String functionName = curFunctionPair.getFunctionName();
                        String argumentList = curFunctionPair.getArgumentList();

                        Map.Entry<String, List<String>> queryData = createParameterizedQueryString(argumentList, SQL_PREP_SHOWPARAMETERSFUNCTION, columnNamePattern);

                        try (PreparedStatement stmt = createMetaDataPreparedStatement(queryData.getKey())) {
                            stmt.setString(1, curCatalog);
                            stmt.setString(2, curSchema.getSchemaName());
                            stmt.setString(3, functionName);
                            int paramIndex = 4;
                            for (String argument : queryData.getValue()) {
                                stmt.setString(paramIndex++, argument);
                            }
                            if (!Utils.isNullOrEmpty(columnNamePattern)) {
                                stmt.setString(paramIndex, columnNamePattern);
                            }

                            stmt.execute();
                            String specificName = getSpecificName(functionName, argumentList);
                            List<ShowParametersInfo> showParamResult = new ArrayList<>();
                            try (ResultSet rs = stmt.getResultSet()) {
                                while (rs.next()) {
                                    showParamResult.add(new ShowParametersInfo(
                                            rs.getString(SHOW_PARAMETERS_DATABASE_NAME),
                                            rs.getString(SHOW_PARAMETERS_SCHEMA_NAME),
                                            null,
                                            rs.getString(SHOW_PARAMETERS_FUNCTION_NAME),
                                            rs.getString(SHOW_PARAMETERS_PARAMETER_NAME),
                                            rs.getString(SHOW_PARAMETERS_ORDINAL_POSITION),
                                            rs.getString(SHOW_PARAMETERS_PARAMETER_TYPE),
                                            rs.getString(SHOW_PARAMETERS_DATA_TYPE),
                                            rs.getString(SHOW_PARAMETERS_CHARACTER_MAXIMUM_LENGTH),
                                            rs.getString(SHOW_PARAMETERS_NUMERIC_PRECISION),
                                            rs.getString(SHOW_PARAMETERS_NUMERIC_SCALE)
                                    ));
                                }
                            }
                            intermediateRs.add(new ProcedureFunctionColumnData(specificName, showParamResult));
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerProxy.getFunctionColumns: " + e.getMessage(), e);
        }

        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Successfully executed SHOW COLUMNS FROM FUNCTION LIKE for catalog = {0}, schemaPattern = {1}, functionNamePattern = {2}, columnNamePattern = {3}",
                    catalog, schemaPattern, functionNamePattern, columnNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Retrieves a list of database/catalog names.
     * @param catalog a catalog name; null means that the catalog name should not be used to narrow the search
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return List of catalog names
     * @throws SQLException if a database error occurs
     */
    protected List<String> fetchCatalogNames(String catalog, boolean isSingleDatabaseMetaData) throws SQLException {
        List<String> catalogList = new ArrayList<>();
        String currentCatalog = connection.getCatalog();
        boolean isCatalogEmpty = Utils.isNullOrEmpty(catalog);

        if (isSingleDatabaseMetaData) {
            if (isCatalogEmpty) {
                catalogList.add(currentCatalog);
            } else if (catalog.equals(currentCatalog)) {
                catalogList.add(currentCatalog);
            }
            return catalogList;
        }

        try (PreparedStatement stmt = createMetaDataPreparedStatement(SQL_PREP_SHOWDATABASES)) {
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    String dbName = rs.getString(SHOW_DATABASES_DATABASE_NAME);
                    if (isCatalogEmpty) {
                        catalogList.add(dbName);
                    } else if (catalog.equals(dbName)) {
                        catalogList.add(catalog);
                        break;
                    }
                }
                return catalogList;
            } catch (SQLException e) {
                throw new RedshiftException("getCatalogsList: " + e.getMessage());
            }
        } catch (SQLException e) {
            throw new RedshiftException("getCatalogsList: " + e.getMessage());
        }
    }

    /**
     * Helper function to get a ResultSet for SHOW SCHEMAS
     * @param catalog The name of the catalog
     * @param schemaPattern The schema name pattern
     * @param fullResult If true, returns full result for SHOW; if false, returns only schema names
     * @return ResultSet from SHOW SCHEMAS
     * @throws SQLException if a database error occurs
     */
    protected List<ShowSchemasInfo> callShowSchemas(String catalog, String schemaPattern, boolean fullResult) throws SQLException {
        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW SCHEMAS");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling Server API SHOW SCHEMAS on catalog: {0}, schemaPattern: {1}", catalog, schemaPattern);
        }

        List<ShowSchemasInfo> showSchemasResult = new ArrayList<>();
        try (PreparedStatement stmt = Utils.isNullOrEmpty(schemaPattern) ?
                createMetaDataPreparedStatement(SQL_PREP_SHOWSCHEMAS) :
                createMetaDataPreparedStatement(SQL_PREP_SHOWSCHEMASLIKE)) {
            stmt.setString(1, catalog);
            if (!Utils.isNullOrEmpty(schemaPattern)) {
                stmt.setString(2, schemaPattern);
            }
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    if (fullResult) {
                        showSchemasResult.add(new ShowSchemasInfo(
                                rs.getString(SHOW_SCHEMAS_DATABASE_NAME),
                                rs.getString(SHOW_SCHEMAS_SCHEMA_NAME)
                        ));
                    } else {
                        showSchemasResult.add(new ShowSchemasInfo(
                                rs.getString(SHOW_SCHEMAS_SCHEMA_NAME)
                        ));
                    }
                }
                return showSchemasResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowSchemas: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowSchemas: " + e.getMessage(), e);
        }
    }

    /**
     * Helper function to get a ResultSet for SHOW TABLES
     *
     * @param catalog The catalog name
     * @param schema The schema name
     * @param tableNamePattern The table name pattern
     * @param fullResult If true, returns full result for SHOW; if false, returns only table names
     * @return Resultset from SHOW TABLES
     * @throws SQLException if a database error occurs
     */
    protected List<ShowTablesInfo> callShowTables(String catalog, String schema, String tableNamePattern, boolean fullResult) throws SQLException {
        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW TABLES");
        }
        if(Utils.isNullOrEmpty(schema)){
            throw new RedshiftException("Schema is not allowed to be null or empty to call SHOW TABLES");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling Server API SHOW TABLES on catalog: {0}, schema: {1}, tableNamePattern: {2}", catalog, schema, tableNamePattern);
        }

        List<ShowTablesInfo> showTablesResult = new ArrayList<>();
        try (PreparedStatement stmt = Utils.isNullOrEmpty(tableNamePattern) ?
                createMetaDataPreparedStatement(SQL_PREP_SHOWTABLES) :
                createMetaDataPreparedStatement(SQL_PREP_SHOWTABLESLIKE)) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            if (!Utils.isNullOrEmpty(tableNamePattern)) {
                stmt.setString(3, tableNamePattern);
            }
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    if (fullResult) {
                        showTablesResult.add(new ShowTablesInfo(
                                rs.getString(SHOW_TABLES_DATABASE_NAME),
                                rs.getString(SHOW_TABLES_SCHEMA_NAME),
                                rs.getString(SHOW_TABLES_TABLE_NAME),
                                rs.getString(SHOW_TABLES_TABLE_TYPE),
                                rs.getString(SHOW_TABLES_REMARKS),
                                rs.getString(SHOW_TABLES_OWNER),
                                rs.getString(SHOW_TABLES_LAST_ALTERED_TIME),
                                rs.getString(SHOW_TABLES_LAST_MODIFIED_TIME),
                                rs.getString(SHOW_TABLES_DIST_STYLE),
                                rs.getString(SHOW_TABLES_TABLE_SUBTYPE)
                        ));
                    } else {
                        showTablesResult.add(new ShowTablesInfo(rs.getString(SHOW_TABLES_TABLE_NAME)));
                    }
                }
                return showTablesResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowTables: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowTables: " + e.getMessage(), e);
        }
    }

    /**
     * Helper function to get a ResultSet for SHOW COLUMNS
     * @param catalog The catalog name
     * @param schema The schema name
     * @param table The table name
     * @param columnNamePattern The column name pattern
     * @return Resultset from SHOW COLUMNS
     * @throws SQLException if a database error occurs
     */
    protected List<ShowColumnsInfo> callShowColumns(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW COLUMNS");
        }
        if(Utils.isNullOrEmpty(schema)){
            throw new RedshiftException("Schema is not allowed to be null or empty to call SHOW COLUMNS");
        }
        if(Utils.isNullOrEmpty(table)){
            throw new RedshiftException("Table is not allowed to be null or empty to call SHOW COLUMNS");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling SHOW COLUMNS on catalog: {0}, schema: {1}, table: {2}, columnNamePattern: {3}", catalog, schema, table, columnNamePattern);
        }

        List<ShowColumnsInfo> showColumnsResult = new ArrayList<>();
        try (PreparedStatement stmt = Utils.isNullOrEmpty(columnNamePattern) ?
                createMetaDataPreparedStatement(SQL_PREP_SHOWCOLUMNS) :
                createMetaDataPreparedStatement(SQL_PREP_SHOWCOLUMNSLIKE)) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            stmt.setString(3, table);
            if (!Utils.isNullOrEmpty(columnNamePattern)) {
                stmt.setString(4, columnNamePattern);
            }
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    showColumnsResult.add(new ShowColumnsInfo(
                        rs.getString(SHOW_COLUMNS_DATABASE_NAME),
                        rs.getString(SHOW_COLUMNS_SCHEMA_NAME),
                        rs.getString(SHOW_COLUMNS_TABLE_NAME),
                        rs.getString(SHOW_COLUMNS_COLUMN_NAME),
                        rs.getString(SHOW_COLUMNS_ORDINAL_POSITION),
                        rs.getString(SHOW_COLUMNS_COLUMN_DEFAULT),
                        rs.getString(SHOW_COLUMNS_IS_NULLABLE),
                        rs.getString(SHOW_COLUMNS_DATA_TYPE),
                        rs.getString(SHOW_COLUMNS_CHARACTER_MAXIMUM_LENGTH),
                        rs.getString(SHOW_COLUMNS_NUMERIC_PRECISION),
                        rs.getString(SHOW_COLUMNS_NUMERIC_SCALE),
                        rs.getString(SHOW_COLUMNS_REMARKS),
                        rs.getString(SHOW_COLUMNS_SORT_KEY_TYPE),
                        rs.getString(SHOW_COLUMNS_SORT_KEY),
                        rs.getString(SHOW_COLUMNS_DIST_KEY),
                        rs.getString(SHOW_COLUMNS_ENCODING),
                        rs.getString(SHOW_COLUMNS_COLLATION)
                    ));
                }
                return showColumnsResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowColumns: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowColumns: " + e.getMessage(), e);
        }
    }

    /**
     * Helper function to get a ResultSet of Show Constraint Primary Key
     * @param catalog The catalog name
     * @param schema The schema name
     * @param table The table name
     * @return Resultset from SHOW CONSTRAINTS PRIMARY KEY
     * @throws SQLException if a database error occurs
     */
    protected List<ShowPrimaryKeysInfo> callShowConstraintsPrimaryKey(String catalog, String schema, String table) throws SQLException {

        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW CONSTRAINTS PRIMARY KEY");
        }
        if(Utils.isNullOrEmpty(schema)){
            throw new RedshiftException("Schema is not allowed to be null or empty to call SHOW CONSTRAINTS PRIMARY KEY");
        }
        if(Utils.isNullOrEmpty(table)){
            throw new RedshiftException("Table is not allowed to be null or empty to call SHOW CONSTRAINTS PRIMARY KEY");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling SHOW CONSTRAINTS PRIMARY KEY on catalog = {0}, schema = {1}, table = {2}",
                    catalog, schema, table);
        }

        List<ShowPrimaryKeysInfo> showPrimaryKeysResult = new ArrayList<>();
        try (PreparedStatement stmt = createMetaDataPreparedStatement(SQL_PREP_SHOWPRIMARYKEYS)) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            stmt.setString(3, table);
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    showPrimaryKeysResult.add(new ShowPrimaryKeysInfo(
                            rs.getString(SHOW_PRIMARY_KEYS_DATABASE_NAME),
                            rs.getString(SHOW_PRIMARY_KEYS_SCHEMA_NAME),
                            rs.getString(SHOW_PRIMARY_KEYS_TABLE_NAME),
                            rs.getString(SHOW_PRIMARY_KEYS_COLUMN_NAME),
                            rs.getString(SHOW_PRIMARY_KEYS_KEY_SEQ),
                            rs.getString(SHOW_PRIMARY_KEYS_PK_NAME)
                    ));
                }
                return showPrimaryKeysResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowConstraintsPrimaryKey: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowConstraintsPrimaryKey: " + e.getMessage(), e);
        }
    }

    /**
     * Helper function to get a ResultSet for Show Constraint Foreign Key
     * @param catalog The catalog name
     * @param schema The schema name
     * @param table The table name
     * @param sql the foreign key sql query
     * @return Resultset from SHOW CONSTRAINTS FOREIGN KEY
     * @throws SQLException if a database error occurs
     */
    protected List<ShowForeignKeysInfo> callShowConstraintsForeignKey(String catalog, String schema, String table, String sql) throws SQLException {

        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW CONSTRAINTS FOREIGN KEY");
        }
        if(Utils.isNullOrEmpty(schema)){
            throw new RedshiftException("Schema is not allowed to be null or empty to call SHOW CONSTRAINTS FOREIGN KEY");
        }
        if(Utils.isNullOrEmpty(table)){
            throw new RedshiftException("Table is not allowed to be null or empty to call SHOW CONSTRAINTS FOREIGN KEY");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling SHOW CONSTRAINTS FOREIGN KEY on catalog = {0}, schema = {1}, table = {2}",
                    catalog, schema, table);
        }

        List<ShowForeignKeysInfo> showForeignKeysResult = new ArrayList<>();
        try (PreparedStatement stmt = createMetaDataPreparedStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            stmt.setString(3, table);
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    showForeignKeysResult.add(new ShowForeignKeysInfo(
                            rs.getString(SHOW_FOREIGN_KEYS_PK_DATABASE_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_PK_SCHEMA_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_PK_TABLE_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_PK_COLUMN_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_FK_DATABASE_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_FK_SCHEMA_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_FK_TABLE_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_FK_COLUMN_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_KEY_SEQ),
                            rs.getString(SHOW_FOREIGN_KEYS_UPDATE_RULE),
                            rs.getString(SHOW_FOREIGN_KEYS_DELETE_RULE),
                            rs.getString(SHOW_FOREIGN_KEYS_FK_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_PK_NAME),
                            rs.getString(SHOW_FOREIGN_KEYS_DEFERRABILITY)
                    ));
                }
                return showForeignKeysResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowConstraintsForeignKey: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowConstraintsForeignKey: " + e.getMessage(), e);
        }
    }

    /**
     * Helper function to get a ResultSet for SHOW PROCEDURES
     * @param catalog The catalog name
     * @param schema The schema name
     * @param procedureNamePattern The procedure name pattern
     * @param fullResult If true, returns full result for SHOW; if false, returns only procedure names and argument lists
     * @return Resultset from SHOW PROCEDURES
     * @throws SQLException if a database error occurs
     */
    protected List<ShowProceduresInfo> callShowProcedures(String catalog, String schema, String procedureNamePattern, boolean fullResult) throws SQLException {
        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW PROCEDURES");
        }
        if(Utils.isNullOrEmpty(schema)){
            throw new RedshiftException("Schema is not allowed to be null or empty to call SHOW PROCEDURES");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling SHOW PROCEDURES on catalog: {0}}, schema: {1}}, procedureNamePattern: {2}", catalog, schema, procedureNamePattern);
        }

        List<ShowProceduresInfo> showProceduresResult = new ArrayList<>();
        try (PreparedStatement stmt = Utils.isNullOrEmpty(procedureNamePattern) ?
                createMetaDataPreparedStatement(SQL_PREP_SHOWPROCEDURES) :
                createMetaDataPreparedStatement(SQL_PREP_SHOWPROCEDURESLIKE)) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            if (!Utils.isNullOrEmpty(procedureNamePattern)) {
                stmt.setString(3, procedureNamePattern);
            }
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    if (fullResult) {
                        showProceduresResult.add(new ShowProceduresInfo(
                                rs.getString(SHOW_PROCEDURES_DATABASE_NAME),
                                rs.getString(SHOW_PROCEDURES_SCHEMA_NAME),
                                rs.getString(SHOW_PROCEDURES_PROCEDURE_NAME),
                                rs.getString(SHOW_PROCEDURES_RETURN_TYPE),
                                rs.getString(SHOW_PROCEDURES_ARGUMENT_LIST)
                        ));
                    } else {
                        showProceduresResult.add(new ShowProceduresInfo(
                                rs.getString(SHOW_PROCEDURES_PROCEDURE_NAME),
                                rs.getString(SHOW_PROCEDURES_ARGUMENT_LIST)
                        ));
                    }
                }
                return showProceduresResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowProcedures: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowProcedures: " + e.getMessage(), e);
        }
    }

    /**
     * Helper function to get a ResultSet for SHOW FUNCTIONS
     * @param catalog The catalog name
     * @param schema The schema name
     * @param functionNamePattern The function name pattern
     * @param fullResult If true, returns full result for SHOW; if false, returns only function names and argument lists
     * @return Resultset from SHOW FUNCTIONS
     * @throws SQLException if a database error occurs
     */
    protected List<ShowFunctionsInfo> callShowFunctions(String catalog, String schema, String functionNamePattern, boolean fullResult) throws SQLException {
        if(Utils.isNullOrEmpty(catalog)){
            throw new RedshiftException("Catalog is not allowed to be null or empty to call SHOW FUNCTIONS");
        }
        if(Utils.isNullOrEmpty(schema)){
            throw new RedshiftException("Schema is not allowed to be null or empty to call SHOW FUNCTIONS");
        }

        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Calling SHOW FUNCTIONS on catalog: {0}, schema: {1}, functionNamePattern: {2}", catalog, schema, functionNamePattern);
        }

        List<ShowFunctionsInfo> showFunctionsResult = new ArrayList<>();
        try (PreparedStatement stmt = Utils.isNullOrEmpty(functionNamePattern) ?
                createMetaDataPreparedStatement(SQL_PREP_SHOWFUNCTIONS) :
                createMetaDataPreparedStatement(SQL_PREP_SHOWFUNCTIONSLIKE)) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            if (!Utils.isNullOrEmpty(functionNamePattern)) {
                stmt.setString(3, functionNamePattern);
            }
            stmt.execute();
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    if (fullResult) {
                        showFunctionsResult.add(new ShowFunctionsInfo(
                                rs.getString(SHOW_FUNCTIONS_DATABASE_NAME),
                                rs.getString(SHOW_FUNCTIONS_SCHEMA_NAME),
                                rs.getString(SHOW_FUNCTIONS_FUNCTION_NAME),
                                rs.getString(SHOW_FUNCTIONS_RETURN_TYPE),
                                rs.getString(SHOW_FUNCTIONS_ARGUMENT_LIST)
                        ));
                    } else {
                        showFunctionsResult.add(new ShowFunctionsInfo(
                                rs.getString(SHOW_FUNCTIONS_FUNCTION_NAME),
                                rs.getString(SHOW_FUNCTIONS_ARGUMENT_LIST)
                        ));
                    }
                }
                return showFunctionsResult;
            } catch (SQLException e) {
                throw new RedshiftException("callShowFunctions: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new RedshiftException("callShowFunctions: " + e.getMessage(), e);
        }
    }
}
