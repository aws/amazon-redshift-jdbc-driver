/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;


import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;

import java.sql.*;
import java.util.*;
import java.text.MessageFormat;
import com.amazon.redshift.core.Utils;

public class MetadataServerAPIHelper extends MetadataAPIHelper{

    public MetadataServerAPIHelper(RedshiftConnectionImpl connection) throws SQLException {
        super(connection);
        prst_QUOTE_LITERAL = createMetaDataPreparedStatement(prepare_QUOTE_LITERAL);
        prst_QUOTE_IDENT = createMetaDataPreparedStatement(prepare_QUOTE_IDENT);
    }

    // TODO: Support for prepare not ready yet
    /*PreparedStatement prst_SHOWDATABASES = null;
    PreparedStatement prst_SHOWDATABASESLIKE = null;
    PreparedStatement prst_SHOWSCHEMAS = null;
    PreparedStatement prst_SHOWSCHEMASLIKE = null;
    PreparedStatement prst_SHOWTABLES = null;
    PreparedStatement prst_SHOWCOLUMNS = null;*/
    PreparedStatement prst_QUOTE_LITERAL = null;
    PreparedStatement prst_QUOTE_IDENT = null;


    /**
    * Returns a Result set for SHOW DATABASES
    *
    * @return the result set for SHOW DATABASES
    */
    protected ResultSet getCatalogsServerAPI() throws SQLException {
        ResultSet rs = null;
        try {
            if (RedshiftLogger.isEnable()){
                connection.getLogger().logDebug("Calling Server API SHOW DATABASES");
            }

            rs = runQuery(SQL_SHOWDATABASES);
        } catch (SQLException e) {
            throw new RedshiftException("MetadataServerAPIHelper.getCatalogsServerAPI: " + e.getMessage());
        }

        // TODO: Support for prepare not ready yet
        /*try {
            if(catalog == null){
               if(prst_SHOWDATABASES == null){
                   prst_SHOWDATABASES = createMetaDataPreparedStatement(SQL_PREP_SHOWDATABASES);
               }
               prst_SHOWDATABASES.execute();
               rs = prst_SHOWDATABASES.getResultSet();
            }
            else{
                if(prst_SHOWDATABASESLIKE == null){
                    prst_SHOWDATABASESLIKE = createMetaDataPreparedStatement(SQL_PREP_SHOWDATABASESLIKE);
                }
                prst_SHOWDATABASESLIKE.setObject(1,catalog);
                prst_SHOWDATABASESLIKE.execute();
                rs = prst_SHOWDATABASESLIKE.getResultSet();
            }
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getCatalogsServerAPI prepare statement error: " + e.getMessage());
        }*/

        return rs;
    }

    /**
     * Returns a list of intermediate result set for SHOW SCHEMAS
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param retEmpty boolean to determine if we want to directly return empty result without calling any SHOW command
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW SCHEMAS
     */
    protected List<ResultSet> getSchemasServerAPI(String catalog, String schemaPattern,
                                                  boolean retEmpty, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ResultSet> intermediateRs = new ArrayList<>();

        // TODO: Support for prepare not ready yet
        /*try{
            if(schemaPattern == null){
                if(prst_SHOWSCHEMAS == null){
                   prst_SHOWSCHEMAS = createMetaDataPreparedStatement(SQL_PREP_SHOWSCHEMAS);
               }
            }
            else{
                if(prst_SHOWSCHEMASLIKE == null){
                    prst_SHOWSCHEMASLIKE = createMetaDataPreparedStatement(SQL_PREP_SHOWSCHEMASLIKE);
                }
            }
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getSchemasServerAPI.createMetaDataPreparedStatement" + e.getMessage());
        }*/

        if(!retEmpty){
            try {
                // Get Catalog list
                List<String> catalogList = callGetCatalogList(catalog, isSingleDatabaseMetaData);

                for (String curCatalog : catalogList) {
                    intermediateRs.add(callShowSchemas(curCatalog, schemaPattern));
                }
            } catch (SQLException e) {
                throw new RedshiftException("MetadataServerAPIHelper.getSchemasServerAPI: " + e.getMessage());
            }
        }
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Return intermediate result set for catalog: {0}, schemaPattern: {1}", catalog, schemaPattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW TABLES
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database
     * @param retEmpty boolean to determine if we want to directly return empty result without calling any SHOW command
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW TABLES
     */
    protected List<ResultSet> getTablesServerAPI(String catalog, String schemaPattern, String tableNamePattern,
                                                 boolean retEmpty, boolean isSingleDatabaseMetaData) throws SQLException {

        List<ResultSet> intermediateRs = new ArrayList<>();

        // TODO: Support for prepare not ready yet
        /*try{
            if(prst_SHOWSCHEMASLIKE == null){
                prst_SHOWSCHEMASLIKE = createMetaDataPreparedStatement(SQL_PREP_SHOWSCHEMASLIKE);
            }
            if(prst_SHOWTABLES == null){
                prst_SHOWTABLES = createMetaDataPreparedStatement(SQL_PREP_SHOWTABLES);
            }
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getTablesServerAPI.createMetaDataPreparedStatement" + e.getMessage());
        }*/

        if(!retEmpty) {
            try {
                // Get Catalog list
                List<String> catalogList = callGetCatalogList(catalog, isSingleDatabaseMetaData);

                for(String curCat : catalogList) {
                    ResultSet schemasRs = callShowSchemas(curCat, schemaPattern);
                    while (schemasRs.next()) {
                        intermediateRs.add(callShowTables(curCat, schemasRs.getString(SHOW_SCHEMAS_SCHEMA_NAME), tableNamePattern));
                    }

                }
            } catch (SQLException e) {
                throw new RedshiftException("MetadataServerAPIHelper.getTablesServerAPI: " + e.getMessage());
            }
        }
        if (RedshiftLogger.isEnable()) {
            connection.getLogger().logDebug("Return intermediate result set for catalog = {0}, schemaPattern = {1}, tableNamePattern = {2}",
                    catalog, schemaPattern, tableNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Returns a list of intermediate result set for SHOW COLUMNS
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database
     * @param retEmpty boolean to determine if we want to directly return empty result without calling any SHOW command
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of intermediate result set for SHOW COLUMNS
     */
    protected List<ResultSet> getColumnsServerAPI(String catalog, String schemaPattern, String tableNamePattern,
                                                  String columnNamePattern, boolean retEmpty,
                                                  boolean isSingleDatabaseMetaData) throws SQLException {

        List<ResultSet> intermediateRs = new ArrayList<>();

        // TODO: Support for prepare not ready yet
        /*try{
            if(prst_SHOWSCHEMASLIKE == null){
                prst_SHOWSCHEMASLIKE = createMetaDataPreparedStatement(SQL_PREP_SHOWSCHEMASLIKE);
            }
            if(prst_SHOWTABLES == null){
                prst_SHOWTABLES = createMetaDataPreparedStatement(SQL_PREP_SHOWTABLES);
            }
            if(prst_SHOWCOLUMNS == null){
                prst_SHOWCOLUMNS = createMetaDataPreparedStatement(sqlSHOWCOLUMNS);
            }
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getColumnsServerAPI.createMetaDataPreparedStatement" + e.getMessage());
        }*/

        if(!retEmpty) {
            try {
                // Get Catalog list
                List<String> catalogList = callGetCatalogList(catalog, isSingleDatabaseMetaData);

                for(String curCat : catalogList){
                    ResultSet schemasRs = callShowSchemas(curCat, schemaPattern);
                    while (schemasRs.next()) {
                        String curSchema = schemasRs.getString(SHOW_SCHEMAS_SCHEMA_NAME);
                        ResultSet tablesRs = callShowTables(curCat, curSchema, tableNamePattern);
                        while (tablesRs.next()) {
                            intermediateRs.add(callShowColumns(curCat, curSchema, tablesRs.getString(SHOW_TABLES_TABLE_NAME), columnNamePattern));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RedshiftException("MetadataServerAPIHelper.getColumnsServerAPI: " + e.getMessage());
            }
        }
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Return intermediate result set for catalog = {0}, schemaPattern = {1}, tableNamePattern = {2}, columnNamePattern = {3}",
                    catalog, schemaPattern, tableNamePattern, columnNamePattern);
        }
        return intermediateRs;
    }

    /**
     * Helper function to get a list of catalog name
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; null means that the catalog name should not be used to narrow the search
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of catalog name
     */
    protected List<String> callGetCatalogList(String catalog, boolean isSingleDatabaseMetaData) throws SQLException {
        List<String> catalogList = null;
        try {
            if(Utils.isNullOrEmpty(catalog)){
                catalogList = getCatalogList(isSingleDatabaseMetaData);
            }
            else{
                catalogList = new ArrayList<>();
                if (isSingleDatabaseMetaData) {
                    if (catalog.equals(connection.getCatalog())) {
                        catalogList.add(catalog);
                    }
                } else {
                    catalogList.add(catalog);
                }
            }
        } catch (Exception e) {
            throw new RedshiftException("callGetCatalogList: " + e.getMessage());
        }

        if(catalogList == null){
            throw new RedshiftException("Error when getting catalogList ... ");
        }

        return catalogList;
    }

    /**
     * Helper function to retrieve a list of catalog name from SHOW DATABASES
     *
     * @param isSingleDatabaseMetaData boolean to determine if we want to retrieve metadata information only from current connected database
     * @return the list of catalog name from SHOW DATABASES
     */
    protected List<String> getCatalogList(boolean isSingleDatabaseMetaData) throws SQLException {
        List<String> catalogList = null;
        try {
            String curConnectedCatalog = connection.getCatalog();
            catalogList = new ArrayList<>();
            ResultSet catalogRs = getCatalogsServerAPI();
            while (catalogRs.next()) {
                if (isSingleDatabaseMetaData) {
                    String curCatalog = catalogRs.getString(SHOW_DATABASES_DATABASE_NAME);
                    if (curCatalog.equals(curConnectedCatalog)) {
                        catalogList.add(curCatalog);
                    }
                } else {
                    catalogList.add(catalogRs.getString(SHOW_DATABASES_DATABASE_NAME));
                }
            }
        } catch (SQLException e) {
            throw new RedshiftException("getCatalogList: " + e.getMessage());
        }
        return catalogList;
    }

    /**
     * Helper function to determine whether calling SHOW SCHEMAS with LIKE or not
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name pattern; must match the schema name as it is stored in the database; null means that the schema name should not be used to narrow the search
     * @return result set for SHOW SCHEMAS
     */
    protected ResultSet callShowSchemas(String catalog, String schema) throws SQLException {
        if(Utils.isNullOrEmpty(schema)){
            return callShowSchemasWithOUTLike(catalog);
        }
        else{
            return callShowSchemasWithLike(catalog, schema);
        }
    }

    /**
     * Helper function to call SHOW SCHEMAS
     *
     * @param catalog a catalog name (can't be null)
     * @return result set for SHOW SCHEMAS
     */
    protected ResultSet callShowSchemasWithOUTLike(String catalog) throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling Server API SHOW SCHEMAS on catalog: {0}", catalog);
        }
        return runQuery(MessageFormat.format(SQL_SHOWSCHEMAS, callQuoteIdent(catalog)));
    }

    /**
     * Helper function to call SHOW SCHEMAS with LIKE
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name pattern (can't be null)
     * @return result set for SHOW SCHEMAS with LIKE
     */
    protected ResultSet callShowSchemasWithLike(String catalog, String schema) throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling Server API SHOW SCHEMAS on catalog: {0}, schemaPattern: {1}", catalog, schema);
        }
        return runQuery(MessageFormat.format(SQL_SHOWSCHEMASLIKE, callQuoteIdent(catalog), callQuoteLiteral(schema)));
    }

    /**
     * Helper function to determine whether calling SHOW TABLES with LIKE or not
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name (can't be null or pattern)
     * @param table a table name pattern; must match the table name as it is stored in the database
     * @return result set for SHOW TABLES
     */
    protected ResultSet callShowTables(String catalog, String schema, String table) throws SQLException {
        if(Utils.isNullOrEmpty(table)){
            return callShowTablesWithOUTLike(catalog, schema);
        }
        else{
            return callShowTablesWithLike(catalog, schema, table);
        }
    }

    /**
     * Helper function to call SHOW TABLES
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name (can't be null or pattern)
     * @return result set for SHOW TABLES
     */
    protected ResultSet callShowTablesWithOUTLike(String catalog, String schema) throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling Server API SHOW TABLES on catalog: {0}, schema: {1}", catalog, schema);
        }
        return runQuery(MessageFormat.format(SQL_SHOWTABLES, callQuoteIdent(catalog), callQuoteIdent(schema)));
    }

    /**
     * Helper function to call SHOW TABLES with LIKE
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name (can't be null or pattern)
     * @param table a table name pattern (can't be null)
     * @return result set for SHOW TABLES with LIKE
     */
    protected ResultSet callShowTablesWithLike(String catalog, String schema, String table) throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling Server API SHOW TABLES on catalog: {0}, schema: {1}, tablePattern: {2}", catalog, schema, table);
        }
        return runQuery(MessageFormat.format(SQL_SHOWTABLESLIKE, callQuoteIdent(catalog), callQuoteIdent(schema), callQuoteLiteral(table)));
    }

    /**
     * Helper function to determine whether calling SHOW COLUMNS with LIKE or not
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name (can't be null or pattern)
     * @param table a table name (can't be null or pattern)
     * @param column a column name pattern; must match the column name as it is stored in the database
     * @return result set for SHOW COLUMNS
     */
    protected ResultSet callShowColumns(String catalog, String schema, String table, String column) throws SQLException {
        if(Utils.isNullOrEmpty(column)){
            return callShowColumnsWithOUTLike(catalog, schema, table);
        }
        else{
            return callShowColumnsWithLike(catalog, schema, table, column);
        }
    }

    /**
     * Helper function to call SHOW COLUMNS
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name (can't be null or pattern)
     * @param table a table name (can't be null or pattern)
     * @return result set for SHOW COLUMNS
     */
    protected ResultSet callShowColumnsWithOUTLike(String catalog, String schema, String table) throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling Server API SHOW COLUMNS on catalog: {0}, schema: {1}, table: {2}", catalog, schema, table);
        }
        return runQuery(MessageFormat.format(SQL_SHOWCOLUMNS, callQuoteIdent(catalog), callQuoteIdent(schema), callQuoteIdent(table)));
    }

    /**
     * Helper function to call SHOW COLUMNS with LIKE
     *
     * @param catalog a catalog name (can't be null)
     * @param schema a schema name (can't be null or pattern)
     * @param table a table name pattern (can't be null or pattern)
     * @param column a column name pattern (can't be null)
     * @return result set for SHOW COLUMNS with LIKE
     */
    protected ResultSet callShowColumnsWithLike(String catalog, String schema, String table, String column) throws SQLException {
        if (RedshiftLogger.isEnable()){
            connection.getLogger().logDebug("Calling Server API SHOW COLUMNS on catalog: {0}, schema: {1}, table: {2}, columnPattern: {3}", catalog, schema, table, column);
        }
        return runQuery(MessageFormat.format(SQL_SHOWCOLUMNSLIKE, callQuoteIdent(catalog), callQuoteIdent(schema), callQuoteIdent(table), callQuoteLiteral(column)));
    }

    /**
     * Helper function to call QUOTE_LITERAL and return result as string
     *
     * @param input the input string we want to parse into QUOTE_LITERAL
     * @return the input string with proper quoting
     */
    protected String callQuoteLiteral(String input) throws SQLException {
        prst_QUOTE_LITERAL.setString(QUOTE_LITERAL_parameter_index,input);
        prst_QUOTE_LITERAL.execute();
        ResultSet rs = prst_QUOTE_LITERAL.getResultSet();
        if(rs == null){
            throw new RedshiftException("callQuoteLiteral: Fail to quote literal: " + input);
        }
        rs.next();
        return rs.getString(QUOTE_LITERAL_result_col_index);
    }

    /**
     * Helper function to call QUOTE_IDENT and return result as string
     *
     * @param input the input string we want to parse into QUOTE_IDENT
     * @return the input string with proper quoting
     */
    protected String callQuoteIdent(String input) throws SQLException {
        prst_QUOTE_IDENT.setString(QUOTE_IDENT_parameter_index,input);
        prst_QUOTE_IDENT.execute();
        ResultSet rs = prst_QUOTE_IDENT.getResultSet();
        if(rs == null){
            throw new RedshiftException("callQuoteIdent: Fail to quote identifier: " + input);
        }
        rs.next();
        return rs.getString(QUOTE_IDENT_result_col_index);
    }
}
