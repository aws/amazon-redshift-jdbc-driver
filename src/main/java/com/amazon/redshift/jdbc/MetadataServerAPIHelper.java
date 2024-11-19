/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;


import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;

import java.sql.*;
import java.util.*;
import java.text.MessageFormat;

public class MetadataServerAPIHelper extends MetadataAPIHelper{

    public MetadataServerAPIHelper(RedshiftConnectionImpl connection) {
        super(connection);
    }

    // TODO: Support for prepare not ready yet
    /*PreparedStatement prst_SHOWDATABASES = null;
    PreparedStatement prst_SHOWDATABASESLIKE = null;
    PreparedStatement prst_SHOWSCHEMAS = null;
    PreparedStatement prst_SHOWSCHEMASLIKE = null;
    PreparedStatement prst_SHOWTABLES = null;
    PreparedStatement prst_SHOWCOLUMNS = null;*/

    protected ResultSet getCatalogsServerAPI(String catalog) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling Server API SHOW DATABASES");

        String sql = null;
        ResultSet rs = null;

        if(catalog == null){
            sql = SQL_SHOWDATABASES;
        }
        else{
            sql = MessageFormat.format(SQL_SHOWDATABASESLIKE, catalog);
        }

        rs = runQuery(sql);

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

        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Successfully executed SHOW DATABASE for catalog = {0}", catalog);

        return rs;
    }

    protected List<ResultSet> getSchemasServerAPI(String catalog, String schemaPattern,
                                            boolean retEmpty, boolean isSingleDatabaseMetaData) throws SQLException {

        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling Server API SHOW SCHEMAS");

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
            List<String> catalogList;

            try {
                // Get Catalog list
                if(checkNameIsExactName(catalog)){
                    catalogList = new ArrayList<>();
                    catalogList.add(catalog);
                }
                else{
                    catalogList = getCatalogList(catalog, isSingleDatabaseMetaData);
                }

            } catch(Exception e){
                throw new RedshiftException("MetadataServerAPIHelper.getSchemasServerAPI.getCatalogList " + e.getMessage());
            }

            if(catalogList == null){
                throw new RedshiftException("Error when creating catalogList ... ");
            }

            for (String curCatalog : catalogList) {
                intermediateRs.add(callShowSchemas(curCatalog, schemaPattern));
            }
            if (RedshiftLogger.isEnable())
                connection.getLogger().logDebug("Successfully executed SHOW SCHEMAS for catalog = {0}, schemaPattern = {1}", catalog, schemaPattern);
        }
        return intermediateRs;
    }

    protected List<ResultSet> getTablesServerAPI(String catalog, String schemaPattern, String tableNamePattern,
                                         boolean retEmpty, boolean isSingleDatabaseMetaData) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling Server API SHOW TABLES");

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
            List<String> catalogList;

            try {
                // Get Catalog list
                if(checkNameIsExactName(catalog)){
                    catalogList = new ArrayList<>();
                    catalogList.add(catalog);
                }
                else{
                    catalogList = getCatalogList(catalog, isSingleDatabaseMetaData);
                }

            } catch (Exception e) {
                throw new RedshiftException("MetadataServerAPIHelper.getTablesServerAPI.getCatalogList " + e.getMessage());
            }

            if(catalogList == null){
                throw new RedshiftException("Error when creating catalogList ... ");
            }

            for(String curCat : catalogList) {
                // Skip SHOW SCHEMAS API call if catalog name and schema name is exact name instead of pattern
                // TODO: the logic is confusing. Need  a follow up to address this
                if (checkNameIsExactName(schemaPattern) && catalogList.size() == 1) {
                    intermediateRs.add(callShowTables(curCat, schemaPattern, tableNamePattern));
                } else {
                    ResultSet schemasRs = callShowSchemas(curCat, schemaPattern);
                    while (schemasRs.next()) {
                        intermediateRs.add(callShowTables(curCat, schemasRs.getString(SHOW_SCHEMAS_SCHEMA_NAME), tableNamePattern));
                    }
                }
            }
            if (RedshiftLogger.isEnable()) {
                connection.getLogger().logDebug("Successfully executed SHOW TABLES for catalog = {0}, schemaPattern = {1}, tableNamePattern = {2}",
                        catalog, schemaPattern, tableNamePattern);
            }
        }
        return intermediateRs;
    }

    protected List<ResultSet> getColumnsServerAPI(String catalog, String schemaPattern, String tableNamePattern,
                                                  String columnNamePattern, boolean retEmpty,
                                                  boolean isSingleDatabaseMetaData) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling Server API SHOW COLUMNS");

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
            List<String> catalogList;

            try {
                // Get Catalog list
                if(checkNameIsExactName(catalog)){
                    catalogList = new ArrayList<>();
                    catalogList.add(catalog);
                }
                else{
                    catalogList = getCatalogList(catalog, isSingleDatabaseMetaData);
                }

            } catch(Exception e){
                throw new RedshiftException("MetadataServerAPIHelper.getColumnsServerAPI.getCatalogList " + e.getMessage());
            }

            if(catalogList == null){
                throw new RedshiftException("Error when creating catalogList ... ");
            }

            for(String curCat : catalogList){
                // Skip SHOW SCHEMAS API call if catalog name and schema name is exact name instead of pattern
                // TODO: the logic is confusing. Need  a follow up to address this
                if (checkNameIsExactName(schemaPattern) && catalogList.size() == 1) {
                    // Skip SHOW TABLES API call if table name is not a pattern
                    if (checkNameIsExactName(tableNamePattern)) {
                        intermediateRs.add(callShowColumns(curCat, schemaPattern, tableNamePattern, columnNamePattern));
                    }
                    else{
                        ResultSet tablesRs = callShowTables(curCat, schemaPattern, tableNamePattern);
                        while (tablesRs.next()) {
                            intermediateRs.add(callShowColumns(curCat, schemaPattern, tablesRs.getString(SHOW_TABLES_TABLE_NAME), columnNamePattern));
                        }
                    }
                }
                else{
                    ResultSet schemasRs = callShowSchemas(curCat, schemaPattern);
                    while (schemasRs.next()) {
                        // Skip SHOW TABLES API call if table name is not a pattern
                        if (checkNameIsExactName(tableNamePattern)) {
                            intermediateRs.add(callShowColumns(curCat, schemasRs.getString(SHOW_SCHEMAS_SCHEMA_NAME), tableNamePattern, columnNamePattern));
                        }
                        else{
                            ResultSet tablesRs = callShowTables(curCat, schemasRs.getString(SHOW_SCHEMAS_SCHEMA_NAME), tableNamePattern);
                            while (tablesRs.next()) {
                                intermediateRs.add(callShowColumns(curCat, schemasRs.getString(SHOW_SCHEMAS_SCHEMA_NAME), tablesRs.getString(SHOW_TABLES_TABLE_NAME), columnNamePattern));
                            }
                        }

                    }
                }
            }

            if (RedshiftLogger.isEnable()){
                connection.getLogger().logDebug("Successfully executed SHOW COLUMNS for catalog = {0}, schema = {1}, tableName = {2}, columnNamePattern = {3}",
                        catalog, schemaPattern, tableNamePattern, columnNamePattern);
            }
        }
        return intermediateRs;
    }

    protected List<String> getCatalogList(String catalog, boolean isSingleDatabaseMetaData) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Create catalog list for catalog = {0}", catalog);

        String curConnectedCatalog = connection.getCatalog();
        List<String> catalogList = new ArrayList<>();
        ResultSet catalogRs = getCatalogsServerAPI(catalog);
        while (catalogRs.next()) {
            if(isSingleDatabaseMetaData){
                String curCatalog = catalogRs.getString(SHOW_DATABASES_DATABASE_NAME);
                if(curCatalog.equals(curConnectedCatalog)){
                    catalogList.add(curCatalog);
                }
            }
            else{
                catalogList.add(catalogRs.getString(SHOW_DATABASES_DATABASE_NAME));
            }
        }
        return catalogList;
    }

    protected ResultSet callShowSchemas(String catalog, String schema) throws SQLException {
        String sqlSCHEMA;
        if (checkNameIsNotPattern(schema)) {
            sqlSCHEMA = MessageFormat.format(SQL_SHOWSCHEMAS, catalog);
        } else {
            sqlSCHEMA = MessageFormat.format(SQL_SHOWSCHEMASLIKE, catalog, schema);
        }
        return runQuery(sqlSCHEMA);

        // TODO: Support for prepare not ready yet
        /*try{
            prst_SHOWSCHEMASLIKE.setObject(1,curCat);
            prst_SHOWSCHEMASLIKE.setObject(2,schemaPattern);
            prst_SHOWSCHEMASLIKE.execute();
            return prst_SHOWSCHEMASLIKE.getResultSet();
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getTablesServerAPI prepare statement error: " + e.getMessage());
        }*/
    }

    protected ResultSet callShowTables(String catalog, String schema, String table) throws SQLException {
        String sqlTABLE;
        if(checkNameIsNotPattern(table)){
            sqlTABLE = MessageFormat.format(SQL_SHOWTABLES, catalog, schema);
        }
        else{
            sqlTABLE = MessageFormat.format(SQL_SHOWTABLESLIKE, catalog, schema, table);
        }
        return runQuery(sqlTABLE);

        //TODO: Support for prepare not ready yet
        /*try {
            prst_SHOWTABLES.setObject(1,curCat);
            prst_SHOWTABLES.setObject(2,curSchema);
            prst_SHOWTABLES.setObject(3,tableNamePattern);
            prst_SHOWTABLES.execute();

            return prst_SHOWTABLES.getResultSet();
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getTablesServerAPI prepare statement error: " + e.getMessage());
        }*/
    }

    protected ResultSet callShowColumns(String catalog, String schema, String table, String column) throws SQLException {
        String sqlCOLUMN;
        if(checkNameIsNotPattern(column)){
            sqlCOLUMN = MessageFormat.format(SQL_SHOWCOLUMNS, catalog, schema, table);
        }
        else{
            sqlCOLUMN = MessageFormat.format(SQL_SHOWCOLUMNSLIKE, catalog, schema, table, column);
        }
        return runQuery(sqlCOLUMN);

        //TODO: Support for prepare not ready yet
        /*try {
            prst_SHOWCOLUMNS.setObject(1,curCat);
            prst_SHOWCOLUMNS.setObject(2,curSchema);
            prst_SHOWCOLUMNS.setObject(3,tableNamePattern);
            prst_SHOWCOLUMNS.setObject(4,columnNamePattern);
            prst_SHOWCOLUMNS.execute();

            return prst_SHOWCOLUMNS.getResultSet();
        } catch (SQLException e){
            throw new RedshiftException("MetadataServerAPIHelper.getTablesServerAPI prepare statement error: " + e.getMessage());
        }*/
    }
}
