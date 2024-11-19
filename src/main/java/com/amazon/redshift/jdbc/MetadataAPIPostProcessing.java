/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.*;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;

import java.sql.*;
import java.util.*;

public class MetadataAPIPostProcessing extends MetadataAPIHelper {

    public MetadataAPIPostProcessing(RedshiftConnectionImpl connection) {
        super(connection);
        this.connection = connection;
    }

    protected final RedshiftConnectionImpl connection; // The connection association


    // Post-processing for metadata API getCatalogs()
    protected ResultSet getCatalogsPostProcessing(ResultSet serverRs) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling getCatalogsPostProcessing");

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> final_data = new ArrayList<>();

        while (serverRs.next()) {
            byte[][] tuple = getEmptyTuple(GET_CATALOGS_COLS, GET_CATALOGS_COLS.length);

            // Apply the post-processing
            tuple[GetCatalogs_Metadata.TABLE_CAT.getIndex()] = encodeStr(serverRs.getString(SHOW_DATABASES_DATABASE_NAME));

            // Add the data row into the tuple list
            final_data.add(new Tuple(tuple));
        }

        return createRs(GET_CATALOGS_COLS, final_data);

    }

    // Post-processing for metadata API getSchemas()
    protected ResultSet getSchemasPostProcessing(List<ResultSet> serverRs, boolean retEmpty) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling getSchemasPostProcessing");

        // Directly return empty ResultSet
        if(retEmpty){
            return createEmptyRs(GET_SCHEMAS_COLS, GET_SCHEMAS_COLS.length);
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> final_data = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing to match the JDBC metadata API spec
        for (ResultSet rs : serverRs) {
            while (rs.next()) {
                byte[][] tuple = getEmptyTuple(GET_SCHEMAS_COLS, GET_SCHEMAS_COLS.length);

                // Apply the post-processing
                tuple[GetSchemas_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(rs.getString(SHOW_SCHEMAS_SCHEMA_NAME));
                tuple[GetSchemas_Metadata.TABLE_CATALOG.getIndex()] = encodeStr(rs.getString(SHOW_SCHEMAS_DATABASE_NAME));

                // Add the data row into the tuple list
                final_data.add(new Tuple(tuple));
            }
        }
        return createRs(GET_SCHEMAS_COLS, final_data);
    }

    // Post-processing for metadata API getTables()
    protected ResultSet getTablesPostProcessing(List<ResultSet> serverRs, boolean retEmpty, String[] types) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling getTablesPostProcessing");

        // Directly return empty ResultSet
        if(retEmpty){
            return createEmptyRs(GET_TABLES_COLS, GET_TABLES_COLS.length);
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> final_data = new ArrayList<>();

        // Create table types filter
        HashSet<String> typeSet = null;
        if(types != null){
            typeSet = new HashSet<>(Arrays.asList((types)));
        }

        // Loop through all the ResultSet received from Server API call and apply post-processing to match the JDBC metadata API spec
        for (ResultSet rs : serverRs) {
            while (rs.next()) {
                // Apply the table types filter
                if(types == null || typeSet.contains(rs.getString(SHOW_TABLES_TABLE_TYPE))) {
                    byte[][] tuple = getEmptyTuple(GET_TABLES_COLS,GET_TABLES_COLS.length);

                    // Apply the post-processing
                    tuple[GetTables_Metadata.TABLE_CAT.getIndex()] = encodeStr(rs.getString(SHOW_TABLES_DATABASE_NAME));
                    tuple[GetTables_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(rs.getString(SHOW_TABLES_SCHEMA_NAME));
                    tuple[GetTables_Metadata.TABLE_NAME.getIndex()] = encodeStr(rs.getString(SHOW_TABLES_TABLE_NAME));
                    tuple[GetTables_Metadata.TABLE_TYPE.getIndex()] = encodeStr(rs.getString(SHOW_TABLES_TABLE_TYPE));
                    tuple[GetTables_Metadata.REMARKS.getIndex()] = encodeStr(rs.getString(SHOW_TABLES_REMARKS));

                    // Add the data row into the tuple list
                    final_data.add(new Tuple(tuple));
                }
            }
        }
        return createRs(GET_TABLES_COLS, final_data);
    }

    // Post-processing for metadata API getColumns()
    protected ResultSet getColumnsPostProcessing(List<ResultSet> serverRs, boolean retEmpty) throws SQLException {
        if (RedshiftLogger.isEnable())
            connection.getLogger().logDebug("Calling getColumnsPostProcessing");

        // Directly return empty ResultSet
        if(retEmpty){
            return createEmptyRs(GET_COLUMNS_COLS, GET_COLUMNS_COLS.length);
        }

        // Create a Tuple list to store the data row for final ResultSet
        List<Tuple> final_data = new ArrayList<>();

        // Loop through all the ResultSet received from Server API call and apply post-processing to match the JDBC metadata API spec
        for (ResultSet rs : serverRs) {
            while (rs.next()) {
                String dataType = rs.getString(SHOW_COLUMNS_DATA_TYPE);
                String rsType;

                // Retrieve customize precision from DATETIME/INTERVAL data type
                int precisions = 0;
                boolean dateTimeCustomizePrecision = false;
                if(dataType.matches("(time|timetz|timestamp|timestamptz)\\(\\d+\\).*") || dataType.matches("interval.*.\\(\\d+\\)")){
                    rsType = getRSType(dataType.replaceAll("\\(\\d+\\)", "").replaceFirst("\\s++$", ""));
                    precisions = Integer.parseInt(dataType.replaceAll(".*\\(([0-9]+)\\).*", "$1"));
                    dateTimeCustomizePrecision = true;
                }
                else{
                    rsType = getRSType(dataType);
                }

                String sqlType = getSQLType(rsType);
                String autoIncrement = getAutoIncrement(rs.getString(SHOW_COLUMNS_COLUMN_DEFAULT));

                byte[][] tuple = getEmptyTuple(GET_COLUMNS_COLS,GET_COLUMNS_COLS.length);

                // Apply the post-processing
                tuple[GetColumns_Metadata.TABLE_CAT.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_DATABASE_NAME));
                tuple[GetColumns_Metadata.TABLE_SCHEM.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_SCHEMA_NAME));
                tuple[GetColumns_Metadata.TABLE_NAME.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_TABLE_NAME));
                tuple[GetColumns_Metadata.COLUMN_NAME.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_COLUMN_NAME));
                tuple[GetColumns_Metadata.DATA_TYPE.getIndex()] = encodeStr(sqlType);
                tuple[GetColumns_Metadata.TYPE_NAME.getIndex()] = encodeStr(rsType);
                tuple[GetColumns_Metadata.COLUMN_SIZE.getIndex()] = encodeStr(getColumnSize(rsType, rs.getString(SHOW_COLUMNS_CHARACTER_MAXIMUM_LENGTH), rs.getString(SHOW_COLUMNS_NUMERIC_PRECISION)));
                tuple[GetColumns_Metadata.BUFFER_LENGTH.getIndex()] = null;                    // Unused
                tuple[GetColumns_Metadata.DECIMAL_DIGITS.getIndex()] = encodeStr(getDecimalDigit(rsType, rs.getString(SHOW_COLUMNS_NUMERIC_SCALE), precisions, dateTimeCustomizePrecision));
                tuple[GetColumns_Metadata.NUM_PREC_RADIX.getIndex()] = encodeStr(getNumPrefixRadix(rs.getString(SHOW_COLUMNS_DATA_TYPE)));
                tuple[GetColumns_Metadata.NULLABLE.getIndex()] = encodeStr(getNullable(rs.getString(SHOW_COLUMNS_IS_NULLABLE)));
                tuple[GetColumns_Metadata.REMARKS.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_REMARKS));
                tuple[GetColumns_Metadata.COLUMN_DEF.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_COLUMN_DEFAULT));
                tuple[GetColumns_Metadata.SQL_DATA_TYPE.getIndex()] = encodeStr(sqlType);      // It's unused in the spec, but previously return SQL type
                tuple[GetColumns_Metadata.SQL_DATETIME_SUB.getIndex()] = null;                 // Unused
                tuple[GetColumns_Metadata.CHAR_OCTET_LENGTH.getIndex()] = encodeStr(getColumnSize(rsType, rs.getString(SHOW_COLUMNS_CHARACTER_MAXIMUM_LENGTH), rs.getString(SHOW_COLUMNS_NUMERIC_PRECISION))); // CHAR_OCTET_LENGTH return same value as COLUMN_SIZE?
                tuple[GetColumns_Metadata.ORDINAL_POSITION.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_ORDINAL_POSITION));
                tuple[GetColumns_Metadata.IS_NULLABLE.getIndex()] = encodeStr(rs.getString(SHOW_COLUMNS_IS_NULLABLE));
                tuple[GetColumns_Metadata.SCOPE_CATALOG.getIndex()] = null;
                tuple[GetColumns_Metadata.SCOPE_SCHEMA.getIndex()] = null;
                tuple[GetColumns_Metadata.SCOPE_TABLE.getIndex()] = null;
                tuple[GetColumns_Metadata.SOURCE_DATA_TYPE.getIndex()] = null;     // Since Redshift doesn't support either distinct or user-defined reference types, it should return null
                tuple[GetColumns_Metadata.IS_AUTOINCREMENT.getIndex()] = encodeStr(autoIncrement);
                tuple[GetColumns_Metadata.IS_GENERATEDCOLUMN.getIndex()] = encodeStr(autoIncrement);

                // Add the data row into the tuple list
                final_data.add(new Tuple(tuple));
            }
        }
        return createRs(GET_COLUMNS_COLS, final_data);
    }
}
