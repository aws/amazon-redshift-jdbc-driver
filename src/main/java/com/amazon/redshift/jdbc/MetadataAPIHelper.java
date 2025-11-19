/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.*;
import com.amazon.redshift.util.RedshiftException;

import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetadataAPIHelper {
  public MetadataAPIHelper(RedshiftConnectionImpl connection) {
    this.connection = connection;

    this.GET_CATALOGS_COLS = getCatalogsField();

    this.GET_SCHEMAS_COLS = getSchemasField();

    this.GET_TABLES_COLS = getTablesField();

    this.GET_COLUMNS_COLS = getColumnsField();

    this.GET_PRIMARY_KEYS_COLS = getPrimaryKeysField();

    this.GET_FOREIGN_KEYS_COLS = getForeignKeysField();

    this.GET_BEST_ROW_IDENTIFIER_COLS = getBestRowIdentifierField();

    this.GET_COLUMN_PRIVILEGES_COLS = getColumnPrivileges();

    this.GET_TABLE_PRIVILEGES_COLS = getTablePrivileges();

    this.GET_PROCEDURES_COLS = getProcedures();

    this.GET_PROCEDURES_COLUMNS_COLS = getProceduresColumns();

    this.GET_FUNCTIONS_COLS = getFunctions();

    this.GET_FUNCTIONS_COLUMNS_COLS = getFunctionsColumns();

    this.GET_TABLE_TYPE_COLS = getTableType();

  }

  protected final RedshiftConnectionImpl connection; // The connection association

  /**
   * Data container for procedure and function column metadata used by getProcedureColumns() and getFunctionColumns().
   *
   * <p>These JDBC metadata methods require a "specific name" to uniquely identify procedures/functions that may
   * have the same name but different parameter signatures (overloading). The specific name is constructed by
   * combining the procedure/function name with its argument list, which matches the format returned by
   * SHOW PROCEDURES/SHOW FUNCTIONS commands.
   *
   * <p>This class associates the specific name with the complete parameter information retrieved from
   * SHOW PARAMETERS, enabling proper mapping between the metadata calls and the underlying database objects.
   *
   * <p><strong>Usage Pattern:</strong>
   * <ol>
   *   <li>Execute SHOW PROCEDURES/FUNCTIONS to get procedure names and argument lists</li>
   *   <li>Build specific names from name + argument list</li>
   *   <li>Execute SHOW PARAMETERS for each procedure/function</li>
   *   <li>Store results in this container for later retrieval by JDBC metadata methods</li>
   * </ol>
   *
   * @see java.sql.DatabaseMetaData#getProcedureColumns(String, String, String, String)
   * @see java.sql.DatabaseMetaData#getFunctionColumns(String, String, String, String)
   */
  protected static class ProcedureFunctionColumnData{
    private final String specificName;
    private final List<ShowParametersInfo> resultSet;

    public ProcedureFunctionColumnData(String specificName, List<ShowParametersInfo> resultSet) {
      this.specificName = specificName;
      this.resultSet = resultSet;
    }

    public String getSpecificName() {
      return specificName;
    }

    public List<ShowParametersInfo> getResultSet() {
      return resultSet;
    }
  }

  /**
   * Data container for the getBestRowIdentifier() metadata operation.
   *
   * <p>The JDBC getBestRowIdentifier() method returns column metadata for the best set of columns
   * that uniquely identifies a table row. In Redshift JDBC, we're returning the primary key columns.
   *
   * <p>This class optimizes the lookup process by:
   * <ol>
   *   <li>Retrieving all primary key column names and storing them in a HashSet for O(1) lookup</li>
   *   <li>Getting complete column metadata for the table via SHOW COLUMNS</li>
   *   <li>Filtering the column metadata to return only primary key columns</li>
   * </ol>
   *
   * <p>The HashSet approach provides efficient filtering when tables have many columns but few
   * primary key columns, avoiding the need for nested loops during the filtering process.
   *
   * @see java.sql.DatabaseMetaData#getBestRowIdentifier(String, String, String, int, boolean)
   */
  public static class BestRowIdenData {
    private final HashSet<String> pkColumnSet;
    private final List<ShowColumnsInfo> resultSet;

    public BestRowIdenData(List<ShowColumnsInfo> resultSet, HashSet<String> pkColumnSet) {
      this.resultSet = resultSet;
      this.pkColumnSet = pkColumnSet;
    }

    public HashSet<String> getPkColumnSet() {
      return pkColumnSet;
    }

    public List<ShowColumnsInfo> getResultSet() {
      return resultSet;
    }
  }

  protected static class RedshiftDataTypes {
    private static final Set<String> VALID_TYPES = new HashSet<>(Arrays.asList(
      // Numeric types
      "smallint", "int2",
      "integer", "int", "int4",
      "bigint", "int8",
      "decimal", "numeric",
      "real", "float4",
      "double precision", "float8", "float",

      // Character types
      "char", "character", "nchar", "bpchar",
      "varchar", "character varying", "nvarchar", "text",

      // Date/Time types
      "date",
      "time", "time without time zone",
      "timetz", "time with time zone",
      "timestamp", "timestamp without time zone",
      "timestamptz", "timestamp with time zone",
      "intervaly2m", "interval year to month",
      "intervald2s", "interval day to second",

      // Other types
      "boolean", "bool",
      "hllsketch",
      "super",
      "varbyte", "varbinary", "binary varying",
      "geometry",
      "geography",

      // Legacy data types
      "oid",
      "smallint[]",
      "pg_attribute",
      "pg_type",
      "refcursor"
    ));

    /**
     * Validates if a single data type is valid.
     */
    private static boolean isValidType(String dataType) {
      return dataType != null && VALID_TYPES.contains(dataType.toLowerCase());
    }

    /**
     * Validates a list of data types and returns invalid ones.
     */
    private static List<String> validateTypes(List<String> dataTypes) {
      return dataTypes.stream()
              .filter(type -> !isValidType(type))
              .collect(Collectors.toList());
    }
  }

  // Define constant value for metadata API getCatalogs() ResultSet
  protected final Field[] GET_CATALOGS_COLS;

  // Define constant value for metadata API getSchemas() ResultSet
  protected final Field[] GET_SCHEMAS_COLS;

  // Define constant value for metadata API getTables() ResultSet
  protected final Field[] GET_TABLES_COLS;

  // Define constant value for metadata API getColumns() ResultSet
  protected final Field[] GET_COLUMNS_COLS;

  // Define constant value for metadata API getPrimaryKeys() ResultSet
  protected final Field[] GET_PRIMARY_KEYS_COLS;

  // Define constant value for metadata API getImportedKeys() and getExportedKeys() ResultSet
  protected final Field[] GET_FOREIGN_KEYS_COLS;

  // Define constant value for metadata API getBestRowIdentifier() ResultSet
  protected final Field[] GET_BEST_ROW_IDENTIFIER_COLS;

  // Define constant value for metadata API getColumnPrivileges() ResultSet
  protected final Field[] GET_COLUMN_PRIVILEGES_COLS;

  // Define constant value for metadata API getTablePrivileges() ResultSet
  protected final Field[] GET_TABLE_PRIVILEGES_COLS;

  // Define constant value for metadata API getProcedures() ResultSet
  protected final Field[] GET_PROCEDURES_COLS;

  // Define constant value for metadata API getProceduresColumns() ResultSet
  protected final Field[] GET_PROCEDURES_COLUMNS_COLS;

  // Define constant value for metadata API getFunctions() ResultSet
  protected final Field[] GET_FUNCTIONS_COLS;

  // Define constant value for metadata API getFunctionsColumns() ResultSet
  protected final Field[] GET_FUNCTIONS_COLUMNS_COLS;

  // Define constant value for metadata API getTableType() ResultSet
  protected final Field[] GET_TABLE_TYPE_COLS;

  // Define constant value for metadata API getImportedKey ResultSet
  protected final short IMPORTED_KEY_NO_ACTION = 3;

  // Define constant value for metadata API getImportedKey() ResultSet
  protected final short IMPORTED_KEY_NOT_DEFERRABLE = 7;

  /**
    * Enum representing the indices of columns in the result set of getImportedKeys/getExportedKeys.
    * Each enum constant corresponds to a specific column in the result set.
    */
  protected enum ForeignKeyColumnIndex {
    PK_CATALOG(0),
    PK_SCHEMA(1),
    PK_TABLE(2),
    PK_COLUMN(3),
    FK_CATALOG(4),
    FK_SCHEMA(5),
    FK_TABLE(6),
    FK_COLUMN(7),
    KEY_SEQ(8);

    private final int index;

    /**
     * Constructor for ForeignKeyColumnIndex enum.
     *
     * @param index The index of the column in the result set.
     */
    ForeignKeyColumnIndex(int index) {
       this.index = index;
    }

    /**
     * Get the index value of the enum constant.
     *
     * @return The index of the column.
     */
    public int getIndex() {
        return index;
    }
  }

  // Define class for metadataAPI result set metadata
  private static class BaseMetadata{
    private final int colIndex;
    private final String colName;
    private final int colOidType;

    BaseMetadata(int colIndex, String colName, int colOidType) {
      this.colIndex = colIndex;
      this.colName = colName;
      this.colOidType = colOidType;
    }

    public int getIndex() {
      return colIndex;
    }

    public String getName() {
      return colName;
    }

    public int getOidType() {
      return colOidType;
    }
  }

  // Define return columns for JDBC API getCatalogs according to the spec:
  // https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getCatalogs--
  public enum GetCatalogs_Metadata implements enumFunc<GetCatalogs_Metadata>{
    TABLE_CAT(new BaseMetadata(0, "TABLE_CAT", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetCatalogs_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  // Define return columns for JDBC API getSchemas according to the spec:
  // https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getSchemas-java.lang.String-java.lang.String-
  public enum GetSchemas_Metadata implements enumFunc<GetSchemas_Metadata>{
    TABLE_SCHEM(new BaseMetadata(0, "TABLE_SCHEM", Oid.VARCHAR)),
    TABLE_CATALOG(new BaseMetadata(1, "TABLE_CATALOG", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetSchemas_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  // Define return columns for JDBC API getTables according to the spec:
  // https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTables-java.lang.String-java.lang.String-java.lang.String-java.lang.String:A-
  public enum GetTables_Metadata implements enumFunc<GetTables_Metadata>{
    TABLE_CAT(new BaseMetadata(0, "TABLE_CAT", Oid.VARCHAR)),
    TABLE_SCHEM(new BaseMetadata(1, "TABLE_SCHEM", Oid.VARCHAR)),
    TABLE_NAME(new BaseMetadata(2, "TABLE_NAME", Oid.VARCHAR)),
    TABLE_TYPE(new BaseMetadata(3, "TABLE_TYPE", Oid.VARCHAR)),
    REMARKS(new BaseMetadata(4, "REMARKS", Oid.VARCHAR)),
    TYPE_CAT(new BaseMetadata(5, "TYPE_CAT", Oid.VARCHAR)),
    TYPE_SCHEM(new BaseMetadata(6, "TYPE_SCHEM", Oid.VARCHAR)),
    TYPE_NAME(new BaseMetadata(7, "TYPE_NAME", Oid.VARCHAR)),
    SELF_REFERENCING_COL_NAME(new BaseMetadata(8, "SELF_REFERENCING_COL_NAME", Oid.VARCHAR)),
    REF_GENERATION(new BaseMetadata(9, "REF_GENERATION", Oid.VARCHAR)),
    OWNER(new BaseMetadata(10, "OWNER", Oid.VARCHAR)),
    LAST_ALTERED_TIME(new BaseMetadata(11, "LAST_ALTERED_TIME", Oid.TIMESTAMP)),
    LAST_MODIFIED_TIME(new BaseMetadata(12, "LAST_MODIFIED_TIME", Oid.TIMESTAMP)),
    DIST_STYLE(new BaseMetadata(13, "DIST_STYLE", Oid.VARCHAR)),
    TABLE_SUBTYPE(new BaseMetadata(14, "TABLE_SUBTYPE", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetTables_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  // Define return columns for JDBC API getColumns according to the spec:
  // https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getColumns-java.lang.String-java.lang.String-java.lang.String-java.lang.String-
  public enum GetColumns_Metadata implements enumFunc<GetColumns_Metadata>{
    TABLE_CAT(new BaseMetadata(0, "TABLE_CAT", Oid.VARCHAR)),
    TABLE_SCHEM(new BaseMetadata(1, "TABLE_SCHEM", Oid.VARCHAR)),
    TABLE_NAME(new BaseMetadata(2, "TABLE_NAME", Oid.VARCHAR)),
    COLUMN_NAME(new BaseMetadata(3, "COLUMN_NAME", Oid.VARCHAR)),
    DATA_TYPE(new BaseMetadata(4, "DATA_TYPE", Oid.INT4)),
    TYPE_NAME(new BaseMetadata(5, "TYPE_NAME", Oid.VARCHAR)),
    COLUMN_SIZE(new BaseMetadata(6, "COLUMN_SIZE", Oid.INT4)),
    BUFFER_LENGTH(new BaseMetadata(7, "BUFFER_LENGTH", Oid.INT4)),
    DECIMAL_DIGITS(new BaseMetadata(8, "DECIMAL_DIGITS", Oid.INT4)),
    NUM_PREC_RADIX(new BaseMetadata(9, "NUM_PREC_RADIX", Oid.INT4)),
    NULLABLE(new BaseMetadata(10, "NULLABLE", Oid.INT4)),
    REMARKS(new BaseMetadata(11, "REMARKS", Oid.VARCHAR)),
    COLUMN_DEF(new BaseMetadata(12, "COLUMN_DEF", Oid.VARCHAR)),
    SQL_DATA_TYPE(new BaseMetadata(13, "SQL_DATA_TYPE", Oid.INT4)),
    SQL_DATETIME_SUB(new BaseMetadata(14, "SQL_DATETIME_SUB", Oid.INT4)),
    CHAR_OCTET_LENGTH(new BaseMetadata(15, "CHAR_OCTET_LENGTH", Oid.INT4)),
    ORDINAL_POSITION(new BaseMetadata(16, "ORDINAL_POSITION", Oid.INT4)),
    IS_NULLABLE(new BaseMetadata(17, "IS_NULLABLE", Oid.VARCHAR)),
    SCOPE_CATALOG(new BaseMetadata(18, "SCOPE_CATALOG", Oid.VARCHAR)),
    SCOPE_SCHEMA(new BaseMetadata(19, "SCOPE_SCHEMA", Oid.VARCHAR)),
    SCOPE_TABLE(new BaseMetadata(20, "SCOPE_TABLE", Oid.VARCHAR)),
    SOURCE_DATA_TYPE(new BaseMetadata(21, "SOURCE_DATA_TYPE", Oid.INT2)),
    IS_AUTOINCREMENT(new BaseMetadata(22, "IS_AUTOINCREMENT", Oid.VARCHAR)),
    IS_GENERATEDCOLUMN(new BaseMetadata(23, "IS_GENERATEDCOLUMN", Oid.VARCHAR)),
    SORT_KEY_TYPE(new BaseMetadata(24, "SORT_KEY_TYPE", Oid.VARCHAR)),
    SORT_KEY(new BaseMetadata(25, "SORT_KEY", Oid.INT4)),
    DIST_KEY(new BaseMetadata(26, "DIST_KEY", Oid.INT4)),
    ENCODING(new BaseMetadata(27, "ENCODING", Oid.VARCHAR)),
    COLLATION(new BaseMetadata(28, "COLLATION", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetColumns_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getPrimaryKeys according to the spec:
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getPrimaryKeys-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetPrimaryKeys_Metadata implements enumFunc<GetPrimaryKeys_Metadata>{
    TABLE_CAT(new BaseMetadata(0, "TABLE_CAT", Oid.VARCHAR)),
    TABLE_SCHEM(new BaseMetadata(1, "TABLE_SCHEM", Oid.VARCHAR)),
    TABLE_NAME(new BaseMetadata(2, "TABLE_NAME", Oid.VARCHAR)),
    COLUMN_NAME(new BaseMetadata(3, "COLUMN_NAME", Oid.VARCHAR)),
    KEY_SEQ(new BaseMetadata(4, "KEY_SEQ", Oid.INT2)),
    PK_NAME(new BaseMetadata(5, "PK_NAME", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetPrimaryKeys_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getImportedKeys according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getImportedKeys-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getExportedKeys-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetForeignKeys_Metadata implements enumFunc<GetForeignKeys_Metadata> {
    PKTABLE_CAT(new BaseMetadata(0, "PKTABLE_CAT", Oid.VARCHAR)),
    PKTABLE_SCHEM(new BaseMetadata(1, "PKTABLE_SCHEM", Oid.VARCHAR)),
    PKTABLE_NAME(new BaseMetadata(2, "PKTABLE_NAME", Oid.VARCHAR)),
    PKCOLUMN_NAME(new BaseMetadata(3, "PKCOLUMN_NAME", Oid.VARCHAR)),
    FKTABLE_CAT(new BaseMetadata(4, "FKTABLE_CAT", Oid.VARCHAR)),
    FKTABLE_SCHEM(new BaseMetadata(5, "FKTABLE_SCHEM", Oid.VARCHAR)),
    FKTABLE_NAME(new BaseMetadata(6, "FKTABLE_NAME", Oid.VARCHAR)),
    FKCOLUMN_NAME(new BaseMetadata(7, "FKCOLUMN_NAME", Oid.VARCHAR)),
    KEY_SEQ(new BaseMetadata(8, "KEY_SEQ", Oid.INT2)),
    UPDATE_RULE(new BaseMetadata(9, "UPDATE_RULE", Oid.INT2)),
    DELETE_RULE(new BaseMetadata(10, "DELETE_RULE", Oid.INT2)),
    FK_NAME(new BaseMetadata(11, "FK_NAME", Oid.VARCHAR)),
    PK_NAME(new BaseMetadata(12, "PK_NAME", Oid.VARCHAR)),
    DEFERRABILITY(new BaseMetadata(13, "DEFERRABILITY", Oid.INT2));

    private final BaseMetadata metadata;

    GetForeignKeys_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getBestRowIdentifier according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getBestRowIdentifier-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetBestRowIdentifier_Metadata implements enumFunc<GetBestRowIdentifier_Metadata> {
    SCOPE(new BaseMetadata(0, "SCOPE", Oid.INT2)),
    COLUMN_NAME(new BaseMetadata(1, "COLUMN_NAME", Oid.VARCHAR)),
    DATA_TYPE(new BaseMetadata(2, "DATA_TYPE", Oid.INT4)),
    TYPE_NAME(new BaseMetadata(3, "TYPE_NAME", Oid.VARCHAR)),
    COLUMN_SIZE(new BaseMetadata(4, "COLUMN_SIZE", Oid.INT4)),
    BUFFER_LENGTH(new BaseMetadata(5, "BUFFER_LENGTH", Oid.INT4)),
    DECIMAL_DIGITS(new BaseMetadata(6, "DECIMAL_DIGITS", Oid.INT2)),
    PSEUDO_COLUMN(new BaseMetadata(7, "PSEUDO_COLUMN", Oid.INT2));

    private final BaseMetadata metadata;

    GetBestRowIdentifier_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getColumnPrivileges according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getColumnPrivileges-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetColumnPrivileges_Metadata implements enumFunc<GetColumnPrivileges_Metadata> {
    TABLE_CAT(new BaseMetadata(0, "TABLE_CAT", Oid.VARCHAR)),
    TABLE_SCHEM(new BaseMetadata(1, "TABLE_SCHEM", Oid.VARCHAR)),
    TABLE_NAME(new BaseMetadata(2, "TABLE_NAME", Oid.VARCHAR)),
    COLUMN_NAME(new BaseMetadata(3, "COLUMN_NAME", Oid.VARCHAR)),
    GRANTOR(new BaseMetadata(4, "GRANTOR", Oid.VARCHAR)),
    GRANTEE(new BaseMetadata(5, "GRANTEE", Oid.VARCHAR)),
    PRIVILEGE(new BaseMetadata(6, "PRIVILEGE", Oid.VARCHAR)),
    IS_GRANTABLE(new BaseMetadata(7, "IS_GRANTABLE", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetColumnPrivileges_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getTablePrivileges according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTablePrivileges-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetTablePrivileges_Metadata implements enumFunc<GetTablePrivileges_Metadata> {
    TABLE_CAT(new BaseMetadata(0, "TABLE_CAT", Oid.VARCHAR)),
    TABLE_SCHEM(new BaseMetadata(1, "TABLE_SCHEM", Oid.VARCHAR)),
    TABLE_NAME(new BaseMetadata(2, "TABLE_NAME", Oid.VARCHAR)),
    GRANTOR(new BaseMetadata(3, "GRANTOR", Oid.VARCHAR)),
    GRANTEE(new BaseMetadata(4, "GRANTEE", Oid.VARCHAR)),
    PRIVILEGE(new BaseMetadata(5, "PRIVILEGE", Oid.VARCHAR)),
    IS_GRANTABLE(new BaseMetadata(6, "IS_GRANTABLE", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetTablePrivileges_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getProcedures according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getProcedures-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetProcedures_Metadata implements enumFunc<GetProcedures_Metadata> {
    PROCEDURE_CAT(new BaseMetadata(0, "PROCEDURE_CAT", Oid.VARCHAR)),
    PROCEDURE_SCHEM(new BaseMetadata(1, "PROCEDURE_SCHEM", Oid.VARCHAR)),
    PROCEDURE_NAME(new BaseMetadata(2, "PROCEDURE_NAME", Oid.VARCHAR)),
    RESERVE1(new BaseMetadata(3, "RESERVE1", Oid.VARCHAR)),
    RESERVE2(new BaseMetadata(4, "RESERVE2", Oid.VARCHAR)),
    RESERVE3(new BaseMetadata(5, "RESERVE3", Oid.VARCHAR)),
    REMARKS(new BaseMetadata(6, "REMARKS", Oid.VARCHAR)),
    PROCEDURE_TYPE(new BaseMetadata(7, "PROCEDURE_TYPE", Oid.INT2)),
    SPECIFIC_NAME(new BaseMetadata(8, "SPECIFIC_NAME", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetProcedures_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getProceduresColumns according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getProceduresColumns-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetProceduresColumns_Metadata implements enumFunc<GetProceduresColumns_Metadata> {
    PROCEDURE_CAT(new BaseMetadata(0, "PROCEDURE_CAT", Oid.VARCHAR)),
    PROCEDURE_SCHEM(new BaseMetadata(1, "PROCEDURE_SCHEM", Oid.VARCHAR)),
    PROCEDURE_NAME(new BaseMetadata(2, "PROCEDURE_NAME", Oid.VARCHAR)),
    COLUMN_NAME(new BaseMetadata(3, "COLUMN_NAME", Oid.VARCHAR)),
    COLUMN_TYPE(new BaseMetadata(4, "COLUMN_TYPE", Oid.INT2)),
    DATA_TYPE(new BaseMetadata(5, "DATA_TYPE", Oid.INT4)),
    TYPE_NAME(new BaseMetadata(6, "TYPE_NAME", Oid.VARCHAR)),
    PRECISION(new BaseMetadata(7, "PRECISION", Oid.INT4)),
    LENGTH(new BaseMetadata(8, "LENGTH", Oid.INT4)),
    SCALE(new BaseMetadata(9, "SCALE", Oid.INT2)),
    RADIX(new BaseMetadata(10, "RADIX", Oid.INT2)),
    NULLABLE(new BaseMetadata(11, "NULLABLE", Oid.INT2)),
    REMARKS(new BaseMetadata(12, "REMARKS", Oid.VARCHAR)),
    COLUMN_DEF(new BaseMetadata(13, "COLUMN_DEF", Oid.VARCHAR)),
    SQL_DATA_TYPE(new BaseMetadata(14, "SQL_DATA_TYPE", Oid.INT4)),
    SQL_DATETIME_SUB(new BaseMetadata(15, "SQL_DATETIME_SUB", Oid.INT4)),
    CHAR_OCTET_LENGTH(new BaseMetadata(16, "CHAR_OCTET_LENGTH", Oid.INT4)),
    ORDINAL_POSITION(new BaseMetadata(17, "ORDINAL_POSITION", Oid.INT4)),
    IS_NULLABLE(new BaseMetadata(18, "IS_NULLABLE", Oid.VARCHAR)),
    SPECIFIC_NAME(new BaseMetadata(19, "SPECIFIC_NAME", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetProceduresColumns_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getFunctions according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getFunctions-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetFunctions_Metadata implements enumFunc<GetFunctions_Metadata> {
    FUNCTION_CAT(new BaseMetadata(0, "FUNCTION_CAT", Oid.VARCHAR)),
    FUNCTION_SCHEM(new BaseMetadata(1, "FUNCTION_SCHEM", Oid.VARCHAR)),
    FUNCTION_NAME(new BaseMetadata(2, "FUNCTION_NAME", Oid.VARCHAR)),
    REMARKS(new BaseMetadata(3, "REMARKS", Oid.VARCHAR)),
    FUNCTION_TYPE(new BaseMetadata(4, "FUNCTION_TYPE", Oid.INT2)),
    SPECIFIC_NAME(new BaseMetadata(5, "SPECIFIC_NAME", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetFunctions_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getFunctionsColumns according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getFunctionsColumns-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetFunctionsColumns_Metadata implements enumFunc<GetFunctionsColumns_Metadata> {
    FUNCTION_CAT(new BaseMetadata(0, "FUNCTION_CAT", Oid.VARCHAR)),
    FUNCTION_SCHEM(new BaseMetadata(1, "FUNCTION_SCHEM", Oid.VARCHAR)),
    FUNCTION_NAME(new BaseMetadata(2, "FUNCTION_NAME", Oid.VARCHAR)),
    COLUMN_NAME(new BaseMetadata(3, "COLUMN_NAME", Oid.VARCHAR)),
    COLUMN_TYPE(new BaseMetadata(4, "COLUMN_TYPE", Oid.INT2)),
    DATA_TYPE(new BaseMetadata(5, "DATA_TYPE", Oid.INT4)),
    TYPE_NAME(new BaseMetadata(6, "TYPE_NAME", Oid.VARCHAR)),
    PRECISION(new BaseMetadata(7, "PRECISION", Oid.INT4)),
    LENGTH(new BaseMetadata(8, "LENGTH", Oid.INT4)),
    SCALE(new BaseMetadata(9, "SCALE", Oid.INT2)),
    RADIX(new BaseMetadata(10, "RADIX", Oid.INT2)),
    NULLABLE(new BaseMetadata(11, "NULLABLE", Oid.INT2)),
    REMARKS(new BaseMetadata(12, "REMARKS", Oid.VARCHAR)),
    CHAR_OCTET_LENGTH(new BaseMetadata(13, "CHAR_OCTET_LENGTH", Oid.INT4)),
    ORDINAL_POSITION(new BaseMetadata(14, "ORDINAL_POSITION", Oid.INT4)),
    IS_NULLABLE(new BaseMetadata(15, "IS_NULLABLE", Oid.VARCHAR)),
    SPECIFIC_NAME(new BaseMetadata(16, "SPECIFIC_NAME", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetFunctionsColumns_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  /** Define return columns for JDBC API getTableType according to the spec
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTableType-java.lang.String-java.lang.String-java.lang.String-">JDBC Specification</a>
   */
  public enum GetTableType_Metadata implements enumFunc<GetTableType_Metadata> {
    TABLE_TYPE(new BaseMetadata(0, "TABLE_TYPE", Oid.VARCHAR));

    private final BaseMetadata metadata;

    GetTableType_Metadata(BaseMetadata metadata) {
      this.metadata = metadata;
    }
    public int getIndex() {
      return metadata.getIndex();
    }
    public String getName() {
      return metadata.getName();
    }
    public int getOidType() {
      return metadata.getOidType();
    }
  }

  public interface enumFunc<T extends Enum<T>> {
    int getIndex();
    String getName();
    int getOidType();
  }
  public static <T extends Enum<T> & enumFunc<T>> Field[] getField(Supplier<T[]> enumObject) {
    T[] enumValue = enumObject.get();
    Field[] columns = new Field[enumValue.length];
    for (T metadata : enumValue) {
      columns[metadata.getIndex()] = new Field(metadata.getName(), metadata.getOidType());
    }
    return columns;
  }
  private Field[] getCatalogsField() {
    return getField(GetCatalogs_Metadata::values);
  }
  private Field[] getSchemasField(){
    return getField(GetSchemas_Metadata::values);
  }
  private Field[] getTablesField(){
    return getField(GetTables_Metadata::values);
  }
  private Field[] getColumnsField(){
    return getField(GetColumns_Metadata::values);
  }
  private Field[] getPrimaryKeysField(){
    return getField(GetPrimaryKeys_Metadata::values);
  }
  private Field[] getForeignKeysField() {
    return getField(GetForeignKeys_Metadata::values);
  }
  private Field[] getBestRowIdentifierField() {
    return getField(GetBestRowIdentifier_Metadata::values);
  }
  private Field[] getTableType() {
    return getField(GetTableType_Metadata::values);
  }
  private Field[] getFunctionsColumns() {
    return getField(GetFunctionsColumns_Metadata::values);
  }
  private Field[] getFunctions() {
    return getField(GetFunctions_Metadata::values);
  }
  private Field[] getProceduresColumns() {
    return getField(GetProceduresColumns_Metadata::values);
  }
  private Field[] getProcedures() {
    return getField(GetProcedures_Metadata::values);
  }
  private Field[] getTablePrivileges() {
    return getField(GetTablePrivileges_Metadata::values);
  }
  private Field[] getColumnPrivileges() {
    return getField(GetColumnPrivileges_Metadata::values);
  }

  // Define column name for SHOW DATABASES
  public static final String SHOW_DATABASES_DATABASE_NAME = "database_name";

  // Define column name for SHOW SCHEMAS
  public static final String SHOW_SCHEMAS_DATABASE_NAME = "database_name";
  public static final String SHOW_SCHEMAS_SCHEMA_NAME = "schema_name";

  /**
   * Data container for schema metadata returned by the SHOW SCHEMAS command.
   *
   * <p>This class represents a single schema entry with its associated database name.
   * Used by JDBC metadata methods like getSchemas() to provide structured access
   * to schema information instead of raw result set data.
   *
   * @see java.sql.DatabaseMetaData#getSchemas()
   * @see java.sql.DatabaseMetaData#getSchemas(String, String)
   * @see MetadataServerProxy#getSchemas(String, String, boolean)
   */
  public static class ShowSchemasInfo {
    private final String databaseName;
    private final String schemaName;

    // Full constructor
    public ShowSchemasInfo(String databaseName, String schemaName) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
    }

    // Minimal constructor
    public ShowSchemasInfo(String schemaName) {
      this(null, schemaName);
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
  }

  // Define column name for SHOW TABLES
  public static final String SHOW_TABLES_DATABASE_NAME = "database_name";
  public static final String SHOW_TABLES_SCHEMA_NAME = "schema_name";
  public static final String SHOW_TABLES_TABLE_NAME = "table_name";
  public static final String SHOW_TABLES_TABLE_TYPE = "table_type";
  public static final String SHOW_TABLES_REMARKS = "remarks";
  public static final String SHOW_TABLES_OWNER = "owner";
  public static final String SHOW_TABLES_LAST_ALTERED_TIME = "last_altered_time";
  public static final String SHOW_TABLES_LAST_MODIFIED_TIME = "last_modified_time";
  public static final String SHOW_TABLES_DIST_STYLE = "dist_style";
  public static final String SHOW_TABLES_TABLE_SUBTYPE = "table_subtype";

  /**
   * Data container for table metadata returned by the SHOW TABLES command.
   *
   * <p>This class represents comprehensive table information including basic metadata
   * (name, type) and Redshift-specific attributes (owner, distribution style, subtype).
   * Used by JDBC metadata methods like getTables() to provide structured access to
   * table information with both minimal and full detail modes.
   *
   * <p>The class supports two usage patterns:
   * <ul>
   *   <li><strong>Minimal mode:</strong> Only table name is populated (for basic table listing)</li>
   *   <li><strong>Full mode:</strong> All available metadata is populated (for detailed table info)</li>
   * </ul>
   *
   * @see java.sql.DatabaseMetaData#getTables(String, String, String, String[])
   * @see MetadataServerProxy#getTables(String, String, String, boolean)
   */
  public static class ShowTablesInfo {
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final String tableType;
    private final String remarks;
    private final String owner;
    private final String lastAlteredTime;
    private final String lastModifiedTime;
    private final String distStyle;
    private final String tableSubtype;

    // Full constructor
    public ShowTablesInfo(String databaseName, String schemaName, String tableName,
                          String tableType, String remarks, String owner,
                          String lastAlteredTime, String lastModifiedTime,
                          String distStyle, String tableSubtype) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.tableType = tableType;
      this.remarks = remarks;
      this.owner = owner;
      this.lastAlteredTime = lastAlteredTime;
      this.lastModifiedTime = lastModifiedTime;
      this.distStyle = distStyle;
      this.tableSubtype = tableSubtype;
    }

    // Minimal constructor
    public ShowTablesInfo(String tableName) {
      this(null, null, tableName, null, null, null, null, null, null, null);
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public String getTableType() { return tableType; }
    public String getRemarks() { return remarks; }
    public String getOwner() { return owner; }
    public String getLastAlteredTime() { return lastAlteredTime; }
    public String getLastModifiedTime() { return lastModifiedTime; }
    public String getDistStyle() { return distStyle; }
    public String getTableSubtype() { return tableSubtype; }
  }

  // Define column name for SHOW COLUMNS
  public static final String SHOW_COLUMNS_DATABASE_NAME = "database_name";
  public static final String SHOW_COLUMNS_SCHEMA_NAME = "schema_name";
  public static final String SHOW_COLUMNS_TABLE_NAME = "table_name";
  public static final String SHOW_COLUMNS_COLUMN_NAME = "column_name";
  public static final String SHOW_COLUMNS_ORDINAL_POSITION = "ordinal_position";
  public static final String SHOW_COLUMNS_COLUMN_DEFAULT = "column_default";
  public static final String SHOW_COLUMNS_IS_NULLABLE = "is_nullable";
  public static final String SHOW_COLUMNS_DATA_TYPE = "data_type";
  public static final String SHOW_COLUMNS_CHARACTER_MAXIMUM_LENGTH = "character_maximum_length";
  public static final String SHOW_COLUMNS_NUMERIC_PRECISION = "numeric_precision";
  public static final String SHOW_COLUMNS_NUMERIC_SCALE = "numeric_scale";
  public static final String SHOW_COLUMNS_REMARKS = "remarks";
  public static final String SHOW_COLUMNS_SORT_KEY_TYPE = "sort_key_type";
  public static final String SHOW_COLUMNS_SORT_KEY = "sort_key";
  public static final String SHOW_COLUMNS_DIST_KEY = "dist_key";
  public static final String SHOW_COLUMNS_ENCODING = "encoding";
  public static final String SHOW_COLUMNS_COLLATION = "collation";

  /**
   * Data container for column metadata returned by the SHOW COLUMNS command.
   *
   * <p>This class represents comprehensive column information including standard SQL metadata
   * (name, type, nullability, defaults) and Redshift-specific attributes (sort keys,
   * distribution keys, encoding). Used by JDBC metadata methods like getColumns() to provide
   * structured access to column information.
   *
   * <p>Redshift-specific fields include:
   * <ul>
   *   <li><strong>sortKeyType/sortKey:</strong> Column's role in table sorting</li>
   *   <li><strong>distKey:</strong> Whether column is used for data distribution</li>
   *   <li><strong>encoding:</strong> Column compression encoding (e.g., LZO, DELTA)</li>
   * </ul>
   *
   * @see java.sql.DatabaseMetaData#getColumns(String, String, String, String)
   * @see MetadataServerProxy#getColumns(String, String, String, String, boolean)
   */
  public static class ShowColumnsInfo {
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final String ordinalPosition;
    private final String columnDefault;
    private final String isNullable;
    private final String dataType;
    private final String characterMaximumLength;
    private final String numericPrecision;
    private final String numericScale;
    private final String remarks;
    private final String sortKeyType;
    private final String sortKey;
    private final String distKey;
    private final String encoding;
    private final String collation;

    // Full constructor
    public ShowColumnsInfo(String databaseName, String schemaName, String tableName,
                           String columnName, String ordinalPosition, String columnDefault,
                           String isNullable, String dataType, String characterMaximumLength,
                           String numericPrecision, String numericScale, String remarks,
                           String sortKeyType, String sortKey, String distKey,
                           String encoding, String collation) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.ordinalPosition = ordinalPosition;
      this.columnDefault = columnDefault;
      this.isNullable = isNullable;
      this.dataType = dataType;
      this.characterMaximumLength = characterMaximumLength;
      this.numericPrecision = numericPrecision;
      this.numericScale = numericScale;
      this.remarks = remarks;
      this.sortKeyType = sortKeyType;
      this.sortKey = sortKey;
      this.distKey = distKey;
      this.encoding = encoding;
      this.collation = collation;
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public String getColumnName() { return columnName; }
    public String getOrdinalPosition() { return ordinalPosition; }
    public String getColumnDefault() { return columnDefault; }
    public String getIsNullable() { return isNullable; }
    public String getDataType() { return dataType; }
    public String getCharacterMaximumLength() { return characterMaximumLength; }
    public String getNumericPrecision() { return numericPrecision; }
    public String getNumericScale() { return numericScale; }
    public String getRemarks() { return remarks; }
    public String getSortKeyType() { return sortKeyType; }
    public String getSortKey() { return sortKey; }
    public String getDistKey() { return distKey; }
    public String getEncoding() { return encoding; }
    public String getCollation() { return collation; }
  }

  // Define column name for SHOW PRIMARY KEYS
  public static final String SHOW_PRIMARY_KEYS_DATABASE_NAME = "database_name";
  public static final String SHOW_PRIMARY_KEYS_SCHEMA_NAME = "schema_name";
  public static final String SHOW_PRIMARY_KEYS_TABLE_NAME = "table_name";
  public static final String SHOW_PRIMARY_KEYS_COLUMN_NAME = "column_name";
  public static final String SHOW_PRIMARY_KEYS_KEY_SEQ = "key_seq";
  public static final String SHOW_PRIMARY_KEYS_PK_NAME = "pk_name";

  /**
   * Data container for primary key metadata returned by the SHOW CONSTRAINTS PRIMARY KEYS command.
   *
   * <p>This class represents information about columns that participate in a table's
   * primary key constraint. Each instance represents one column in the primary key,
   * with the key sequence indicating the column's position within a multi-column key.
   * Used by JDBC metadata methods like getPrimaryKeys() to provide structured access
   * to primary key information.
   *
   * <p>For composite primary keys (multiple columns), multiple instances will be created
   * with the same pkName but different keySeq values to indicate column ordering.
   *
   * @see java.sql.DatabaseMetaData#getPrimaryKeys(String, String, String)
   * @see MetadataServerProxy#getPrimaryKeys(String, String, String, boolean)
   */
  public static class ShowPrimaryKeysInfo {
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final String keySeq;
    private final String pkName;

    // Full constructor
    public ShowPrimaryKeysInfo(String databaseName, String schemaName, String tableName,
                               String columnName, String keySeq, String pkName) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.keySeq = keySeq;
      this.pkName = pkName;
    }

    // Minimal constructor
    public ShowPrimaryKeysInfo(String tableName, String columnName) {
      this(null, null, tableName, columnName, null, null);
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public String getColumnName() { return columnName; }
    public String getKeySeq() { return keySeq; }
    public String getPkName() { return pkName; }
  }

  // Define column names for SHOW CONSTRAINTS FOREIGN KEYS and SHOW CONSTRAINTS FOREIGN KEYS EXPORT
  public static final String SHOW_FOREIGN_KEYS_PK_DATABASE_NAME = "pk_database_name";
  public static final String SHOW_FOREIGN_KEYS_PK_SCHEMA_NAME = "pk_schema_name";
  public static final String SHOW_FOREIGN_KEYS_PK_TABLE_NAME = "pk_table_name";
  public static final String SHOW_FOREIGN_KEYS_PK_COLUMN_NAME = "pk_column_name";
  public static final String SHOW_FOREIGN_KEYS_FK_DATABASE_NAME = "fk_database_name";
  public static final String SHOW_FOREIGN_KEYS_FK_SCHEMA_NAME = "fk_schema_name";
  public static final String SHOW_FOREIGN_KEYS_FK_TABLE_NAME = "fk_table_name";
  public static final String SHOW_FOREIGN_KEYS_FK_COLUMN_NAME = "fk_column_name";
  public static final String SHOW_FOREIGN_KEYS_KEY_SEQ = "key_seq";
  public static final String SHOW_FOREIGN_KEYS_UPDATE_RULE = "update_rule";
  public static final String SHOW_FOREIGN_KEYS_DELETE_RULE = "delete_rule";
  public static final String SHOW_FOREIGN_KEYS_FK_NAME = "fk_name";
  public static final String SHOW_FOREIGN_KEYS_PK_NAME = "pk_name";
  public static final String SHOW_FOREIGN_KEYS_DEFERRABILITY = "deferrability";

  /**
   * Data container for foreign key metadata returned by the SHOW CONSTRAINTS FOREIGN KEYS command.
   *
   * <p>This class represents information about foreign key constraints in a table.
   * Each instance represents one column in the foreign key, with the key sequence
   * indicating the column's position within a multi-column key. Used by JDBC metadata
   * methods like getImportedKeys() to provide structured access to foreign key information.
   *
   * <p>For composite foreign keys (multiple columns), multiple instances will be created
   * with the same fkName but different keySeq values to indicate column ordering.
   *
   * @see java.sql.DatabaseMetaData#getImportedKeys(String, String, String)
   * @see MetadataServerProxy#getForeignKeys(String, String, String, boolean, boolean)
   */
  public static class ShowForeignKeysInfo {
    private final String pkDatabaseName;
    private final String pkSchemaName;
    private final String pkTableName;
    private final String pkColumnName;
    private final String fkDatabaseName;
    private final String fkSchemaName;
    private final String fkTableName;
    private final String fkColumnName;
    private final String keySeq;
    private final String updateRule;
    private final String deleteRule;
    private final String fkName;
    private final String pkName;
    private final String deferrability;

    // Full constructor
    public ShowForeignKeysInfo(String pkDatabaseName, String pkSchemaName, String pkTableName,
                               String pkColumnName, String fkDatabaseName, String fkSchemaName,
                               String fkTableName, String fkColumnName, String keySeq,
                               String updateRule, String deleteRule, String fkName,
                               String pkName, String deferrability) {
      this.pkDatabaseName = pkDatabaseName;
      this.pkSchemaName = pkSchemaName;
      this.pkTableName = pkTableName;
      this.pkColumnName = pkColumnName;
      this.fkDatabaseName = fkDatabaseName;
      this.fkSchemaName = fkSchemaName;
      this.fkTableName = fkTableName;
      this.fkColumnName = fkColumnName;
      this.keySeq = keySeq;
      this.updateRule = updateRule;
      this.deleteRule = deleteRule;
      this.fkName = fkName;
      this.pkName = pkName;
      this.deferrability = deferrability;
    }

    public String getPkDatabaseName() { return pkDatabaseName; }
    public String getPkSchemaName() { return pkSchemaName; }
    public String getPkTableName() { return pkTableName; }
    public String getPkColumnName() { return pkColumnName; }
    public String getFkDatabaseName() { return fkDatabaseName; }
    public String getFkSchemaName() { return fkSchemaName; }
    public String getFkTableName() { return fkTableName; }
    public String getFkColumnName() { return fkColumnName; }
    public String getKeySeq() { return keySeq; }
    public String getUpdateRule() { return updateRule; }
    public String getDeleteRule() { return deleteRule; }
    public String getFkName() { return fkName; }
    public String getPkName() { return pkName; }
    public String getDeferrability() { return deferrability; }
  }

  // Define column names for SHOW GRANT
  public static final String SHOW_GRANT_DATABASE_NAME = "database_name";
  public static final String SHOW_GRANT_SCHEMA_NAME = "schema_name";
  public static final String SHOW_GRANT_OBJECT_NAME = "object_name";
  public static final String SHOW_GRANT_TABLE_NAME = "table_name";
  public static final String SHOW_GRANT_COLUMN_NAME = "column_name";
  public static final String SHOW_GRANT_GRANTOR = "grantor_name";
  public static final String SHOW_GRANT_IDENTITY_NAME = "identity_name";
  public static final String SHOW_GRANT_PRIVILEGE_TYPE = "privilege_type";
  public static final String SHOW_GRANT_ADMIN_OPTION = "admin_option";

  /**
   * Data container for privilege grant metadata returned by the SHOW GRANTS command.
   *
   * <p>This class represents a single privilege grant, showing what permissions have been
   * granted to which users/roles on which database objects. Each instance represents one
   * privilege grant entry, which can apply to databases, schemas, tables, or individual columns.
   * Used by JDBC metadata methods like getTablePrivileges() and getColumnPrivileges() to
   * provide structured access to privilege information.
   *
   * <p>Grant hierarchy levels:
   * <ul>
   *   <li><strong>Database level:</strong> Only databaseName is populated</li>
   *   <li><strong>Schema level:</strong> databaseName and schemaName are populated</li>
   *   <li><strong>Table level:</strong> databaseName, schemaName, and tableName are populated</li>
   *   <li><strong>Column level:</strong> All fields including columnName are populated</li>
   * </ul>
   *
   * <p>The adminOption indicates whether the grantee can further grant this privilege to others
   * (equivalent to "WITH GRANT OPTION" in SQL).
   *
   * @see java.sql.DatabaseMetaData#getTablePrivileges(String, String, String)
   * @see java.sql.DatabaseMetaData#getColumnPrivileges(String, String, String, String)
   * @see MetadataServerProxy#getTablePrivileges(String, String, String, boolean)
   * @see MetadataServerProxy#getColumnPrivileges(String, String, String, String, boolean)
   */
  public static class ShowGrantsInfo {
    private final String databaseName;
    private final String schemaName;
    private final String objectName;
    private final String tableName;
    private final String columnName;
    private final String grantor;
    private final String identityName;
    private final String privilegeType;
    private final boolean adminOption;

    public ShowGrantsInfo(String databaseName, String schemaName, String objectName,
                          String tableName, String columnName, String grantor,
                          String identityName, String privilegeType, boolean adminOption) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.objectName = objectName;
      this.tableName = tableName;
      this.columnName = columnName;
      this.grantor = grantor;
      this.identityName = identityName;
      this.privilegeType = privilegeType;
      this.adminOption = adminOption;
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getObjectName() { return objectName; }
    public String getTableName() { return tableName; }
    public String getColumnName() { return columnName; }
    public String getGrantor() { return grantor; }
    public String getIdentityName() { return identityName; }
    public String getPrivilegeType() { return privilegeType; }
    public boolean getAdminOption() { return adminOption; }
  }


    // Define column names for SHOW PROCEDURE
  public static final String SHOW_PROCEDURES_DATABASE_NAME = "database_name";
  public static final String SHOW_PROCEDURES_SCHEMA_NAME = "schema_name";
  public static final String SHOW_PROCEDURES_PROCEDURE_NAME = "procedure_name";
  public static final String SHOW_PROCEDURES_RETURN_TYPE = "return_type";
  public static final String SHOW_PROCEDURES_ARGUMENT_LIST = "argument_list";

  /**
   * Data container for stored procedure metadata returned by the SHOW PROCEDURES command.
   *
   * <p>This class represents information about stored procedures including their signature
   * (name and argument list) and return type. Used by JDBC metadata methods like getProcedures()
   * to provide structured access to procedure information.
   *
   * @see java.sql.DatabaseMetaData#getProcedures(String, String, String)
   * @see MetadataServerProxy#getProcedures(String, String, String, boolean)
   */
  public static class ShowProceduresInfo {
    private final String databaseName;
    private final String schemaName;
    private final String procedureName;
    private final String returnType;
    private final String argumentList;

    public ShowProceduresInfo(String databaseName, String schemaName, String procedureName,
                              String returnType, String argumentList) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.procedureName = procedureName;
      this.returnType = returnType;
      this.argumentList = argumentList;
    }

    public ShowProceduresInfo(String procedureName, String argumentList) {
      this(null, null, procedureName, null, argumentList);
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getProcedureName() { return procedureName; }
    public String getReturnType() { return returnType; }
    public String getArgumentList() { return argumentList; }
  }

  // Define column names for SHOW FUNCTIONS
  public static final String SHOW_FUNCTIONS_DATABASE_NAME = "database_name";
  public static final String SHOW_FUNCTIONS_SCHEMA_NAME = "schema_name";
  public static final String SHOW_FUNCTIONS_FUNCTION_NAME = "function_name";
  public static final String SHOW_FUNCTIONS_RETURN_TYPE = "return_type";
  public static final String SHOW_FUNCTIONS_ARGUMENT_LIST = "argument_list";

  /**
   * Data container for user-defined function metadata returned by the SHOW FUNCTIONS command.
   *
   * <p>This class represents information about user-defined functions including their signature
   * (name and argument list) and return type. Used by JDBC metadata methods like getFunctions()
   * to provide structured access to function information.
   *
   * @see java.sql.DatabaseMetaData#getFunctions(String, String, String)
   * @see MetadataServerProxy#getFunctions(String, String, String, boolean)
   */
  public static class ShowFunctionsInfo {
    private final String databaseName;
    private final String schemaName;
    private final String functionName;
    private final String returnType;
    private final String argumentList;

    public ShowFunctionsInfo(String databaseName, String schemaName, String functionName,
                             String returnType, String argumentList) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.functionName = functionName;
      this.returnType = returnType;
      this.argumentList = argumentList;
    }

    public ShowFunctionsInfo(String functionName, String argumentList) {
            this(null, null, functionName, null, argumentList);
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getFunctionName() { return functionName; }
    public String getReturnType() { return returnType; }
    public String getArgumentList() { return argumentList; }
  }

  // Define column names for SHOW PARAMETERS
  public static final String SHOW_PARAMETERS_DATABASE_NAME = "database_name";
  public static final String SHOW_PARAMETERS_SCHEMA_NAME = "schema_name";
  public static final String SHOW_PARAMETERS_PROCEDURE_NAME = "procedure_name";
  public static final String SHOW_PARAMETERS_FUNCTION_NAME = "function_name";
  public static final String SHOW_PARAMETERS_PARAMETER_NAME = "parameter_name";
  public static final String SHOW_PARAMETERS_ORDINAL_POSITION = "ordinal_position";
  public static final String SHOW_PARAMETERS_PARAMETER_TYPE = "parameter_type";
  public static final String SHOW_PARAMETERS_DATA_TYPE = "data_type";
  public static final String SHOW_PARAMETERS_CHARACTER_MAXIMUM_LENGTH = "character_maximum_length";
  public static final String SHOW_PARAMETERS_NUMERIC_PRECISION = "numeric_precision";
  public static final String SHOW_PARAMETERS_NUMERIC_SCALE = "numeric_scale";

  /**
   * Data container for stored procedure parameter metadata returned by the SHOW PARAMETERS command.
   *
   * <p>This class represents information about stored procedure parameters including their
   * name, type, and position. Used by JDBC metadata methods like getProcedureColumns() and
   * getFunctionColumns() to provide structured access to parameter information.
   *
   * @see java.sql.DatabaseMetaData#getProcedureColumns(String, String, String, String)
   * @see java.sql.DatabaseMetaData#getFunctionColumns(String, String, String, String)
   * @see MetadataServerProxy#getProcedureColumns(String, String, String, String, boolean)
   * @see MetadataServerProxy#getFunctionColumns(String, String, String, String, boolean)
   */
  public static class ShowParametersInfo {
    private final String databaseName;
    private final String schemaName;
    private final String procedureName;
    private final String functionName;
    private final String parameterName;
    private final String ordinalPosition;
    private final String parameterType;
    private final String dataType;
    private final String characterMaximumLength;
    private final String numericPrecision;
    private final String numericScale;

    public ShowParametersInfo(String databaseName, String schemaName, String procedureName,
                              String functionName, String parameterName, String ordinalPosition,
                              String parameterType, String dataType, String characterMaximumLength,
                              String numericPrecision, String numericScale) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.procedureName = procedureName;
      this.functionName = functionName;
      this.parameterName = parameterName;
      this.ordinalPosition = ordinalPosition;
      this.parameterType = parameterType;
      this.dataType = dataType;
      this.characterMaximumLength = characterMaximumLength;
      this.numericPrecision = numericPrecision;
      this.numericScale = numericScale;
    }

    public String getDatabaseName() { return databaseName; }
    public String getSchemaName() { return schemaName; }
    public String getProcedureName() { return procedureName; }
    public String getFunctionName() { return functionName; }
    public String getParameterName() { return parameterName; }
    public String getOrdinalPosition() { return ordinalPosition; }
    public String getParameterType() { return parameterType; }
    public String getDataType() { return dataType; }
    public String getCharacterMaximumLength() { return characterMaximumLength; }
    public String getNumericPrecision() { return numericPrecision; }
    public String getNumericScale() { return numericScale; }
  }

  // Define SQL query for prepare statement
  protected final String SQL_PREP_SHOWDATABASES = "SHOW DATABASES;";
  protected final String SQL_PREP_SHOWSCHEMAS = "SHOW SCHEMAS FROM DATABASE ?;";
  protected final String SQL_PREP_SHOWSCHEMASLIKE = "SHOW SCHEMAS FROM DATABASE ? LIKE ?;";
  protected final String SQL_PREP_SHOWTABLES = "SHOW TABLES FROM SCHEMA ?.?;";
  protected final String SQL_PREP_SHOWTABLESLIKE = "SHOW TABLES FROM SCHEMA ?.? LIKE ?;";
  protected final String SQL_PREP_SHOWCOLUMNS = "SHOW COLUMNS FROM TABLE ?.?.?";
  protected final String SQL_PREP_SHOWCOLUMNSLIKE = "SHOW COLUMNS FROM TABLE ?.?.? LIKE ?;";
  protected final String SQL_PREP_SHOWPRIMARYKEYS = "SHOW CONSTRAINTS PRIMARY KEYS FROM TABLE ?.?.?;";
  protected final String SQL_PREP_SHOWFOREIGNKEYS = "SHOW CONSTRAINTS FOREIGN KEYS FROM TABLE ?.?.?;";
  protected final String SQL_PREP_SHOWFOREIGNEXPORTEDKEYS = "SHOW CONSTRAINTS FOREIGN KEYS EXPORTED FROM TABLE ?.?.?;";
  protected final String SQL_PREP_SHOWGRANTSCOLUMN = "SHOW COLUMN GRANTS ON TABLE ?.?.?;";
  protected final String SQL_PREP_SHOWGRANTSCOLUMNLIKE = "SHOW COLUMN GRANTS ON TABLE ?.?.? LIKE ?;";
  protected final String SQL_PREP_SHOWGRANTSTABLE = "SHOW GRANTS ON TABLE ?.?.?;";
  protected final String SQL_PREP_SHOWPROCEDURES = "SHOW PROCEDURES FROM SCHEMA ?.?;";
  protected final String SQL_PREP_SHOWPROCEDURESLIKE = "SHOW PROCEDURES FROM SCHEMA ?.? LIKE ?;";
  protected final String SQL_PREP_SHOWFUNCTIONS = "SHOW FUNCTIONS FROM SCHEMA ?.?;";
  protected final String SQL_PREP_SHOWFUNCTIONSLIKE = "SHOW FUNCTIONS FROM SCHEMA ?.? LIKE ?;";
  protected final String SQL_PREP_SHOWPARAMETERSPROCEDURE = "SHOW PARAMETERS OF PROCEDURE ?.?.?";
  protected final String SQL_PREP_SHOWPARAMETERSFUNCTION = "SHOW PARAMETERS OF FUNCTION ?.?.?";

  protected final String SQL_SEMICOLON = ";";
  protected final String SQL_LIKE = " LIKE ?;";

  //Custom precision from DATETIME/INTERVAL data type
  protected static final String DATETIME_PRECISION_PATTERN = "(time|timetz|timestamp|timestamptz)\\(\\d+\\).*";
  protected static final String INTERVAL_PRECISION_PATTERN = "interval.*.\\(\\d+\\)";
  protected static final String PRECISION_EXTRACTION_PATTERN = ".*\\(([0-9]+)\\).*";
  protected static final String PRECISION_REMOVAL_PATTERN = "\\(\\d+\\)";
  protected static final String TRAILING_SPACES_PATTERN = "\\s++$";

  // Constants for Post-processing
  protected static final String RADIX_VALUE = "10";
  protected static final String NULLABLE_UNKNOWN_VALUE = "2";
  protected static final String EMPTY_REMARKS = "";
  protected static final String IS_NULLABLE_VALUE = "";
  protected static final String PSEUDO_COLUMN_VALUE = "1";

  protected static final Map<String, Integer> PROCEDURE_COLUMN_TYPE_MAP;
  static {
    Map<String, Integer> map = new HashMap<>();
    map.put("IN", DatabaseMetaData.procedureColumnIn);
    map.put("OUT", DatabaseMetaData.procedureColumnOut);
    map.put("INOUT", DatabaseMetaData.procedureColumnInOut);
    map.put("TABLE", DatabaseMetaData.procedureColumnResult);
    map.put("RETURN", DatabaseMetaData.procedureColumnReturn);
    PROCEDURE_COLUMN_TYPE_MAP = Collections.unmodifiableMap(map);
  }

  protected static final Map<String, Integer> FUNCTION_COLUMN_TYPE_MAP;
  static {
    Map<String, Integer> map = new HashMap<>();
    map.put("IN", DatabaseMetaData.functionColumnIn);
    map.put("OUT", DatabaseMetaData.functionColumnOut);
    map.put("INOUT", DatabaseMetaData.functionColumnInOut);
    map.put("TABLE", DatabaseMetaData.functionColumnResult);
    map.put("RETURN", DatabaseMetaData.functionReturn);
      FUNCTION_COLUMN_TYPE_MAP = Collections.unmodifiableMap(map);
  }

  // Create statement for executing query
  protected Statement createMetaDataStatement() throws SQLException {
    return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
  }

  // Create Prepare statement for executing query
  protected PreparedStatement createMetaDataPreparedStatement(String sql) throws SQLException {
    return connection.prepareStatement(sql);
  }

  // Create ResultSet based on column Field[] and Tuple list
  protected ResultSet createRs(Field[] col, List<Tuple> data) throws SQLException{
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(col, data);
  }

  // Helper function for executing query
  protected ResultSet runQuery(String sql) throws SQLException{
    return createMetaDataStatement().executeQuery(sql);
  }

  // Helper function to create empty Tuple based on column Field[] and size
  protected byte[][] getEmptyTuple(Field[] col, int size) throws SQLException {
    byte[][] tuple = new byte[size][];
    byte[] temStr = encodeStr("");

    // Currently we put 0 in empty ResultSet for non-varchar data type. Otherwise, we'll hit error in function RedshiftResultSet/internalGetObject
    byte[] temNum = encodeStr("0");

    for(int i=0 ; i<size ; i++){
      tuple[i] = col[i].getOID() == Oid.VARCHAR ? temStr:temNum;
    }
    return tuple;
  }

  // Helper function to encode String
  protected byte[] encodeStr(String str) throws SQLException {
    return connection.encodeString(str);
  }

  // Create mapping from server returning Data type to Redshift type
  private static final Map<String, String> rsTypeMap = new HashMap<>();
  static {
    rsTypeMap.put("character varying", "varchar");
    rsTypeMap.put("\"char\"", "char");
    rsTypeMap.put("character", "char");
    rsTypeMap.put("smallint", "int2");
    rsTypeMap.put("integer", "int4");
    rsTypeMap.put("bigint", "int8");
    rsTypeMap.put("real", "float4");
    rsTypeMap.put("double precision", "float8");
    rsTypeMap.put("boolean", "bool");
    rsTypeMap.put("time without time zone", "time");
    rsTypeMap.put("time with time zone", "timetz");
    rsTypeMap.put("timestamp without time zone", "timestamp");
    rsTypeMap.put("timestamp with time zone", "timestamptz");
    rsTypeMap.put("interval year to month", "intervaly2m");
    rsTypeMap.put("interval year", "intervaly2m");
    rsTypeMap.put("interval month", "intervaly2m");
    rsTypeMap.put("interval day to second", "intervald2s");
    rsTypeMap.put("interval day", "intervald2s");
    rsTypeMap.put("interval second", "intervald2s");
  }
  // Helper function to get Redshift type String
  protected String getRSType(String rsType) throws SQLException {
    return rsTypeMap.getOrDefault(rsType, rsType);
  }

  // Helper function to get SQL type from given Redshift type
  protected String getSQLType(String rsType) throws SQLException {
    return Integer.toString(connection.getTypeInfo().getSQLType(rsType));
  }

  // Create mapping from Redshift type to Column size
  private static final Map<String, Integer> rsColumnSizeMap = new HashMap<>();
  static {
    rsColumnSizeMap.put("bit", 1);
    rsColumnSizeMap.put("bool", 1);
    rsColumnSizeMap.put("int2", 5);
    rsColumnSizeMap.put("int4", 10);
    rsColumnSizeMap.put("int8", 19);
    rsColumnSizeMap.put("float4", 8);
    rsColumnSizeMap.put("float8", 17);
    rsColumnSizeMap.put("date", 13);
    rsColumnSizeMap.put("time", 15);
    rsColumnSizeMap.put("timetz", 21);
    rsColumnSizeMap.put("timestamp", 29);
    rsColumnSizeMap.put("timestamptz", 35);
    rsColumnSizeMap.put("intervaly2m", 32);
    rsColumnSizeMap.put("intervald2s", 64);
  }

  // Helper function to get Column size from given Redshift type
  // Copy from existing query
  // character_maximum_length and numeric_precision are returned from Server API SHOW COLUMNS
  protected String getColumnSize(String rsType, String character_maximum_length, String numeric_precision){
    switch (rsType) {
      case "decimal": case "numeric":
        return numeric_precision;
      case "varchar": case "character varying": case "char": case "character": case "nchar": case "bpchar": case "nvarchar":
        return character_maximum_length;
      case "geometry": case "super": case "varbyte": case "geography":
        return null;
      default:
        return Integer.toString(rsColumnSizeMap.getOrDefault(rsType,2147483647));
    }
  }

  /**
   * Gets the column length for a given Redshift data type
   * @param rsType The Redshift data type
   * @return The column length as a string, or null if not found
   */
  protected String getColumnLength(String rsType) {
    if (rsType == null) {
      return null;
    }

    switch (rsType) {
      case "bool":
      case "bit":
      case "boolean":
        return "1";

      case "int2":
      case "smallint":
        return "2";

      case "int4":
      case "integer":
      case "int":
        return "4";

      case "int8":
      case "bigint":
        return "20";

      case "float4":
      case "real":
        return "4";

      case "float8":
      case "double precision":
        return "8";

      case "date":
        return "6";

      case "time":
        return "15";

      case "timetz":
        return "21";

      case "timestamp":
        return "6";

      case "timestamptz":
        return "35";

      case "intervaly2m":
        return "4";

      case "intervald2s":
        return "8";

      case "super":
        return "4194304";

      case "geography":
      case "varbyte":
        return "1000000";

      default:
        return null;
    }
  }

  // Helper function to get Decimal Digit from given Redshift type
  // Copy from existing query
  // numeric_scale is directly returned from Server API SHOW COLUMNS
  // precision is computed from data_type for time, timetz, timestamp, timestamptz, intervald2s and intervaly2m
  protected String getDecimalDigit(String rsType, String numeric_scale, int precision, boolean customizePrecision){
    switch (rsType) {
      case "float4": case "real":
        return "8";
      case "float8": case "double precision":
        return "17";
      case "time": case "time without time zone": case "timetz": case "time with time zone": case "timestamp": case "timestamp without time zone": case "timestamptz": case "timestamp with time zone": case "intervald2s":
        return customizePrecision ? String.valueOf(precision) : "6";
      case "intervaly2m":
        return customizePrecision ? String.valueOf(precision) : "0";
      case "geometry": case "super": case "varbyte": case "geography":
        return null;
      case "numeric":
        return numeric_scale;
      default:
        return "0";
    }
  }

  // Helper function to get Number of Prefix Radix from given Redshift type
  protected String getNumPrefixRadix(String rsType){
    switch (rsType) {
      case "varbyte": case "geography":
        return "2";
      default:
        return "10";
    }
  }

  // Helper function to convert nullable String to corresponding number
  protected String getNullable(String nullable){
    switch (nullable) {
      case "YES":
        return Integer.toString(ResultSetMetaData.columnNullable);
      case "NO":
        return Integer.toString(ResultSetMetaData.columnNoNulls);
      default:
        return Integer.toString(ResultSetMetaData.columnNullableUnknown);
    }
  }

  // Helper function to get auto-increment/generated value
  protected String getAutoIncrement(String colDef){
    if(colDef != null && (colDef.contains("\"identity\"") || colDef.contains("default_identity"))){
      return "YES";
    }
    else{
      return "NO";
    }
  }

  // Helper function to return specific name for procedure and function by appending arguments at the end with ()
  protected String getSpecificName(String name, String argument) throws SQLException {
    if (Utils.isNullOrEmpty(name)) {
        throw new RedshiftException("Function/Procedure name cannot be null or empty");
    }
    argument = (argument == null ? "" : argument);
    return name + "(" + argument + ")";
  }

  /**
   * Creates a parameterized SQL query string based on the argument list and base SQL statement.
   * Appends either a semicolon or LIKE clause depending on whether column_name_pattern is provided.
   *
   * @param argumentList      Comma-separated string of argument types
   *                         (e.g. "integer, short, character varying")
   * @param sqlBase          Base SQL statement to which parameters will be added
   *                         (e.g. "SHOW PARAMETERS OF PROCEDURE")
   * @param columnNamePattern Optional pattern for filtering column names.
   *                         If provided, adds LIKE clause instead of semicolon
   * @return A Pair containing:
   *         - String: Complete SQL query with appropriate placeholders and termination
   *         - {@code List<String>}: List of argument types with whitespace stripped
   */
  protected Map.Entry<String, List<String>> createParameterizedQueryString(
          String argumentList,
          String sqlBase,
          String columnNamePattern) throws SQLException {
    if (Utils.isNullOrEmpty(sqlBase)) {
      throw new RedshiftException("SQL base cannot be null or empty");
    }
    if (Utils.isNullOrEmpty(argumentList)) {
      String sql = sqlBase + "()" + (Utils.isNullOrEmpty(columnNamePattern) ? SQL_SEMICOLON : SQL_LIKE);
      return new AbstractMap.SimpleEntry<>(sql, new ArrayList<>());
    }

    // Split the argument list and strip whitespace from each element
    List<String> args = Arrays.stream(argumentList.split(","))
            .map(String::trim)
            .collect(Collectors.toList());

    // Validate data types
    List<String> invalidTypes = RedshiftDataTypes.validateTypes(args);
    if (!invalidTypes.isEmpty()) {
      throw new RedshiftException(String.format("Invalid data type(s) in argument list: %s. Argument list provided: %s",
            String.join(", ", invalidTypes),
            argumentList)
      );
    }

    // Create the parameterized string
    String placeholders = String.join(", ",
            Arrays.stream(new String[args.size()])
                    .map(s -> "?")
                    .toArray(String[]::new));

    String sql = String.format("%s(%s)%s",
            sqlBase,
            placeholders,
            Utils.isNullOrEmpty(columnNamePattern) ? SQL_SEMICOLON : SQL_LIKE);

    return new AbstractMap.SimpleEntry<>(sql, args);
  }

  protected static int getProcedureType(String returnType) {
    if (Utils.isNullOrEmpty(returnType)) {
      return DatabaseMetaData.procedureNoResult;
    }
    return DatabaseMetaData.procedureReturnsResult;
  }

  protected static int getFunctionType(String returnType) {
    if (returnType != null && returnType.equals("record")) {
      return DatabaseMetaData.functionReturnsTable;
    }
    return DatabaseMetaData.functionNoTable;
  }

  protected static int getProcedureColumnType(String parameterType) {
    if (Utils.isNullOrEmpty(parameterType)) {
      return DatabaseMetaData.procedureColumnUnknown;
    }
    return PROCEDURE_COLUMN_TYPE_MAP.getOrDefault(parameterType.toUpperCase(), DatabaseMetaData.procedureColumnUnknown);
  }

  protected static int getFunctionColumnType(String parameterType) {
    if (Utils.isNullOrEmpty(parameterType)) {
      return DatabaseMetaData.functionColumnUnknown;
    }
    return FUNCTION_COLUMN_TYPE_MAP.getOrDefault(parameterType.toUpperCase(), DatabaseMetaData.functionColumnUnknown);
  }

  /**
   * Sorts a list of foreign key tuples based on specified criteria.
   * The sorting is performed based on the following criteria, in order:
   * 1. Catalog (PK_CATALOG for imported, FK_CATALOG for exported)
   * 2. Schema (PK_SCHEMA for imported, FK_SCHEMA for exported)
   * 3. Table (PK_TABLE for imported, FK_TABLE for exported)
   * 4. Key sequence (KEY_SEQ)
   *
   * @param foreignKeyTuples The list of foreign key tuples to be sorted.
   * @param isImported Boolean flag indicating whether the keys are imported (true) or exported (false).
   *                   This affects which columns are used for comparison during sorting.
   *
   * Each comparison is done using the compareBytes method.
   */
  protected static void sortForeignKeyTuples(List<Tuple> foreignKeyTuples, boolean isImported) {
    Collections.sort(foreignKeyTuples, (t1, t2) -> {
      int comparedCol = isImported ?
              ForeignKeyColumnIndex.PK_CATALOG.getIndex() : ForeignKeyColumnIndex.FK_CATALOG.getIndex();
      int comparison = compareBytes(t1.get(comparedCol), t2.get(comparedCol));
      if (comparison != 0) return comparison;

      comparedCol = isImported ?
              ForeignKeyColumnIndex.PK_SCHEMA.getIndex() : ForeignKeyColumnIndex.FK_SCHEMA.getIndex();
      comparison = compareBytes(t1.get(comparedCol), t2.get(comparedCol));
      if (comparison != 0) return comparison;

      comparedCol = isImported ?
              ForeignKeyColumnIndex.PK_TABLE.getIndex() : ForeignKeyColumnIndex.FK_TABLE.getIndex();
      comparison = compareBytes(t1.get(comparedCol), t2.get(comparedCol));
      if (comparison != 0) return comparison;

      // Finally, compare key_seq column
      return compareBytes(t1.get(ForeignKeyColumnIndex.KEY_SEQ.getIndex()), t2.get(ForeignKeyColumnIndex.KEY_SEQ.getIndex()));
    });
  }

  /**
   * Compares two byte arrays lexicographically.
   * This method handles null values and performs a direct byte-by-byte comparison.
   * If the arrays have different lengths but are equal up to the length of the shorter array,
   * the longer array is considered greater.
   *
   * @param b1 The first byte array to compare.
   * @param b2 The second byte array to compare.
   * @return A negative integer, zero, or a positive integer as the first array
   *         is less than, equal to, or greater than the second array.
   */
  private static int compareBytes(byte[] b1, byte[] b2) {
    // Handle null cases
    if (b1 == null && b2 == null) return 0;
    if (b1 == null) return -1;
    if (b2 == null) return 1;

    // Direct byte comparison
    int length = Math.min(b1.length, b2.length);
    for (int i = 0; i < length; i++) {
      int diff = Byte.toUnsignedInt(b1[i]) - Byte.toUnsignedInt(b2[i]);
      if (diff != 0) return diff;
    }

    // If one array is longer than the other
    return Integer.compare(b1.length, b2.length);
  }

  /**
   * Helper function to bridge the gap where SHOW return boolean but JDBC spec required YES/NO
   */
  protected static String getIsGrantable(Boolean admin_option) {
      return admin_option == null ? "NO" : (admin_option ? "YES" : "NO");
  }

  /**
   * Pattern matching function similar to SQL LIKE operator. Driver currently treat empty string as null
   * which match anything
   *
   * @param str The input string to match
   * @param pattern The pattern to match against, containing wildcards:
   *               '%' - matches zero or more characters
   *               '_' - matches exactly one character
   * @return true if string matches the pattern, false otherwise
   */
  public static boolean patternMatch(String str, String pattern) {
    // Empty pattern matches any string
    if (pattern == null || pattern.isEmpty()) {
      return true;
    }
    // Convert SQL LIKE pattern to regex
    String regexPattern = convertSqlLikeToRegex(pattern);

    // Use regex to match with DOTALL flag to make . match newlines
    Pattern compiledPattern = Pattern.compile(regexPattern, Pattern.DOTALL);
    Matcher matcher = compiledPattern.matcher(str);

    return matcher.matches();
  }

  /**
   * Convert SQL LIKE pattern to regex pattern
   * @param pattern SQL LIKE pattern with % and _ wildcards
   * @return Equivalent regex pattern
   */
  public static String convertSqlLikeToRegex(String pattern) {
    if (pattern == null || pattern.isEmpty()) {
      return ".*"; // Empty pattern matches anything
    }

    // Check if pattern only contains '%'
    boolean onlyPercent = true;
    for (char c : pattern.toCharArray()) {
      if (c != '%') {
        onlyPercent = false;
        break;
      }
    }
    if (onlyPercent) {
      return ".*";
    }

    StringBuilder regexPattern = new StringBuilder();
    int i = 0;

    while (i < pattern.length()) {
      char ch = pattern.charAt(i);

      if (ch == '\\' && i + 1 < pattern.length()) {
        // Handle escaped characters
        char nextChar = pattern.charAt(i + 1);
        if (nextChar == '%' || nextChar == '_' || nextChar == '\\') {
          // Escape the next character for regex
          regexPattern.append(Pattern.quote(String.valueOf(nextChar)));
          i += 2;
        } else {
          // Not a special escape, treat backslash literally
          regexPattern.append(Pattern.quote(String.valueOf(ch)));
          i += 1;
        }
      } else if (ch == '%') {
        // % matches zero or more characters
        regexPattern.append(".*");
        i += 1;
      } else if (ch == '_') {
        // _ matches exactly one character
        regexPattern.append(".");
        i += 1;
      } else {
        // Regular character, escape it for regex
        regexPattern.append(Pattern.quote(String.valueOf(ch)));
        i += 1;
      }
    }
    return "^" + regexPattern.toString() + "$";
  }
}
