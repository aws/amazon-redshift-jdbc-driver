/*
 * Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.*;
import com.amazon.redshift.logger.RedshiftLogger;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

public class MetadataAPIHelper {
  public MetadataAPIHelper(RedshiftConnectionImpl connection) {
    this.connection = connection;

    this.GET_CATALOGS_COLS = getCatalogsField();

    this.GET_SCHEMAS_COLS = getSchemasField();

    this.GET_TABLES_COLS = getTablesField();

    this.GET_COLUMNS_COLS = getColumnsField();
  }

  protected final RedshiftConnectionImpl connection; // The connection association


  // Define constant value for metadata API getCatalogs() ResultSet
  protected final Field[] GET_CATALOGS_COLS;

  // Define constant value for metadata API getSchemas() ResultSet
  protected final Field[] GET_SCHEMAS_COLS;

  // Define constant value for metadata API getTables() ResultSet
  protected final Field[] GET_TABLES_COLS;

  // Define constant value for metadata API getColumns() ResultSet
  protected final Field[] GET_COLUMNS_COLS;


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
    REF_GENERATION(new BaseMetadata(9, "REF_GENERATION", Oid.VARCHAR));

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
    IS_GENERATEDCOLUMN(new BaseMetadata(23, "IS_GENERATEDCOLUMN", Oid.VARCHAR));

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

  // Define column name for SHOW DATABASES
  public static final String SHOW_DATABASES_DATABASE_NAME = "database_name";

  // Define column name for SHOW SCHEMAS
  public static final String SHOW_SCHEMAS_DATABASE_NAME = "database_name";
  public static final String SHOW_SCHEMAS_SCHEMA_NAME = "schema_name";

  // Define column name for SHOW TABLES
  public static final String SHOW_TABLES_DATABASE_NAME = "database_name";
  public static final String SHOW_TABLES_SCHEMA_NAME = "schema_name";
  public static final String SHOW_TABLES_TABLE_NAME = "table_name";
  public static final String SHOW_TABLES_TABLE_TYPE = "table_type";
  public static final String SHOW_TABLES_REMARKS = "remarks";

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


  // Define SQL query for normal statement
  protected final String SQL_SHOWDATABASES = "SHOW DATABASES;";
  protected final String SQL_SHOWDATABASESLIKE = "SHOW DATABASES LIKE ''{0}'';";
  protected final String SQL_SHOWSCHEMAS = "SHOW SCHEMAS FROM DATABASE {0};";
  protected final String SQL_SHOWSCHEMASLIKE = "SHOW SCHEMAS FROM DATABASE {0} LIKE ''{1}'';";
  protected final String SQL_SHOWTABLES = "SHOW TABLES FROM SCHEMA {0}.{1};";
  protected final String SQL_SHOWTABLESLIKE = "SHOW TABLES FROM SCHEMA {0}.{1} LIKE ''{2}'';";
  protected final String SQL_SHOWCOLUMNS = "SHOW COLUMNS FROM TABLE {0}.{1}.{2};";
  protected final String SQL_SHOWCOLUMNSLIKE = "SHOW COLUMNS FROM TABLE {0}.{1}.{2} LIKE ''{3}'';";

  // Define SQL query for prepare statement
  protected final String SQL_PREP_SHOWDATABASES = "SHOW DATABASES;";
  protected final String SQL_PREP_SHOWDATABASESLIKE = "SHOW DATABASES LIKE '$1';";
  protected final String SQL_PREP_SHOWSCHEMAS = "SHOW SCHEMAS FROM DATABASE $1;";
  protected final String SQL_PREP_SHOWSCHEMASLIKE = "SHOW SCHEMAS FROM DATABASE $1 LIKE '$2';";
  protected final String SQL_PREP_SHOWTABLES = "SHOW TABLES FROM SCHEMA $1.$2 LIKE '$3';";
  protected final String SQL_PREP_SHOWCOLUMNS = "SHOW COLUMNS FROM TABLE $1.$2.$3 LIKE '$4';";

  // Create statement for executing query
  protected Statement createMetaDataStatement() throws SQLException {
    return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
  }

  // Create Prepare statement for executing query
  protected PreparedStatement createMetaDataPreparedStatement(String sql) throws SQLException {
    return connection.prepareStatement(sql);
  }

  // Create empty ResultSet based on column Field[] and size
  protected ResultSet createEmptyRs(Field[] col, int size) throws SQLException{
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(col, getEmptyTupleList(col, size));
  }

  // Create null ResultSet based on column Field[] and size
  protected ResultSet createNullRs(Field[] col, int size) throws SQLException{
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(col, getNullTupleList(size));
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

  // Helper function to create empty Tuple List
  protected List<Tuple> getEmptyTupleList(Field[] col, int size) throws SQLException {
    if (RedshiftLogger.isEnable())
      connection.getLogger().logDebug("Create empty tuple list");

    List<Tuple> emptyTuple = new ArrayList<>();
    emptyTuple.add(new Tuple(getEmptyTuple(col, size)));
    return emptyTuple;
  }

  // Helper function to create null Tuple based on size
  protected byte[][] getNullTuple(int size) throws SQLException {
    return new byte[size][];
  }

  // Helper function to create null Tuple List
  protected List<Tuple> getNullTupleList(int size) throws SQLException {
    if (RedshiftLogger.isEnable())
      connection.getLogger().logDebug("Create null tuple list");

    List<Tuple> nullTuple = new ArrayList<>();
    nullTuple.add(new Tuple(getNullTuple(size)));
    return nullTuple;
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

  // Helper function to check if name was null/wildcard/empty string to determine
  // if we want to specify LIKE in SHOW command
  protected boolean checkNameIsNotPattern(String name){
    return name == null || name.isEmpty() || name.equals("%");
  }

  // Helper function to check if name was exact name to determine
  // if we can skip corresponding SHOW API call
  protected boolean checkNameIsExactName(String name){
    return name != null && !name.isEmpty() && !name.contains("%");
  }
}
