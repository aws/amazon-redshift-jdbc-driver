/*
 * Copyright (c) 2005, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.core.BaseStatement;
import com.amazon.redshift.core.Oid;
import com.amazon.redshift.core.QueryExecutor;
import com.amazon.redshift.core.ServerVersion;
import com.amazon.redshift.core.TypeInfo;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftObject;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TypeInfoCache implements TypeInfo {
  // rsname (String) -> java.sql.Types (Integer)
  private Map<String, Integer> rsNameToSQLType;

  // rsname (String) -> java class name (String)
  // ie "text" -> "java.lang.String"
  private Map<String, String> rsNameToJavaClass;

  // oid (Integer) -> rsname (String)
  private Map<Integer, String> oidToRsName;
  // rsname (String) -> oid (Integer)
  private Map<String, Integer> rsNameToOid;

  // rsname (String) -> extension rsobject (Class)
  private Map<String, Class<? extends RedshiftObject>> rsNameToRsObject;

  // type array oid -> base type's oid
  private Map<Integer, Integer> rsArrayToRsType;

  // array type oid -> base type array element delimiter
  private Map<Integer, Character> arrayOidToDelimiter;

  private BaseConnection conn;
  private final int unknownLength;
  private PreparedStatement getOidStatementSimple;
  private PreparedStatement getOidStatementComplexNonArray;
  private PreparedStatement getOidStatementComplexArray;
  private PreparedStatement getNameStatement;
  private PreparedStatement getArrayElementOidStatement;
  private PreparedStatement getArrayDelimiterStatement;
  private PreparedStatement getTypeInfoStatement;
  private PreparedStatement getAllTypeInfoStatement;

  // Geometry
  public static final String GEOMETRY_NAME = "geometry";
  public static final int GEOMETRYOID = Oid.GEOMETRY;
  public static final int GEOMETRYHEXOID = Oid.GEOMETRYHEX;

  // super (previous name Omni)
  public static final String SUPER_NAME = "super";
  public static final int SUPEROID = Oid.SUPER;

  public static final String VARBYTE_NAME = "varbyte";
  public static final int VARBYTEOID = Oid.VARBYTE;

  public static final String GEOGRAPHY_NAME = "geography";
  public static final int GEOGRAPHYOID = Oid.GEOGRAPHY;

  public static final String TID_NAME = "tid";
  public static final String TID_ARRAY_NAME = "_tid";
  public static final String XID_NAME = "xid";
  public static final String XID_ARRAY_NAME = "_xid";
  
  
  // basic rs types info:
  // 0 - type name
  // 1 - type oid
  // 2 - sql type
  // 3 - java class
  // 4 - array type oid
  private static final Object[][] types = {
  		// Aliases of sql types first, so map has actual type later in the array
      {"oid", Oid.OID, Types.BIGINT, "java.lang.Long", Oid.OID_ARRAY},
      {"money", Oid.MONEY, Types.DOUBLE, "java.lang.Double", Oid.MONEY_ARRAY},
      {"double precision", Oid.FLOAT8, Types.DOUBLE, "java.lang.Double", Oid.FLOAT8_ARRAY},
      {"bpchar", Oid.BPCHAR, Types.CHAR, "java.lang.String", Oid.BPCHAR_ARRAY},
      {"text", Oid.TEXT, Types.VARCHAR, "java.lang.String", Oid.TEXT_ARRAY},
      {"name", Oid.NAME, Types.VARCHAR, "java.lang.String", Oid.NAME_ARRAY},
      {"character varying", Oid.VARCHAR, Types.VARCHAR, "java.lang.String", Oid.VARCHAR_ARRAY},
      {"bit", Oid.BIT, Types.BIT, "java.lang.Boolean", Oid.BIT_ARRAY},
      {"time without time zone", Oid.TIME, Types.TIME, "java.sql.Time", Oid.TIME_ARRAY},
      {"timestamp without time zone", Oid.TIMESTAMP, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMP_ARRAY},
      {"timestamp with time zone", Oid.TIMESTAMPTZ, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMPTZ_ARRAY},
      {GEOMETRY_NAME, Oid.GEOMETRYHEX, Types.LONGVARBINARY, "[B", Oid.GEOMETRYHEX_ARRAY},
      {XID_NAME, Oid.XIDOID, Types.BIGINT, "java.lang.Long", Oid.XIDARRAYOID},
      {TID_NAME, Oid.TIDOID, Types.VARCHAR, "java.lang.String", Oid.TIDARRAYOID},
      {"abstime", Oid.ABSTIMEOID, Types.TIMESTAMP, "java.sql.Timestamp", Oid.ABSTIMEARRAYOID},
      
      // Actual types
      {"int2", Oid.INT2, Types.SMALLINT, "java.lang.Integer", Oid.INT2_ARRAY},
      {"int4", Oid.INT4, Types.INTEGER, "java.lang.Integer", Oid.INT4_ARRAY},
      {"int8", Oid.INT8, Types.BIGINT, "java.lang.Long", Oid.INT8_ARRAY},
      {"int2vector", Oid.INT2VECTOR, Types.VARCHAR, "java.lang.Object", Oid.INT2VECTOR_ARRAY},
      {"numeric", Oid.NUMERIC, Types.NUMERIC, "java.math.BigDecimal", Oid.NUMERIC_ARRAY},
      {"float4", Oid.FLOAT4, Types.REAL, "java.lang.Float", Oid.FLOAT4_ARRAY},
      {"float8", Oid.FLOAT8, Types.DOUBLE, "java.lang.Double", Oid.FLOAT8_ARRAY},
      {"char", Oid.CHAR, Types.CHAR, "java.lang.String", Oid.CHAR_ARRAY},
      {"varchar", Oid.VARCHAR, Types.VARCHAR, "java.lang.String", Oid.VARCHAR_ARRAY},
      {"varbit", Oid.VARBIT, Types.OTHER, "java.lang.String", Oid.VARBIT_ARRAY},
      {"bytea", Oid.BYTEA, Types.BINARY, "[B", Oid.BYTEA_ARRAY},
      {"bool", Oid.BOOL, Types.BIT, "java.lang.Boolean", Oid.BOOL_ARRAY},
      {"date", Oid.DATE, Types.DATE, "java.sql.Date", Oid.DATE_ARRAY},
      {"time", Oid.TIME, Types.TIME, "java.sql.Time", Oid.TIME_ARRAY},
      {"time with time zone", Oid.TIMETZ, Types.TIME, "java.sql.Time", Oid.TIMETZ_ARRAY},
      {"timetz", Oid.TIMETZ, Types.TIME, "java.sql.Time", Oid.TIMETZ_ARRAY},
      {"timestamp", Oid.TIMESTAMP, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMP_ARRAY},
      {"timestamptz", Oid.TIMESTAMPTZ, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMPTZ_ARRAY},
      {"reltime", Oid.RELTIME, Types.TIME, "java.sql.Time", Oid.RELTIME_ARRAY},
      {"intervaly2m", Oid.INTERVALY2M, Types.OTHER, "com.amazon.redshift.util.RedshiftIntervalYearToMonth", Oid.INTERVALY2M_ARRAY},
      {"intervald2s", Oid.INTERVALD2S, Types.OTHER, "com.amazon.redshift.util.RedshiftIntervalDayToSecond", Oid.INTERVALD2S_ARRAY},
      {"tinterval", Oid.TINTERVAL, Types.OTHER, "java.lang.String", Oid.TINTERVAL_ARRAY},
      {"interval", Oid.INTERVAL, Types.OTHER, "java.lang.String", Oid.INTERVAL_ARRAY},
      //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
      {"refcursor", Oid.REF_CURSOR, Types.REF_CURSOR, "java.sql.ResultSet", Oid.REF_CURSOR_ARRAY},
      //JCP! endif
      {"aclitem", Oid.ACLITEM, Types.OTHER, "java.lang.Object", Oid.ACLITEM_ARRAY}, 
      {"regproc", Oid.REGPROC, Types.OTHER, "java.lang.Object", Oid.REGPROC_ARRAY},
      {"oidvector", Oid.OIDVECTOR, Types.VARCHAR, "java.lang.Object", Oid.OIDVECTOR_ARRAY},
      {"cid", Oid.CID, Types.INTEGER, "java.lang.Integer", Oid.CID_ARRAY},
      {"cidr", Oid.CIDR, Types.INTEGER, "java.lang.Integer", Oid.CIDR_ARRAY},
      {"lseg", Oid.LSEG, Types.OTHER, "java.lang.String", Oid.LSEG_ARRAY},
      {"path", Oid.PATH, Types.VARCHAR, "java.lang.String", Oid.PATH_ARRAY},
      {"polygon", Oid.POLYGON, Types.OTHER, "java.lang.String", Oid.POLYGON_ARRAY},
      {"line", Oid.LINE, Types.INTEGER, "java.lang.Integer", Oid.LINE_ARRAY},
      {"circle", Oid.CIRCLE, Types.OTHER, "java.lang.String", Oid.CIRCLE_ARRAY},
      {"macaddr", Oid.MACADDR, Types.VARCHAR, "java.lang.String", Oid.MACADDR_ARRAY},
      {"inet", Oid.INET, Types.VARCHAR, "java.lang.String", Oid.INET_ARRAY},
      {"void", Oid.VOID, Types.VARCHAR, "java.lang.String", Oid.VOID_ARRAY},

      {"regprocedure", Oid.REGPROCEDURE, Types.OTHER, "java.lang.String", Oid.REGPROCEDURE_ARRAY},
      {"regoper", Oid.REGOPER, Types.OTHER, "java.lang.String", Oid.REGOPER_ARRAY},
      {"regoperator", Oid.REGOPERATOR, Types.OTHER, "java.lang.String", Oid.REGOPERATOR_ARRAY},
      {"regclass", Oid.REGCLASS, Types.OTHER, "java.lang.String", Oid.REGCLASS_ARRAY},
      {"regtype", Oid.REGTYPE, Types.OTHER, "java.lang.String", Oid.REGTYPE_ARRAY},
      {"box", Oid.BOX, Types.OTHER, "java.lang.Object", Oid.BOX_ARRAY},
      {"useritem", Oid.USERITEM, Types.OTHER, "java.lang.Object", Oid.USERITEM_ARRAY},
      {"roleitem", Oid.ROLEITEM, Types.OTHER, "java.lang.Object", Oid.ROLEITEM_ARRAY},

      {"json", Oid.JSON, Types.OTHER, "com.amazon.redshift.util.RedshiftObject", Oid.JSON_ARRAY},
      {"point", Oid.POINT, Types.OTHER, "com.amazon.redshift.geometric.RedshiftPoint", Oid.POINT_ARRAY},
      {GEOMETRY_NAME, Oid.GEOMETRY, Types.LONGVARBINARY, "[B", Oid.GEOMETRY_ARRAY},
      {SUPER_NAME, Oid.SUPER, Types.LONGVARCHAR, "java.lang.String", Oid.SUPER_ARRAY},
      {VARBYTE_NAME, Oid.VARBYTE, Types.LONGVARBINARY, "[B", Oid.VARBYTE_ARRAY},
      {GEOGRAPHY_NAME, Oid.GEOGRAPHY, Types.LONGVARBINARY, "[B", Oid.GEOGRAPHY_ARRAY},

      {"smgr", Oid.SMGR, Types.OTHER, "java.lang.String", Oid.SMGR_ARRAY},
      {"unknown", Oid.UNKNOWN, Types.VARCHAR, "java.lang.String", Oid.UNKNOWN_ARRAY},
      {"record", Oid.RECORD, Types.OTHER, "java.lang.String", Oid.RECORD_ARRAY},
      {"cstring", Oid.CSTRING, Types.VARCHAR, "java.lang.String", Oid.CSTRING_ARRAY},
      {"any", Oid.ANY, Types.OTHER, "java.lang.String", Oid.ANY_ARRAY},
      {"anyarray", Oid.ANYARRAY, Types.OTHER, "java.lang.String", Oid.ANYARRAY_ARRAY},
      {"trigger", Oid.TRIGGER, Types.OTHER, "java.lang.String", Oid.TRIGGER_ARRAY},
      {"language_handler", Oid.LANGUAGE_HANDLER, Types.OTHER, "java.lang.String", Oid.LANGUAGE_HANDLER_ARRAY},
      {"internal", Oid.INTERNAL, Types.OTHER, "java.lang.String", Oid.INTERNAL_ARRAY},
      {"opaque", Oid.OPAQUE, Types.OTHER, "java.lang.String", Oid.OPAQUE_ARRAY},
      {"anyelement", Oid.ANYELEMENT, Types.OTHER, "java.lang.String", Oid.ANYELEMENT_ARRAY},
      {"hllsketch", Oid.HLLSKETCH, Types.OTHER, "java.lang.String", Oid.HLLSKETCH_ARRAY},
      {"cardinal_number", Oid.CARDINAL_NUMBER, Types.OTHER, "java.lang.String", Oid.CARDINAL_NUMBER_ARRAY},
      {"character_data", Oid.CHARACTER_DATA, Types.OTHER, "java.lang.String", Oid.CHARACTER_DATA_ARRAY},
      {"sql_identifier", Oid.SQL_IDENTIFIER, Types.OTHER, "java.lang.String", Oid.SQL_IDENTIFIER_ARRAY},
      {"time_stamp", Oid.TIME_STAMP, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIME_STAMP_ARRAY}
  };

  /**
   * RS maps several alias to real type names. When we do queries against pg_catalog, we must use
   * the real type, not an alias, so use this mapping.
   */
  private static final HashMap<String, String> typeAliases;

  static {
    typeAliases = new HashMap<String, String>();
    typeAliases.put("smallint", "int2");
    typeAliases.put("integer", "int4");
    typeAliases.put("int", "int4");
    typeAliases.put("bigint", "int8");
    typeAliases.put("float", "float8");
    typeAliases.put("boolean", "bool");
    typeAliases.put("decimal", "numeric");
  }

  public TypeInfoCache(BaseConnection conn, int unknownLength) {
    this.conn = conn;
    this.unknownLength = unknownLength;
    oidToRsName = new HashMap<Integer, String>((int) Math.round(types.length * 1.5));
    rsNameToOid = new HashMap<String, Integer>((int) Math.round(types.length * 1.5));
    rsNameToJavaClass = new HashMap<String, String>((int) Math.round(types.length * 1.5));
    rsNameToRsObject = new HashMap<String, Class<? extends RedshiftObject>>((int) Math.round(types.length * 1.5));
    rsArrayToRsType = new HashMap<Integer, Integer>((int) Math.round(types.length * 1.5));
    arrayOidToDelimiter = new HashMap<Integer, Character>((int) Math.round(types.length * 2.5));

    // needs to be synchronized because the iterator is returned
    // from getRSTypeNamesWithSQLTypes()
    rsNameToSQLType = Collections.synchronizedMap(new HashMap<String, Integer>((int) Math.round(types.length * 1.5)));

    for (Object[] type : types) {
      String pgTypeName = (String) type[0];
      Integer oid = (Integer) type[1];
      Integer sqlType = (Integer) type[2];
      String javaClass = (String) type[3];
      Integer arrayOid = (Integer) type[4];

      addCoreType(pgTypeName, oid, sqlType, javaClass, arrayOid);
    }

    rsNameToJavaClass.put("hstore", Map.class.getName());
  }

  public synchronized void addCoreType(String rsTypeName, Integer oid, Integer sqlType,
      String javaClass, Integer arrayOid) {
    rsNameToJavaClass.put(rsTypeName, javaClass);
    rsNameToOid.put(rsTypeName, oid);
    oidToRsName.put(oid, rsTypeName);
    rsArrayToRsType.put(arrayOid, oid);
    rsNameToSQLType.put(rsTypeName, sqlType);

    // Currently we hardcode all core types array delimiter
    // to a comma. In a stock install the only exception is
    // the box datatype and it's not a JDBC core type.
    //
    Character delim = ',';
    arrayOidToDelimiter.put(oid, delim);
    arrayOidToDelimiter.put(arrayOid, delim);

    String pgArrayTypeName = rsTypeName + "[]";
    rsNameToJavaClass.put(pgArrayTypeName, "java.sql.Array");
    rsNameToSQLType.put(pgArrayTypeName, Types.ARRAY);
    rsNameToOid.put(pgArrayTypeName, arrayOid);
    pgArrayTypeName = "_" + rsTypeName;
    if (!rsNameToJavaClass.containsKey(pgArrayTypeName)) {
      rsNameToJavaClass.put(pgArrayTypeName, "java.sql.Array");
      rsNameToSQLType.put(pgArrayTypeName, Types.ARRAY);
      rsNameToOid.put(pgArrayTypeName, arrayOid);
      oidToRsName.put(arrayOid, pgArrayTypeName);
    }
  }

  public synchronized void addDataType(String type, Class<? extends RedshiftObject> klass)
      throws SQLException {
    rsNameToRsObject.put(type, klass);
    rsNameToJavaClass.put(type, klass.getName());
  }

  public Iterator<String> getRSTypeNamesWithSQLTypes() {
    return rsNameToSQLType.keySet().iterator();
  }

  private String getSQLTypeQuery(boolean typnameParam) {
    // There's no great way of telling what's an array type.
    // People can name their own types starting with _.
    // Other types use typelem that aren't actually arrays, like box.
    //
    // in case of multiple records (in different schemas) choose the one from the current
    // schema,
    // otherwise take the last version of a type that is at least more deterministic then before
    // (keeping old behaviour of finding types, that should not be found without correct search
    // path)
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT typinput='array_in'::regproc as is_array, typtype, typname ");
    sql.append("  FROM pg_catalog.pg_type ");
    sql.append("  LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r ");
    sql.append("          from pg_namespace as ns ");
    // -- go with older way of unnesting array to be compatible with 8.0
    sql.append("          join ( select s.r, (current_schemas(false))[s.r] as nspname ");
    sql.append("                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r ");
    sql.append("         using ( nspname ) ");
    sql.append("       ) as sp ");
    sql.append("    ON sp.nspoid = typnamespace ");
    if (typnameParam) {
      sql.append(" WHERE typname = ? ");
    }
    sql.append(" ORDER BY sp.r, pg_type.oid DESC;");
    return sql.toString();
  }

  private int getSQLTypeFromQueryResult(ResultSet rs, RedshiftLogger logger) throws SQLException {
    Integer type = null;
    boolean isArray = rs.getBoolean("is_array");
    String typtype = rs.getString("typtype");
    String typname = rs.getString("typname");
    
    if (isArray) {
      type = Types.ARRAY;
    } else if ("c".equals(typtype)) {
      type = Types.STRUCT;
    } else if ("d".equals(typtype)) {
      type = Types.DISTINCT;
    } else if ("e".equals(typtype)) {
      type = Types.VARCHAR;
    } else if ("p".equals(typtype)) {
      type = Types.VARCHAR;
    } else if ("b".equals(typtype)
    						&& typname.equals("oidvector")) {
	    type = Types.VARCHAR;
	  }

    if (type == null) {
    	if(RedshiftLogger.isEnable()
    			&& logger != null)
    		logger.log(LogLevel.DEBUG, " isArray=" + isArray + " typname= " + typname + " typtype=" + typtype);

    	type = Types.OTHER;
    }
    return type;
  }

  public void cacheSQLTypes(RedshiftLogger logger) throws SQLException {
  	if(RedshiftLogger.isEnable())
  		logger.log(LogLevel.DEBUG, "caching all SQL typecodes");
  	
    if (getAllTypeInfoStatement == null) {
      getAllTypeInfoStatement = conn.prepareStatement(getSQLTypeQuery(false));
    }
    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) getAllTypeInfoStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }
    ResultSet rs = getAllTypeInfoStatement.getResultSet();
    while (rs.next()) {
      String typeName = rs.getString("typname");
      Integer type = getSQLTypeFromQueryResult(rs, logger);
      if (!rsNameToSQLType.containsKey(typeName)) {
        rsNameToSQLType.put(typeName, type);
      }
    }
    rs.close();
  }

  public int getSQLType(int oid) throws SQLException {
    return getSQLType(getRSType(oid));
  }

  public synchronized int getSQLType(String pgTypeName) throws SQLException {
    if(pgTypeName == null){
      if (RedshiftLogger.isEnable()){
        conn.getLogger().log(LogLevel.INFO, "Unknown pgTypeName found when retrieving the SQL Type --null");
      }
      throw new RedshiftException("Unknown pgTypeName found when retrieving the SQL Type --null");
    }

    if (pgTypeName.endsWith("[]")) {
      return Types.ARRAY;
    }
    Integer i = rsNameToSQLType.get(pgTypeName);
    if (i != null) {
      return i;
    }

    if(RedshiftLogger.isEnable() && conn.getLogger()!=null){
      conn.getLogger().log(LogLevel.INFO, "Unknown pgTypeName found when retrieving the SQL Type --" + pgTypeName);
    }

    if (getTypeInfoStatement == null) {
      getTypeInfoStatement = conn.prepareStatement(getSQLTypeQuery(true));
    }

    getTypeInfoStatement.setString(1, pgTypeName);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) getTypeInfoStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    ResultSet rs = getTypeInfoStatement.getResultSet();

    Integer type = Types.OTHER;
    if (rs.next()) {
      type = getSQLTypeFromQueryResult(rs, conn.getLogger());
    }
    rs.close();

    rsNameToSQLType.put(pgTypeName, type);
    return type;
  }

  private PreparedStatement getOidStatement(String pgTypeName) throws SQLException {
    boolean isArray = pgTypeName.endsWith("[]");
    boolean hasQuote = pgTypeName.contains("\"");
    int dotIndex = pgTypeName.indexOf('.');

    if (dotIndex == -1 && !hasQuote && !isArray) {
      if (getOidStatementSimple == null) {
        String sql;
        // see comments in @getSQLType()
        // -- go with older way of unnesting array to be compatible with 8.0
        sql = "SELECT pg_type.oid, typname "
              + "  FROM pg_catalog.pg_type "
              + "  LEFT "
              + "  JOIN (select ns.oid as nspoid, ns.nspname, r.r "
              + "          from pg_namespace as ns "
              + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
              + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
              + "         using ( nspname ) "
              + "       ) as sp "
              + "    ON sp.nspoid = typnamespace "
              + " WHERE typname = ? "
              + " ORDER BY sp.r, pg_type.oid DESC LIMIT 1;";
        getOidStatementSimple = conn.prepareStatement(sql);
      }
      // coerce to lower case to handle upper case type names
      String lcName = pgTypeName.toLowerCase();
      // default arrays are represented with _ as prefix ... this dont even work for public schema
      // fully
      getOidStatementSimple.setString(1, lcName);
      return getOidStatementSimple;
    }
    PreparedStatement oidStatementComplex;
    if (isArray) {
      if (getOidStatementComplexArray == null) {
        String sql;
/*        if (conn.haveMinimumServerVersion(ServerVersion.v8_3)) {
          sql = "SELECT t.typarray, arr.typname "
              + "  FROM pg_catalog.pg_type t"
              + "  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid"
              + "  JOIN pg_catalog.pg_type arr ON arr.oid = t.typarray"
              + " WHERE t.typname = ? AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))"
              + " ORDER BY t.oid DESC LIMIT 1";
        } else */
        {
          sql = "SELECT t.oid, t.typname "
              + "  FROM pg_catalog.pg_type t"
              + "  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid"
              + " WHERE t.typelem = (SELECT oid FROM pg_catalog.pg_type WHERE typname = ?)"
              + " AND substring(t.typname, 1, 1) = '_' AND t.typlen = -1"
              + " AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))"
              + " ORDER BY t.typelem DESC LIMIT 1";
        }
        getOidStatementComplexArray = conn.prepareStatement(sql);
      }
      oidStatementComplex = getOidStatementComplexArray;
    } else {
      if (getOidStatementComplexNonArray == null) {
        String sql = "SELECT t.oid, t.typname "
            + "  FROM pg_catalog.pg_type t"
            + "  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid"
            + " WHERE t.typname = ? AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))"
            + " ORDER BY t.oid DESC LIMIT 1";
        getOidStatementComplexNonArray = conn.prepareStatement(sql);
      }
      oidStatementComplex = getOidStatementComplexNonArray;
    }
    //type name requested may be schema specific, of the form "{schema}"."typeName",
    //or may check across all schemas where a schema is not specified.
    String fullName = isArray ? pgTypeName.substring(0, pgTypeName.length() - 2) : pgTypeName;
    String schema;
    String name;
    // simple use case
    if (dotIndex == -1) {
      schema = null;
      name = fullName;
    } else {
      if (fullName.startsWith("\"")) {
        if (fullName.endsWith("\"")) {
          String[] parts = fullName.split("\"\\.\"");
          schema = parts.length == 2 ? parts[0] + "\"" : null;
          name = parts.length == 2 ? "\"" + parts[1] : parts[0];
        } else {
          int lastDotIndex = fullName.lastIndexOf('.');
          name = fullName.substring(lastDotIndex + 1);
          schema = fullName.substring(0, lastDotIndex);
        }
      } else {
        schema = fullName.substring(0, dotIndex);
        name = fullName.substring(dotIndex + 1);
      }
    }
    if (schema != null && schema.startsWith("\"") && schema.endsWith("\"")) {
      schema = schema.substring(1, schema.length() - 1);
    } else if (schema != null) {
      schema = schema.toLowerCase();
    }
    if (name.startsWith("\"") && name.endsWith("\"")) {
      name = name.substring(1, name.length() - 1);
    } else {
      name = name.toLowerCase();
    }
    oidStatementComplex.setString(1, name);
    oidStatementComplex.setString(2, schema);
    oidStatementComplex.setBoolean(3, schema == null);
    return oidStatementComplex;
  }

  public synchronized int getRSType(String pgTypeName) throws SQLException {
    Integer oid = rsNameToOid.get(pgTypeName);
    if (oid != null) {
      return oid;
    }
    
    if(RedshiftLogger.isEnable() && conn.getLogger()!=null){
      conn.getLogger().log(LogLevel.INFO, "Unknown pgTypeName found when retrieving the RedShift Type -- " + pgTypeName);
    }

    PreparedStatement oidStatement = getOidStatement(pgTypeName);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) oidStatement).executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    oid = Oid.UNSPECIFIED;
    ResultSet rs = oidStatement.getResultSet();
    if (rs.next()) {
      oid = (int) rs.getLong(1);
      String internalName = rs.getString(2);
      oidToRsName.put(oid, internalName);
      rsNameToOid.put(internalName, oid);
    }
    rsNameToOid.put(pgTypeName, oid);
    rs.close();

    return oid;
  }

  public synchronized String getRSType(int oid) throws SQLException {
    if (oid == Oid.UNSPECIFIED) {
      if(RedshiftLogger.isEnable() && conn.getLogger()!=null) {
        conn.getLogger().log(LogLevel.INFO, "Unspecified oid found when retrieving the RedShift Type");
      }
      throw new RedshiftException("Unspecified oid found when retrieving the RedShift Type");
    }

    String rsTypeName = oidToRsName.get(oid);
    if (rsTypeName != null) {
      return rsTypeName;
    }
    else{
      if(RedshiftLogger.isEnable() && conn.getLogger()!=null) {
        conn.getLogger().log(LogLevel.INFO, "Unknown oid found when retrieving the RedShift Type --" + oid);
      }
      throw new RedshiftException("Unknown oid found when retrieving the RedShift Type --" + oid);
    }
  }

  public int getRSArrayType(String elementTypeName) throws SQLException {
    elementTypeName = getTypeForAlias(elementTypeName);
    return getRSType(elementTypeName + "[]");
  }

  /**
   * Return the oid of the array's base element if it's an array, if not return the provided oid.
   * This doesn't do any database lookups, so it's only useful for the originally provided type
   * mappings. This is fine for it's intended uses where we only have intimate knowledge of types
   * that are already known to the driver.
   *
   * @param oid input oid
   * @return oid of the array's base element or the provided oid (if not array)
   */
  protected synchronized int convertArrayToBaseOid(int oid) {
    Integer i = rsArrayToRsType.get(oid);
    if (i == null) {
      return oid;
    }
    return i;
  }

  public synchronized char getArrayDelimiter(int oid) throws SQLException {
    if (oid == Oid.UNSPECIFIED) {
      return ',';
    }

    Character delim = arrayOidToDelimiter.get(oid);
    if (delim != null) {
      return delim;
    }
    
    if(RedshiftLogger.isEnable() && conn.getLogger()!=null)
      conn.getLogger().log(LogLevel.INFO, "Unknown oid found when retrieving the Array Delimiter Type --" + oid);
    
    if (getArrayDelimiterStatement == null) {
      String sql;
      sql = "SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e "
            + "WHERE t.oid = ? and t.typelem = e.oid";
      getArrayDelimiterStatement = conn.prepareStatement(sql);
    }

    getArrayDelimiterStatement.setInt(1, oid);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) getArrayDelimiterStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    ResultSet rs = getArrayDelimiterStatement.getResultSet();
    if (!rs.next()) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    String s = rs.getString(1);
    delim = s.charAt(0);

    arrayOidToDelimiter.put(oid, delim);

    rs.close();

    return delim;
  }

  public synchronized int getRSArrayElement(int oid) throws SQLException {
    if (oid == Oid.UNSPECIFIED) {
      return Oid.UNSPECIFIED;
    }

    Integer rsType = rsArrayToRsType.get(oid);

    if (rsType != null) {
      return rsType;
    }
    
    if(RedshiftLogger.isEnable() && conn.getLogger()!=null)
      conn.getLogger().log(LogLevel.INFO, "Unknown oid found when retrieving the RS Array Element --" + oid);

    if (getArrayElementOidStatement == null) {
      String sql;
      sql = "SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e ON t.typelem = e.oid "
            + "JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = ?";
      getArrayElementOidStatement = conn.prepareStatement(sql);
    }

    getArrayElementOidStatement.setInt(1, oid);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) getArrayElementOidStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    ResultSet rs = getArrayElementOidStatement.getResultSet();
    if (!rs.next()) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    rsType = (int) rs.getLong(1);
    boolean onPath = rs.getBoolean(2);
    String schema = rs.getString(3);
    String name = rs.getString(4);
    rsArrayToRsType.put(oid, rsType);
    rsNameToOid.put(schema + "." + name, rsType);
    String fullName = "\"" + schema + "\".\"" + name + "\"";
    rsNameToOid.put(fullName, rsType);
    if (onPath && name.equals(name.toLowerCase())) {
      oidToRsName.put(rsType, name);
      rsNameToOid.put(name, rsType);
    } else {
      oidToRsName.put(rsType, fullName);
    }

    rs.close();

    return rsType;
  }

  public synchronized Class<? extends RedshiftObject> getRSobject(String type) {
    return rsNameToRsObject.get(type);
  }

  public synchronized String getJavaClass(int oid) throws SQLException {
    String pgTypeName = getRSType(oid);

    String result = rsNameToJavaClass.get(pgTypeName);
    if (result != null) {
      return result;
    }

    if(RedshiftLogger.isEnable() && conn.getLogger()!=null)
      conn.getLogger().log(LogLevel.INFO, "Unknown oid found when retrieving the java class Type --" + oid);

    if (getSQLType(pgTypeName) == Types.ARRAY) {
      result = "java.sql.Array";
      rsNameToJavaClass.put(pgTypeName, result);
    }

    return result;
  }

  public String getTypeForAlias(String alias) {
    String type = typeAliases.get(alias);
    if (type != null) {
      return type;
    }
    if (alias.indexOf('"') == -1) {
      type = typeAliases.get(alias.toLowerCase());
      if (type != null) {
        return type;
      }
    }
    return alias;
  }

  public int getPrecision(int oid, int typmod) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.INT2:
        return 5;

      case Oid.OID:
      case Oid.INT4:
        return 10;

      case Oid.INT8:
      case Oid.XIDOID:
        return 19;

      case Oid.FLOAT4:
        // For float4 and float8, we can normally only get 6 and 15
        // significant digits out, but extra_float_digits may raise
        // that number by up to two digits.
        return 8;

      case Oid.FLOAT8:
        return 17;

      case Oid.NUMERIC:
        if (typmod == -1) {
          return 0;
        }
        return ((typmod - 4) & 0xFFFF0000) >> 16;

      case Oid.CHAR:
      case Oid.BOOL:
        return 1;

      case Oid.BPCHAR:
      case Oid.VARCHAR:
      case Oid.SUPER:
      case Oid.VARBYTE:
      case Oid.GEOGRAPHY:
      case Oid.TIDOID:
      case Oid.ABSTIMEOID:
      	
        if (typmod == -1) {
          return unknownLength;
        }
        return typmod - 4;

      // datetime types get the
      // "length in characters of the String representation"
      case Oid.DATE:
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.INTERVAL:
      case Oid.INTERVALY2M:
      case Oid.INTERVALD2S:
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        return getDisplaySize(oid, typmod);

      case Oid.BIT:
        return typmod;

      case Oid.VARBIT:
        if (typmod == -1) {
          return unknownLength;
        }
        return typmod;

      case Oid.NAME:
      	return 64;
        
      case Oid.TEXT:
      case Oid.BYTEA:
      default:
        return unknownLength;
    }
  }

  public int getScale(int oid, int typmod) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.FLOAT4:
        return 8;
      case Oid.FLOAT8:
        return 17;
      case Oid.NUMERIC:
        if (typmod == -1) {
          return 0;
        }
        return (typmod - 4) & 0xFFFF;
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        if (typmod == -1) {
          return 6;
        }
        return typmod;
      case Oid.INTERVAL:
        if (typmod == -1) {
          return 6;
        }
        return typmod & 0xFFFF;
      default:
        return 0;
    }
  }

  public boolean isCaseSensitive(int oid) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.OID:
      case Oid.INT2:
      case Oid.INT4:
      case Oid.INT8:
      case Oid.FLOAT4:
      case Oid.FLOAT8:
      case Oid.NUMERIC:
      case Oid.BOOL:
      case Oid.BIT:
      case Oid.VARBIT:
      case Oid.DATE:
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
      case Oid.INTERVAL:
      case Oid.INTERVALY2M:
      case Oid.INTERVALD2S:
      case Oid.GEOGRAPHY:
        return false;
      default:
        return true;
    }
  }

  public boolean isSigned(int oid) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.INT2:
      case Oid.INT4:
      case Oid.INT8:
      case Oid.FLOAT4:
      case Oid.FLOAT8:
      case Oid.NUMERIC:
        return true;
      default:
        return false;
    }
  }

  public int getDisplaySize(int oid, int typmod) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.INT2:
        return 6; // -32768 to +32767
      case Oid.INT4:
        return 11; // -2147483648 to +2147483647
      case Oid.OID:
        return 10; // 0 to 4294967295
      case Oid.INT8:
      case Oid.XIDOID:
        return 20; // -9223372036854775808 to +9223372036854775807
      case Oid.FLOAT4:
        // varies based upon the extra_float_digits GUC.
        // These values are for the longest possible length.
        return 15; // sign + 9 digits + decimal point + e + sign + 2 digits
      case Oid.FLOAT8:
        return 25; // sign + 18 digits + decimal point + e + sign + 3 digits
      case Oid.CHAR:
        return 1;
      case Oid.BOOL:
        return 1;
      case Oid.DATE:
        return 13; // "4713-01-01 BC" to "01/01/4713 BC" - "31/12/32767"
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        // Calculate the number of decimal digits + the decimal point.
        int secondSize;
        switch (typmod) {
          case -1:
            secondSize = 6 + 1;
            break;
          case 0:
            secondSize = 0;
            break;
          case 1:
            // Bizarrely SELECT '0:0:0.1'::time(1); returns 2 digits.
            secondSize = 2 + 1;
            break;
          default:
            secondSize = typmod + 1;
            break;
        }

        // We assume the worst case scenario for all of these.
        // time = '00:00:00' = 8
        // date = '5874897-12-31' = 13 (although at large values second precision is lost)
        // date = '294276-11-20' = 12 --enable-integer-datetimes
        // zone = '+11:30' = 6;

        switch (oid) {
          case Oid.TIME:
            return 8 + secondSize;
          case Oid.TIMETZ:
            return 8 + secondSize + 6;
          case Oid.TIMESTAMP:
            return 13 + 1 + 8 + secondSize;
          case Oid.TIMESTAMPTZ:
            return 13 + 1 + 8 + secondSize + 6;
        }
      case Oid.INTERVAL:
      case Oid.INTERVALY2M:
      case Oid.INTERVALD2S:
        // SELECT LENGTH('-123456789 years 11 months 33 days 23 hours 10.123456 seconds'::interval);
        return 49;
      case Oid.VARCHAR:
      case Oid.BPCHAR:
      case Oid.SUPER:
      case Oid.VARBYTE:
      case Oid.GEOGRAPHY:
      case Oid.TIDOID:
      case Oid.ABSTIMEOID:
        if (typmod == -1) {
          return unknownLength;
        }
        return typmod - 4;
      case Oid.NUMERIC:
        if (typmod == -1) {
          return 131089; // SELECT LENGTH(pow(10::numeric,131071)); 131071 = 2^17-1
        }
        int precision = (typmod - 4 >> 16) & 0xffff;
        int scale = (typmod - 4) & 0xffff;
        // sign + digits + decimal point (only if we have nonzero scale)
        return 1 + precision + (scale != 0 ? 1 : 0);
      case Oid.BIT:
        return typmod;
      case Oid.VARBIT:
        if (typmod == -1) {
          return unknownLength;
        }
        return typmod;

      case Oid.NAME:
      	return 64;
        
      case Oid.TEXT:
      case Oid.BYTEA:
        return unknownLength;
      default:
        return unknownLength;
    }
  }

  public int getMaximumPrecision(int oid) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.NUMERIC:
        return 1000;
      case Oid.TIME:
      case Oid.TIMETZ:
        // Technically this depends on the --enable-integer-datetimes
        // configure setting. It is 6 with integer and 10 with float.
        return 6;
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
      case Oid.INTERVAL:
      case Oid.INTERVALD2S:
        return 6;
      case Oid.BPCHAR:
      case Oid.VARCHAR:
        return 10485760;
      case Oid.SUPER:
        return 4194304;
      case Oid.VARBYTE:
      case Oid.GEOGRAPHY:
        return 1000000;
      case Oid.BIT:
      case Oid.VARBIT:
        return 83886080;
      default:
        return 0;
    }
  }

  public boolean requiresQuoting(int oid) throws SQLException {
    int sqlType = getSQLType(oid);
    return requiresQuotingSqlType(sqlType);
  }

  /**
   * Returns true if particular sqlType requires quoting.
   * This method is used internally by the driver, so it might disappear without notice.
   *
   * @param sqlType sql type as in java.sql.Types
   * @return true if the type requires quoting
   * @throws SQLException if something goes wrong
   */
  public boolean requiresQuotingSqlType(int sqlType) throws SQLException {
    switch (sqlType) {
      case Types.BIGINT:
      case Types.DOUBLE:
      case Types.FLOAT:
      case Types.INTEGER:
      case Types.REAL:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return false;
    }
    return true;
  }
}
