/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.BaseStatement;
import com.amazon.redshift.core.Field;
import com.amazon.redshift.core.Oid;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.TypeInfo;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.ByteConverter;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.JdbcBlackHole;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.math.BigInteger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class RedshiftDatabaseMetaData implements DatabaseMetaData {

	// Universal (local+external), local , and external schema indicators.
	private final  int NO_SCHEMA_UNIVERSAL_QUERY = 0;
	private final  int LOCAL_SCHEMA_QUERY = 1;
	private final  int EXTERNAL_SCHEMA_QUERY = 2;
	
  public RedshiftDatabaseMetaData(RedshiftConnectionImpl conn) {
    this.connection = conn;
  }

  private String keywords;

  protected final RedshiftConnectionImpl connection; // The connection association

  private int nameDataLength = 0; // length for name datatype
  private int indexMaxKeys = 0; // maximum number of keys in an index.

  protected int getMaxIndexKeys() throws SQLException {
    if (indexMaxKeys == 0) {
/*    	dev=#  show max_index_keys;
    	 max_index_keys
    	----------------
    	 32
    	(1 row) */
    	indexMaxKeys = 32;
/*      String sql;
      sql = "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'";

      Statement stmt = connection.createStatement();
      ResultSet rs = null;
      try {
        rs = stmt.executeQuery(sql);
        if (!rs.next()) {
          stmt.close();
          throw new RedshiftException(
              GT.tr(
                  "Unable to determine a value for MaxIndexKeys due to missing system catalog data."),
              RedshiftState.UNEXPECTED_ERROR);
        }
        indexMaxKeys = rs.getInt(1);
      } finally {
        JdbcBlackHole.close(rs);
        JdbcBlackHole.close(stmt);
      } */
    }
    return indexMaxKeys;
  }

  protected int getMaxNameLength() throws SQLException {
    if (nameDataLength == 0) {
      String sql;
      sql = "SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n "
            + "WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'";

      Statement stmt = connection.createStatement();
      ResultSet rs = null;
      try {
        rs = stmt.executeQuery(sql);
        if (!rs.next()) {
          throw new RedshiftException(GT.tr("Unable to find name datatype in the system catalogs."),
              RedshiftState.UNEXPECTED_ERROR);
        }
        nameDataLength = rs.getInt("typlen");
      } finally {
        JdbcBlackHole.close(rs);
        JdbcBlackHole.close(stmt);
      }
    }
    return nameDataLength - 1;
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return true; // For now...
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true; // For now...
  }

  public String getURL() throws SQLException {
    return connection.getURL();
  }

  public String getUserName() throws SQLException {
    return connection.getUserName();
  }

  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return true;
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  /**
   * Retrieves the name of this database product. We hope that it is Redshift, so we return that
   * explicitly.
   *
   * @return "Redshift"
   */
  @Override
  public String getDatabaseProductName() throws SQLException {
    return "Redshift"; // "PostgreSQL" "Redshift";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return connection.getDBVersionNumber();
  }

  @Override
  public String getDriverName() {
    return com.amazon.redshift.util.DriverInfo.DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() {
    return com.amazon.redshift.util.DriverInfo.DRIVER_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    return com.amazon.redshift.util.DriverInfo.MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() {
    return com.amazon.redshift.util.DriverInfo.MINOR_VERSION;
  }

  /**
   * Does the database store tables in a local file? No - it stores them in a file on the server.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  /**
   * Does the database use a file for each table? Well, not really, since it doesn't use local files.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  /**
   * Does the database treat mixed case unquoted SQL identifiers as case sensitive and as a result
   * store them in mixed case? A JDBC-Compliant driver will always return false.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  /**
   * Does the database treat mixed case quoted SQL identifiers as case sensitive and as a result
   * store them in mixed case? A JDBC compliant driver will always return true.
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  /**
   * What is the string used to quote SQL identifiers? This returns a space if identifier quoting
   * isn't supported. A JDBC Compliant driver will always use a double quote character.
   *
   * @return the quoting string
   * @throws SQLException if a database access error occurs
   */
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  /**
   * {@inheritDoc}
   *
   * <p>From PostgreSQL 9.0+ return the keywords from pg_catalog.pg_get_keywords()</p>
   *
   * @return a comma separated list of keywords we use
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSQLKeywords() throws SQLException {
    // Static list from PG8.2 src/backend/parser/keywords.c with SQL:2003 excluded.
    String keywords = "abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,"
        + "cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,"
        + "delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,"
        + "greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,"
        + "instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,"
        + "nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,"
        + "recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,"
        + "statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,"
        + "unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile";
    return keywords;
  }

  public String getNumericFunctions() throws SQLException {
    return EscapedFunctions.ABS + ',' + EscapedFunctions.ACOS + ',' + EscapedFunctions.ASIN + ','
        + EscapedFunctions.ATAN + ',' + EscapedFunctions.ATAN2 + ',' + EscapedFunctions.CEILING
        + ',' + EscapedFunctions.COS + ',' + EscapedFunctions.COT + ',' + EscapedFunctions.DEGREES
        + ',' + EscapedFunctions.EXP + ',' + EscapedFunctions.FLOOR + ',' + EscapedFunctions.LOG
        + ',' + EscapedFunctions.LOG10 + ',' + EscapedFunctions.MOD + ',' + EscapedFunctions.PI
        + ',' + EscapedFunctions.POWER + ',' + EscapedFunctions.RADIANS + ',' + EscapedFunctions.RANDOM + ','
        + EscapedFunctions.ROUND + ',' + EscapedFunctions.SIGN + ',' + EscapedFunctions.SIN + ','
        + EscapedFunctions.SQRT + ',' + EscapedFunctions.TAN + ',' + EscapedFunctions.TRUNCATE;

  }

  public String getStringFunctions() throws SQLException {
    String funcs = EscapedFunctions.ASCII + ',' + EscapedFunctions.CHAR + ','
    		+ EscapedFunctions.CHAR_LENGTH + ',' + EscapedFunctions.CHARACTER_LENGTH + ','
        + EscapedFunctions.CONCAT + ',' + EscapedFunctions.LCASE + ',' + EscapedFunctions.LEFT + ','
        + EscapedFunctions.LENGTH + ',' + EscapedFunctions.LTRIM + ',' 
        + EscapedFunctions.OCTET_LENGTH + ',' + EscapedFunctions.POSITION + ',' 
        + EscapedFunctions.REPEAT + ','
        + EscapedFunctions.RIGHT + ',' + EscapedFunctions.RTRIM + ',' + EscapedFunctions.SPACE + ','
        + EscapedFunctions.SUBSTRING + ',' + EscapedFunctions.UCASE;

    // Currently these don't work correctly with parameterized
    // arguments, so leave them out. They reorder the arguments
    // when rewriting the query, but no translation layer is provided,
    // so a setObject(N, obj) will not go to the correct parameter.
    // ','+EscapedFunctions.INSERT+','+EscapedFunctions.LOCATE+
    // ','+EscapedFunctions.RIGHT+

    funcs += ',' + EscapedFunctions.REPLACE;

    return funcs;
  }

  public String getSystemFunctions() throws SQLException {
    return EscapedFunctions.DATABASE + ',' + EscapedFunctions.IFNULL + ',' + EscapedFunctions.USER;
  }

  public String getTimeDateFunctions() throws SQLException {
    String timeDateFuncs = EscapedFunctions.CURDATE + ',' + EscapedFunctions.CURTIME + ','
        + EscapedFunctions.DAYNAME + ',' + EscapedFunctions.DAYOFMONTH + ','
        + EscapedFunctions.DAYOFWEEK + ',' + EscapedFunctions.DAYOFYEAR + ','
        + EscapedFunctions.HOUR + ',' + EscapedFunctions.MINUTE + ',' + EscapedFunctions.MONTH + ','
        + EscapedFunctions.MONTHNAME + ',' + EscapedFunctions.NOW + ',' + EscapedFunctions.QUARTER
        + ',' + EscapedFunctions.SECOND + ',' + EscapedFunctions.WEEK + ',' + EscapedFunctions.YEAR;

    timeDateFuncs += ',' + EscapedFunctions.TIMESTAMPADD;

    // +','+EscapedFunctions.TIMESTAMPDIFF;

    return timeDateFuncs;
  }

  public String getSearchStringEscape() throws SQLException {
    // This method originally returned "\\\\" assuming that it
    // would be fed directly into pg's input parser so it would
    // need two backslashes. This isn't how it's supposed to be
    // used though. If passed as a PreparedStatement parameter
    // or fed to a DatabaseMetaData method then double backslashes
    // are incorrect. If you're feeding something directly into
    // a query you are responsible for correctly escaping it.
    // With 8.2+ this escaping is a little trickier because you
    // must know the setting of standard_conforming_strings, but
    // that's not our problem.

    return "\\";
  }

  /**
   * {@inheritDoc}
   *
   * <p>Redshift allows any high-bit character to be used in an unquoted identifier, so we can't
   * possibly list them all.</p>
   *
   * <p>From the file src/backend/parser/scan.l, an identifier is ident_start [A-Za-z\200-\377_]
   * ident_cont [A-Za-z\200-\377_0-9\$] identifier {ident_start}{ident_cont}*</p>
   *
   * @return a string containing the extra characters
   * @throws SQLException if a database access error occurs
   */
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  /**
   * {@inheritDoc}
   *
   * @return true
   */
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true
   */
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return true;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    return false;
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true
   */
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  /*
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  /*
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return true;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This grammar is defined at:
   * <a href="http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm">
   *     http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm</a></p>
   *
   * <p>In Appendix C. From this description, we seem to support the ODBC minimal (Level 0) grammar.</p>
   *
   * @return true
   */
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  /**
   * Does this driver support the Core ODBC SQL grammar. We need SQL-92 conformance for this.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  /**
   * Does this driver support the Extended (Level 2) ODBC SQL grammar. We don't conform to the Core
   * (Level 1), so we can't conform to the Extended SQL Grammar.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  /**
   * Does this driver support the ANSI-92 entry level SQL grammar? All JDBC Compliant drivers must
   * return true. We currently report false until 'schema' support is added. Then this should be
   * changed to return true, since we will be mostly compliant (probably more compliant than many
   * other databases) And since this is a requirement for all JDBC drivers we need to get to the
   * point where we can return true.
   *
   * @return true 
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return false
   */
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @return false
   */
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  /*
   * Is the SQL Integrity Enhancement Facility supported? Our best guess is that this means support
   * for constraints
   *
   * @return true
   *
   * @exception SQLException if a database access error occurs
   */
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>Redshift doesn't have schemas, but when it does, we'll use the term "schema".</p>
   *
   * @return {@code "schema"}
   */
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "procedure"}
   */
  public String getProcedureTerm() throws SQLException {
    return "procedure"; // function 
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code "database"}
   */
  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  /**
   * We support cursors for gets only it seems. I dont see a method to get a positioned delete.
   *
   * @return false
   * @throws SQLException if a database access error occurs
   */
  public boolean supportsPositionedDelete() throws SQLException {
    return false; // For now...
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false; // For now...
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsSelectForUpdate() throws SQLException {
    return true;
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsUnion() throws SQLException {
    return true; // since 6.3
  }

  /**
   * {@inheritDoc}
   *
   * @return true 
   */
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc} In Redshift, Cursors are only open within transactions.
   */
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   * <p>Can statements remain open across commits? They may, but this driver cannot guarantee that. In
   * further reflection. we are talking a Statement object here, so the answer is yes, since the
   * Statement is only a vehicle to ExecSQL()</p>
   *
   * @return true
   */
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>Can statements remain open across rollbacks? They may, but this driver cannot guarantee that.
   * In further contemplation, we are talking a Statement object here, so the answer is yes, since
   * the Statement is only a vehicle to ExecSQL() in Connection</p>
   *
   * @return true
   */
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

  public int getMaxCharLiteralLength() throws SQLException {
    return 0; // no limit
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0; // no limit
  }

  public int getMaxColumnNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0; // no limit
  }

  public int getMaxColumnsInIndex() throws SQLException {
    return getMaxIndexKeys();
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0; // no limit
  }

  public int getMaxColumnsInSelect() throws SQLException {
    return 0; // no limit
  }

  /**
   * {@inheritDoc} What is the maximum number of columns in a table? From the CREATE TABLE reference
   * page...
   *
   * <p>"The new class is created as a heap with no initial data. A class can have no more than 1600
   * attributes (realistically, this is limited by the fact that tuple sizes must be less than 8192
   * bytes)..."</p>
   *
   * @return the max columns
   * @throws SQLException if a database access error occurs
   */
  public int getMaxColumnsInTable() throws SQLException {
    return 1600;
  }

  /**
   * {@inheritDoc} How many active connection can we have at a time to this database? Well, since it
   * depends on postmaster, which just does a listen() followed by an accept() and fork(), its
   * basically very high. Unless the system runs out of processes, it can be 65535 (the number of
   * aux. ports on a TCP/IP system). I will return 8192 since that is what even the largest system
   * can realistically handle,
   *
   * @return the maximum number of connections
   * @throws SQLException if a database access error occurs
   */
  public int getMaxConnections() throws SQLException {
    return 0; // 8192
  }

  public int getMaxCursorNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxIndexLength() throws SQLException {
    return 0; // no limit (larger than an int anyway)
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxProcedureNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxCatalogNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxRowSize() throws SQLException {
    return 1073741824; // 1 GB
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    return 0; // actually whatever fits in size_t
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTableNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 0; // no limit
  }

  public int getMaxUserNameLength() throws SQLException {
    return getMaxNameLength();
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_SERIALIZABLE; // TRANSACTION_READ_COMMITTED;
  }

  public boolean supportsTransactions() throws SQLException {
    return true;
  }

  /**
   * {@inheritDoc}
   * <p>We only support TRANSACTION_SERIALIZABLE and TRANSACTION_READ_COMMITTED before 8.0; from 8.0
   * READ_UNCOMMITTED and REPEATABLE_READ are accepted aliases for READ_COMMITTED.</p>
   */
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
  	return (level == Connection.TRANSACTION_SERIALIZABLE);
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return true;
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  /**
   * <p>Does a data definition statement within a transaction force the transaction to commit? It seems
   * to mean something like:</p>
   *
   * <pre>
   * CREATE TABLE T (A INT);
   * INSERT INTO T (A) VALUES (2);
   * BEGIN;
   * UPDATE T SET A = A + 1;
   * CREATE TABLE X (A INT);
   * SELECT A FROM T INTO X;
   * COMMIT;
   * </pre>
   *
   * <p>Does the CREATE TABLE call cause a commit? The answer is no.</p>
   *
   * @return true if so
   * @throws SQLException if a database access error occurs
   */
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  /**
   * Turn the provided value into a valid string literal for direct inclusion into a query. This
   * includes the single quotes needed around it.
   *
   * @param s input value
   *
   * @return string literal for direct inclusion into a query
   * @throws SQLException if something wrong happens
   */
  protected String escapeQuotes(String s) throws SQLException {
    StringBuilder sb = new StringBuilder();
/*    if (!connection.getStandardConformingStrings()) {
      sb.append("E");
    } */
    sb.append("'");
    sb.append(connection.escapeString(s));
    sb.append("'");
    return sb.toString();
  }

  protected String escapeOnlyQuotes(String s) throws SQLException {
    StringBuilder sb = new StringBuilder();
/*    if (!connection.getStandardConformingStrings()) {
      sb.append("E");
    } */
    sb.append("'");
    sb.append(connection.escapeOnlyQuotesString(s));
    sb.append("'");
    return sb.toString();
  }
  
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    String sql;
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern, procedureNamePattern);
    
    sql = "SELECT current_database() AS PROCEDURE_CAT, n.nspname AS PROCEDURE_SCHEM, p.proname AS PROCEDURE_NAME, "
          + "NULL, NULL, NULL, d.description AS REMARKS, "
          + " CASE  "
          + " WHEN p.prokind='f' or p.proargmodes is not null THEN 2 "
          + " WHEN p.prokind='p' THEN 1 "
          + " ELSE 0 "
          + " END AS PROCEDURE_TYPE, "
          + " p.proname || '_' || p.prooid AS SPECIFIC_NAME "
          + " FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc_info p "
          + " LEFT JOIN pg_catalog.pg_description d ON (p.prooid=d.objoid) "
          + " LEFT JOIN pg_catalog.pg_class c ON (d.classoid=c.oid AND c.relname='pg_proc') "
          + " LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace=pn.oid AND pn.nspname='pg_catalog') "
          + " WHERE p.pronamespace=n.oid ";

/*    if (connection.haveMinimumServerVersion(ServerVersion.v11)) {
      sql += " AND p.prokind='p'";
    } */
    
    sql += getCatalogFilterCondition(catalog);
    
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
      sql += " AND n.nspname LIKE " + escapeQuotes(schemaPattern);
    } else {
      /* limit to current schema if no schema given */
      sql += "and pg_function_is_visible(p.prooid)";
    }
    if (procedureNamePattern != null && !procedureNamePattern.isEmpty()) {
      sql += " AND p.proname LIKE " + escapeQuotes(procedureNamePattern);
    }
    if (connection.getHideUnprivilegedObjects()) {
      sql += " AND has_function_privilege(p.prooid,'EXECUTE')";
    }
    sql += " ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, p.prooid::text ";

    ResultSet rs = createMetaDataStatement().executeQuery(sql);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);

    return rs;
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern) throws SQLException {
    String sql;
    final String unknownColumnSize = "2147483647";
    
    StringBuilder procedureColQuery = new StringBuilder();

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    
    procedureColQuery.append(
        "SELECT PROCEDURE_CAT , PROCEDURE_SCHEM , PROCEDURE_NAME, COLUMN_NAME, "
        + " COLUMN_TYPE, DATA_TYPE, TYPE_NAME, COLUMN_SIZE AS PRECISION, LENGTH , DECIMAL_DIGITS AS SCALE,  "
        + " NUM_PREC_RADIX AS RADIX, NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, "
        + " CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE, SPECIFIC_NAME  "
        + " FROM (");

    procedureColQuery.append("SELECT current_database() AS PROCEDURE_CAT, "
            + " n.nspname as PROCEDURE_SCHEM, "
            + " p.proname AS PROCEDURE_NAME, "

            + " CAST(CASE ((array_upper(proargnames, 0) - array_lower(proargnames, 0)) > 0) "
            + " WHEN 't' THEN proargnames[array_upper(proargnames, 1)] "
            + " ELSE '' "
            + " END AS VARCHAR(256)) AS COLUMN_NAME, "

            + " CAST(CASE p.proretset "
            + " WHEN 't' THEN 3 "
            + " ELSE 0 "
            + " END AS SMALLINT) AS COLUMN_TYPE, "
            + " CAST(CASE pg_catalog.format_type(p.prorettype, NULL) "
            + " WHEN 'text' THEN 12 "
            + " WHEN 'bit' THEN  -7 "
            + " WHEN 'bool' THEN  -7 "
            + " WHEN 'boolean' THEN  -7 "
            + " WHEN 'varchar' THEN 12 "
            + " WHEN 'character varying' THEN  12 "
            + " WHEN '\"char\"' THEN 1"
            + " WHEN 'char' THEN  1 "
            + " WHEN 'character' THEN  1 "
            + " WHEN 'nchar' THEN 1 "
            + " WHEN 'bpchar' THEN 1 "
            + " WHEN 'nvarchar' THEN 12 "
            + " WHEN 'date' THEN 91 "
            + " WHEN 'time' THEN 92 "
            + " WHEN 'time without time zone' THEN 92 "
            + " WHEN 'timetz' THEN 2013 "
            + " WHEN 'time with time zone' THEN 2013 "
            + " WHEN 'timestamp' THEN 93 "
            + " WHEN 'timestamp without time zone' THEN 93 "
            + " WHEN 'timestamptz' THEN 2014 "
            + " WHEN 'timestamp with time zone' THEN 2014 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN -5 "
            + " WHEN 'int8' THEN -5 "
            + " WHEN 'real' THEN 7 "
            + " WHEN 'float4' THEN 7 "
            + " WHEN 'double precision' THEN 6 "
            + " WHEN 'float8' THEN 6 "
            + " WHEN 'float' THEN 6 "
            + " WHEN 'decimal' THEN 3 "
            + " WHEN 'numeric' THEN 2 "
            + " WHEN '_float4' THEN 2003 "
            + " WHEN '_aclitem' THEN 2003 "
            + " WHEN '_text' THEN 2003 "
            + " WHEN 'bytea' THEN -2 "
            + " WHEN 'oid' THEN -5 "
            + " WHEN 'name' THEN 12 "
            + " WHEN '_int4' THEN 2003 "
            + " WHEN '_int2' THEN 2003 "
            + " WHEN 'ARRAY' THEN 2003 "
            + " WHEN 'geometry' THEN -4 "
            + " WHEN 'super' THEN -16 "
            + " WHEN 'varbyte' THEN -4 "
            + " WHEN 'geography' THEN -4 "
            + " ELSE 1111 "
            + " END AS SMALLINT) AS DATA_TYPE, "
            + " pg_catalog.format_type(p.prorettype, NULL) AS TYPE_NAME, "
            + " CASE pg_catalog.format_type(p.prorettype, NULL) "
            + " WHEN 'text' THEN NULL "
            + " WHEN 'varchar' THEN NULL "
            + " WHEN 'character varying' THEN NULL "
            + " WHEN '\"char\"' THEN NULL "
            + " WHEN 'character' THEN NULL "
            + " WHEN 'nchar' THEN NULL "
            + " WHEN 'bpchar' THEN NULL "
            + " WHEN 'nvarchar' THEN NULL "
            + " WHEN 'date' THEN 10 "
            + " WHEN 'time' THEN 15 "
            + " WHEN 'time without time zone' THEN 15 "
            + " WHEN 'timetz' THEN 21 "
            + " WHEN 'time with time zone' THEN 21 "
            + " WHEN 'timestamp' THEN 29 "
            + " WHEN 'timestamp without time zone' THEN 29 "
            + " WHEN 'timestamptz' THEN 35 "
            + " WHEN 'timestamp with time zone' THEN 35 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 10 "
            + " WHEN 'int' THEN 10 "
            + " WHEN 'int4' THEN 10 "
            + " WHEN 'bigint' THEN 19 "
            + " WHEN 'int8' THEN 19 "
            + " WHEN 'decimal' THEN 38 "
            + " WHEN 'real' THEN 24 "
            + " WHEN 'float4' THEN 53 "
            + " WHEN 'double precision' THEN 53 "
            + " WHEN 'float8' THEN 53 "
            + " WHEN 'float' THEN 53 "
            + " WHEN 'geometry' THEN NULL "
            + " WHEN 'super' THEN 4194304 "
            + " WHEN 'varbyte' THEN NULL "
            + " WHEN 'geography' THEN NULL "
            + " ELSE " + unknownColumnSize
            + " END AS COLUMN_SIZE, "
            + " CASE pg_catalog.format_type(p.prorettype, NULL) "
            + " WHEN 'text' THEN NULL "
            + " WHEN 'varchar' THEN NULL "
            + " WHEN 'character varying' THEN NULL "
            + " WHEN '\"char\"' THEN NULL "
            + " WHEN 'character' THEN NULL "
            + " WHEN 'nchar' THEN NULL "
            + " WHEN 'bpchar' THEN NULL "
            + " WHEN 'nvarchar' THEN NULL "
            + " WHEN 'date' THEN 6 "
            + " WHEN 'time' THEN 15 "
            + " WHEN 'time without time zone' THEN 15 "
            + " WHEN 'timetz' THEN 21 "
            + " WHEN 'time with time zone' THEN 21 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'timestamp without time zone' THEN 6 "
            + " WHEN 'timestamptz' THEN 35 "
            + " WHEN 'timestamp with time zone' THEN 35 "
            + " WHEN 'smallint' THEN 2 "
            + " WHEN 'int2' THEN 2 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN 20 "
            + " WHEN 'int8' THEN 20 "
            + " WHEN 'decimal' THEN 8 "
            + " WHEN 'real' THEN 4 "
            + " WHEN 'float4' THEN 8 "
            + " WHEN 'double precision' THEN 8 "
            + " WHEN 'float8' THEN 8 "
            + " WHEN 'float' THEN  8 "
            + " WHEN 'geometry' THEN NULL "
            + " WHEN 'super' THEN 4194304 "
            + " WHEN 'varbyte' THEN NULL "
            + " WHEN 'geography' THEN NULL "
            + " END AS LENGTH, "
            + " CAST(CASE pg_catalog.format_type(p.prorettype, NULL) "
            + " WHEN 'smallint' THEN 0 "
            + " WHEN 'int2' THEN 0 "
            + " WHEN 'integer' THEN 0 "
            + " WHEN 'int' THEN 0 "
            + " WHEN 'int4' THEN 0 "
            + " WHEN 'bigint' THEN 0 "
            + " WHEN 'int8' THEN 0 "
            + " WHEN 'decimal' THEN 0 "
            + " WHEN 'real' THEN 8 "
            + " WHEN 'float4' THEN 8 "
            + " WHEN 'double precision' THEN 17 "
            + " WHEN 'float' THEN 17 "
            + " WHEN 'float8' THEN 17 "
            + " WHEN 'time' THEN 6 "
            + " WHEN 'time without time zone' THEN 6 "
            + " WHEN 'timetz' THEN 6 "
            + " WHEN 'time with time zone' THEN 6 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'timestamp without time zone' THEN 6 "
            + " WHEN 'timestamptz' THEN 6 "
            + " WHEN 'timestamp with time zone' THEN 6 "
            + " ELSE NULL END AS SMALLINT) AS DECIMAL_DIGITS, "
            + " 10 AS NUM_PREC_RADIX, "
            + " CAST(2 AS SMALLINT) AS NULLABLE, "
            + " CAST('' AS VARCHAR(256)) AS REMARKS, "
            + " NULL AS COLUMN_DEF, "
            + " CAST(CASE  pg_catalog.format_type(p.prorettype, NULL)"
            + " WHEN 'text' THEN 12 "
            + " WHEN 'bit' THEN  -7 "
            + " WHEN 'bool' THEN  -7 "
            + " WHEN 'boolean' THEN  -7 "
            + " WHEN 'varchar' THEN 12 "
            + " WHEN 'character varying' THEN  12 "
            + " WHEN '\"char\"' THEN 1"
            + " WHEN 'char' THEN  1 "
            + " WHEN 'character' THEN  1 "
            + " WHEN 'nchar' THEN 1 "
            + " WHEN 'bpchar' THEN 1 "
            + " WHEN 'nvarchar' THEN 12 "
            + " WHEN 'date' THEN 91 "
            + " WHEN 'time' THEN 92 "
            + " WHEN 'time without time zone' THEN 92 "
            + " WHEN 'timetz' THEN 2013 "
            + " WHEN 'time with time zone' THEN 2013 "
            + " WHEN 'timestamp' THEN 93 "
            + " WHEN 'timestamp without time zone' THEN 93 "
            + " WHEN 'timestamptz' THEN 2014 "
            + " WHEN 'timestamp with time zone' THEN 2014 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN -5 "
            + " WHEN 'int8' THEN -5 "
            + " WHEN 'real' THEN 7 "
            + " WHEN 'float4' THEN 7 "
            + " WHEN 'double precision' THEN 6 "
            + " WHEN 'float8' THEN 6 "
            + " WHEN 'float' THEN 6 "
            + " WHEN 'decimal' THEN 3 "
            + " WHEN 'numeric' THEN 2 "
            + " WHEN 'bytea' THEN -2 "
            + " WHEN 'oid' THEN -5 "
            + " WHEN 'name' THEN 12 "
            + " WHEN 'ARRAY' THEN 2003 "
            + " WHEN 'geometry' THEN -4 "
            + " WHEN 'super' THEN -16 "
            + " WHEN 'varbyte' THEN -4 "
            + " WHEN 'geography' THEN -4 "
            + " END AS SMALLINT) AS SQL_DATA_TYPE, "
            + " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, "
            + " CAST(NULL AS SMALLINT) AS CHAR_OCTET_LENGTH, "
            + " CAST(0 AS SMALLINT) AS ORDINAL_POSITION, "
            + " CAST('' AS VARCHAR(256)) AS IS_NULLABLE, "
            + " p.proname || '_' || p.prooid AS SPECIFIC_NAME, "
            + " p.prooid as PROOID, "
            + " -1 AS PROARGINDEX "

            + " FROM pg_catalog.pg_proc_info p LEFT JOIN pg_namespace n ON n.oid = p.pronamespace "
            + " WHERE pg_catalog.format_type(p.prorettype, NULL) != 'void' ");
    
    procedureColQuery.append(getCatalogFilterCondition(catalog));
    
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	procedureColQuery.append(" AND n.nspname LIKE " + escapeQuotes(schemaPattern));
    }

    if (procedureNamePattern != null && !procedureNamePattern.isEmpty()) {
    	procedureColQuery.append(" AND proname LIKE " + escapeQuotes(procedureNamePattern));
    }
    
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
    	procedureColQuery.append(" AND COLUMN_NAME LIKE " + escapeQuotes(columnNamePattern));
    }
    
    procedureColQuery.append(" UNION ALL ");

    procedureColQuery.append(" SELECT DISTINCT current_database() AS PROCEDURE_CAT, "
            + " PROCEDURE_SCHEM, "
            + " PROCEDURE_NAME, "
            + "CAST(CASE (char_length(COLUMN_NAME) > 0) WHEN 't' THEN COLUMN_NAME "
            + "ELSE '' "
            + "END AS VARCHAR(256)) AS COLUMN_NAME, "

            + " CAST( CASE COLUMN_TYPE "
            + " WHEN 105 THEN 1 "
            + " WHEN 98 THEN 2 "
            + " WHEN 111 THEN 4 "
            + " ELSE 0 END AS SMALLINT) AS COLUMN_TYPE, "
            + " CAST(CASE DATA_TYPE "
            + " WHEN 'text' THEN 12 "
            + " WHEN 'bit' THEN  -7 "
            + " WHEN 'bool' THEN  -7 "
            + " WHEN 'boolean' THEN  -7 "
            + " WHEN 'varchar' THEN 12 "
            + " WHEN 'character varying' THEN  12 "
            + " WHEN '\"char\"' THEN  1 "
            + " WHEN 'char' THEN  1 "
            + " WHEN 'character' THEN  1 "
            + " WHEN 'nchar' THEN 1 "
            + " WHEN 'bpchar' THEN 1 "
            + " WHEN 'nvarchar' THEN 12 "
            + " WHEN 'date' THEN 91 "
            + " WHEN 'time' THEN 92 "
            + " WHEN 'time without time zone' THEN 92 "
            + " WHEN 'timetz' THEN 2013 "
            + " WHEN 'time with time zone' THEN 2013 "
            + " WHEN 'timestamp' THEN 93 "
            + " WHEN 'timestamp without time zone' THEN 93 "
            + " WHEN 'timestamptz' THEN 2014 "
            + " WHEN 'timestamp with time zone' THEN 2014 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN -5 "
            + " WHEN 'int8' THEN -5 "
            + " WHEN 'real' THEN 7 "
            + " WHEN 'float4' THEN 7 "
            + " WHEN 'double precision' THEN 6 "
            + " WHEN 'float8' THEN 6 "
            + " WHEN 'float' THEN 6 "
            + " WHEN 'decimal' THEN 3 "
            + " WHEN 'numeric' THEN 2 "
            + " WHEN 'bytea' THEN -2 "
            + " WHEN 'oid' THEN -5 "
            + " WHEN 'name' THEN 12 "
            + " WHEN 'ARRAY' THEN 2003 "
            + " WHEN 'geometry' THEN -4 "
            + " WHEN 'super' THEN -16 "
            + " WHEN 'varbyte' THEN -4 "
            + " WHEN 'geography' THEN -4 "
            + " ELSE 1111 "
            + " END AS SMALLINT) AS DATA_TYPE, "
            + " TYPE_NAME, "
            + " CASE COLUMN_SIZE "
            + " WHEN 'text' THEN COLUMN_BYTES "
            + " WHEN 'varchar' THEN COLUMN_BYTES "
            + " WHEN 'character varying' THEN COLUMN_BYTES "
            + " WHEN '\"char\"' THEN COLUMN_BYTES "
            + " WHEN 'character' THEN COLUMN_BYTES "
            + " WHEN 'nchar' THEN COLUMN_BYTES "
            + " WHEN 'bpchar' THEN COLUMN_BYTES "
            + " WHEN 'nvarchar' THEN COLUMN_BYTES "
            + " WHEN 'date' THEN 10 "
            + " WHEN 'time' THEN 15 "
            + " WHEN 'time without time zone' THEN 15 "
            + " WHEN 'timetz' THEN 21 "
            + " WHEN 'time with time zone' THEN 21 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'timestamp without time zone' THEN 6 "
            + " WHEN 'timestamptz' THEN 35 "
            + " WHEN 'timestamp with time zone' THEN 35 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 10 "
            + " WHEN 'int' THEN 10 "
            + " WHEN 'int4' THEN 10 "
            + " WHEN 'bigint' THEN 19 "
            + " WHEN 'int8' THEN 19 "
            + " WHEN 'decimal' THEN 38 "
            + " WHEN 'real' THEN 24 "
            + " WHEN 'float4' THEN 53 "
            + " WHEN 'double precision' THEN 53 "
            + " WHEN 'float8' THEN 53 "
            + " WHEN 'float' THEN 53 "
            + " WHEN 'numeric' THEN NUMERIC_PRECISION "
            + " WHEN 'geometry' THEN NULL "
            + " WHEN 'super' THEN 4194304 "
            + " WHEN 'varbyte' THEN NULL "
            + " WHEN 'geography' THEN NULL "
            + " ELSE " + unknownColumnSize
            + " END AS COLUMN_SIZE, "
            + " CASE LENGTH "
            + " WHEN 'text' THEN COLUMN_BYTES "
            + " WHEN 'varchar' THEN COLUMN_BYTES "
            + " WHEN 'character varying' THEN COLUMN_BYTES "
            + " WHEN '\"char\"' THEN COLUMN_BYTES "
            + " WHEN 'character' THEN COLUMN_BYTES "
            + " WHEN 'nchar' THEN COLUMN_BYTES "
            + " WHEN 'bpchar' THEN COLUMN_BYTES "
            + " WHEN 'nvarchar' THEN COLUMN_BYTES "
            + " WHEN 'date' THEN 6 "
            + " WHEN 'time' THEN 6 "
            + " WHEN 'time without time zone' THEN 6 "
            + " WHEN 'timetz' THEN 6 "
            + " WHEN 'time with time zone' THEN 6 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'timestamp without time zone' THEN 6 "
            + " WHEN 'timestamptz' THEN 6 "
            + " WHEN 'timestamp with time zone' THEN 6 "
            + " WHEN 'smallint' THEN 2 "
            + " WHEN 'int2' THEN 2 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN 20 "
            + " WHEN 'int8' THEN 20 "
            + " WHEN 'decimal' THEN 8 "
            + " WHEN 'real' THEN 4 "
            + " WHEN 'float4' THEN 8 "
            + " WHEN 'double precision' THEN 8 "
            + " WHEN 'float8' THEN 8 "
            + " WHEN 'float' THEN  8 "
            + " WHEN 'geometry' THEN NULL "
            + " WHEN 'super' THEN 4194304 "
            + " WHEN 'varbyte' THEN NULL "
            + " WHEN 'geography' THEN NULL "
            + " END AS LENGTH, "
            + " CAST(CASE DECIMAL_DIGITS "
            + " WHEN 'smallint' THEN 0 "
            + " WHEN 'int2' THEN 0 "
            + " WHEN 'integer' THEN 0 "
            + " WHEN 'int' THEN 0 "
            + " WHEN 'int4' THEN 0 "
            + " WHEN 'bigint' THEN 0 "
            + " WHEN 'int8' THEN 0 "
            + " WHEN 'decimal' THEN 0 "
            + " WHEN 'real' THEN 8 "
            + " WHEN 'float4' THEN 8 "
            + " WHEN 'double precision' THEN 17 "
            + " WHEN 'float' THEN 17 "
            + " WHEN 'float8' THEN 17 "
            + " WHEN 'numeric' THEN NUMERIC_SCALE "
            + " WHEN 'time' THEN 6 "
            + " WHEN 'time without time zone' THEN 6 "
            + " WHEN 'timetz' THEN 6 "
            + " WHEN 'time with time zone' THEN 6 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'timestamp without time zone' THEN 6 "
            + " WHEN 'timestamptz' THEN 6 "
            + " WHEN 'timestamp with time zone' THEN 6 "
            + " ELSE NULL END AS SMALLINT) AS DECIMAL_DIGITS, "
            + " 10 AS NUM_PREC_RADIX, "
            + " CAST(2 AS SMALLINT) AS NULLABLE, "
            + " CAST(''AS VARCHAR(256)) AS REMARKS, "
            + " NULL AS COLUMN_DEF,"
            + " CAST( CASE SQL_DATA_TYPE"
            + " WHEN 'text' THEN 12 "
            + " WHEN 'bit' THEN  -7 "
            + " WHEN 'bool' THEN  -7 "
            + " WHEN 'boolean' THEN  -7 "
            + " WHEN 'varchar' THEN 12 "
            + " WHEN 'character varying' THEN  12 "
            + " WHEN '\"char\"' THEN  1 "
            + " WHEN 'char' THEN  1 "
            + " WHEN 'character' THEN  1 "
            + " WHEN 'nchar' THEN 1 "
            + " WHEN 'bpchar' THEN 1 "
            + " WHEN 'nvarchar' THEN 12 "
            + " WHEN 'date' THEN 91 "
            + " WHEN 'time' THEN 92 "
            + " WHEN 'time without time zone' THEN 92 "
            + " WHEN 'timetz' THEN 2013 "
            + " WHEN 'time with time zone' THEN 2013 "
            + " WHEN 'timestamp' THEN 93 "
            + " WHEN 'timestamp without time zone' THEN 93 "
            + " WHEN 'timestamptz' THEN 2014 "
            + " WHEN 'timestamp with time zone' THEN 2014 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN -5 "
            + " WHEN 'int8' THEN -5 "
            + " WHEN 'real' THEN 7 "
            + " WHEN 'float4' THEN 7 "
            + " WHEN 'double precision' THEN 6 "
            + " WHEN 'float8' THEN 6 "
            + " WHEN 'float' THEN 6 "
            + " WHEN 'decimal' THEN 3 "
            + " WHEN 'numeric' THEN 2 "
            + " WHEN 'bytea' THEN -2 "
            + " WHEN 'oid' THEN -5 "
            + " WHEN 'name' THEN 12 "
            + " WHEN 'ARRAY' THEN 2003 "
            + " WHEN 'geometry' THEN -4 "
            + " WHEN 'super' THEN -16 "
            + " WHEN 'varbyte' THEN -4 "
            + " WHEN 'geography' THEN -4 "
            + " END AS SMALLINT) AS SQL_DATA_TYPE, "
            + " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, "
            + " CAST(NULL AS SMALLINT) AS CHAR_OCTET_LENGTH, "
            + " PROARGINDEX AS ORDINAL_POSITION, "
            + " CAST(''AS VARCHAR(256)) AS IS_NULLABLE, "
            + " SPECIFIC_NAME, PROOID, PROARGINDEX "
            + " FROM ( "
            + " SELECT current_database() AS PROCEDURE_CAT,"
            + " n.nspname AS PROCEDURE_SCHEM, "
            + " proname AS PROCEDURE_NAME, "
            + " CASE WHEN (proallargtypes is NULL) THEN proargnames[pos+1] "
            + " ELSE proargnames[pos] END AS COLUMN_NAME,"
            + " CASE WHEN proargmodes is NULL THEN 105 "
            + " ELSE CAST(proargmodes[pos] AS INT) END AS COLUMN_TYPE, "
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS DATA_TYPE,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL) "
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS TYPE_NAME,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS COLUMN_SIZE,"
            + " CASE  WHEN (proallargtypes IS NOT NULL) and prokind='p' AND proallargtypes[pos] IN (1042, 1700, 1043) "
            + "				THEN (string_to_array(textin(byteaout(substring(probin from 1 for length(probin)-3))),','))[pos]::integer "
            + " 			WHEN (proallargtypes IS NULL) AND prokind='p' AND proargtypes[pos] IN (1042,1700,1043) "
            +	"				THEN (string_to_array(textin(byteaout(substring(probin FROM 1 FOR length(probin)-3))), ',')) [pos+1]::integer "
            + " END AS PROBIN_BYTES, "
            + " CASE "
            + "   WHEN (PROBIN_BYTES IS NOT NULL) "
            + " 				AND (proallargtypes[pos] IN (1042, 1043) or proargtypes[pos] in (1042,1043)) "
            + "		THEN PROBIN_BYTES-4 "
            + " END AS COLUMN_BYTES, "            
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS LENGTH,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS DECIMAL_DIGITS,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS RADIX,"
            + " CAST(2 AS SMALLINT) AS NULLABLE,"
            + " CAST(''AS VARCHAR(256)) AS REMARKS,"
            + " CAST(NULL AS SMALLINT) AS COLUMN_DEF,"
            + " pg_catalog.format_type(proargtypes[pos], NULL) AS SQL_DATA_TYPE,"
            + " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,"
            + " pg_catalog.format_type(proargtypes[pos], NULL) AS CHAR_OCTET_LENGTH,"
            + " CASE WHEN (proallargtypes is NULL) THEN pos+1"
            + " WHEN pos = array_upper(proallargtypes, 1) THEN 0"
            + " ELSE pos END AS ORDINAL_POSITION,"
            + " CAST('' AS VARCHAR(256)) AS IS_NULLABLE,"
            + " p.prooid AS PROOID,"
            + " CASE WHEN (proallargtypes is NULL) THEN pos+1"
            + " WHEN prokind = 'f' AND pos = array_upper(proallargtypes, 1) THEN 0"
            + " ELSE pos END AS PROARGINDEX, "
            +	" CASE WHEN (proallargtypes IS NULL AND proargtypes[pos] = 1700 AND prokind='p') OR (proallargtypes IS NOT NULL AND proallargtypes[pos] = 1700 AND prokind='p' AND proallargtypes[pos] = 1700) THEN (PROBIN_BYTES-4)/65536 END as NUMERIC_PRECISION, "
            + " CASE WHEN (proallargtypes IS NULL AND proargtypes[pos] = 1700 AND prokind='p') OR (proallargtypes IS NOT NULL AND proallargtypes[pos] = 1700 AND prokind='p' AND proallargtypes[pos] = 1700) THEN (((PROBIN_BYTES::numeric-4)/65536 - (PROBIN_BYTES-4)/65536) *  65536)::INT END as NUMERIC_SCALE, "
            + " p.proname || '_' || p.prooid AS SPECIFIC_NAME "
            + " FROM (pg_catalog.pg_proc_info p LEFT JOIN pg_namespace n"
            + " ON n.oid = p.pronamespace)"
            + " LEFT JOIN (SELECT "
            + " CASE WHEN (proallargtypes IS NULL) "
            + " THEN generate_series(array_lower(proargnames, 1), array_upper(proargnames, 1))-1"
            + " ELSE generate_series(array_lower(proargnames, 1), array_upper(proargnames, 1)+1)-1 "
            + " END AS pos"
            + " FROM pg_catalog.pg_proc_info p ) AS s ON (pos >= 0)");
    
    procedureColQuery.append(" WHERE true ");
    
    procedureColQuery.append(getCatalogFilterCondition(catalog));
    
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	procedureColQuery.append(" AND n.nspname LIKE " + escapeQuotes(schemaPattern));
    }

    if (procedureNamePattern != null && !procedureNamePattern.isEmpty()) {
    	procedureColQuery.append(" AND proname LIKE " + escapeQuotes(procedureNamePattern));
    }
    
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
    	procedureColQuery.append(" AND COLUMN_NAME LIKE " + escapeQuotes(columnNamePattern));
    }
    
    procedureColQuery.append(" ) AS INPUT_PARAM_TABLE"
        + " WHERE ORDINAL_POSITION IS NOT NULL"
        + " ) AS RESULT_SET WHERE (DATA_TYPE != 1111 OR (TYPE_NAME IS NOT NULL AND TYPE_NAME != '-'))"
        + " ORDER BY PROCEDURE_CAT ,PROCEDURE_SCHEM,"
        + " PROCEDURE_NAME, PROOID, PROARGINDEX, COLUMN_TYPE DESC");
    
    sql = procedureColQuery.toString();
    
    ResultSet rs = createMetaDataStatement().executeQuery(sql);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);
    
    return rs;
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                             String[] types) throws SQLException {
    String sql = null;
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern, tableNamePattern, types);
    
    int schemaPatternType = getExtSchemaPatternMatch(schemaPattern);

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logInfo("schemaPatternType = {0}", schemaPatternType);
    
    if (schemaPatternType == LOCAL_SCHEMA_QUERY) {
    	// Join on pg_catalog
    	sql = buildLocalSchemaTablesQuery(catalog, schemaPattern, tableNamePattern, types);    	
    }
    else if (schemaPatternType == NO_SCHEMA_UNIVERSAL_QUERY) {
    	if (isSingleDatabaseMetaData()) {
	    	// svv_tables
	    	sql = buildUniversalSchemaTablesQuery(catalog, schemaPattern, tableNamePattern, types);
    	}
    	else {
	    	// svv_all_tables
	    	sql = buildUniversalAllSchemaTablesQuery(catalog, schemaPattern, tableNamePattern, types);
    	}
    }
    else if (schemaPatternType == EXTERNAL_SCHEMA_QUERY) {
    	// svv_external_tables
    	sql = buildExternalSchemaTablesQuery(catalog, schemaPattern, tableNamePattern, types);
    }
    
    ResultSet rs = createMetaDataStatement().executeQuery(sql);

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);

    return rs;
  }
  
  private String buildLocalSchemaTablesQuery(String catalog,
  												String schemaPattern, 
  												String tableNamePattern,
      										String[] types) throws SQLException {
    String select;
    
    select = "SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, "
             + " CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' "
             + " WHEN true THEN CASE "
             + " WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind "
             + "  WHEN 'r' THEN 'SYSTEM TABLE' "
             + "  WHEN 'v' THEN 'SYSTEM VIEW' "
             + "  WHEN 'i' THEN 'SYSTEM INDEX' "
             + "  ELSE NULL "
             + "  END "
             + " WHEN n.nspname = 'pg_toast' THEN CASE c.relkind "
             + "  WHEN 'r' THEN 'SYSTEM TOAST TABLE' "
             + "  WHEN 'i' THEN 'SYSTEM TOAST INDEX' "
             + "  ELSE NULL "
             + "  END "
             + " ELSE CASE c.relkind "
             + "  WHEN 'r' THEN 'TEMPORARY TABLE' "
             + "  WHEN 'p' THEN 'TEMPORARY TABLE' "
             + "  WHEN 'i' THEN 'TEMPORARY INDEX' "
             + "  WHEN 'S' THEN 'TEMPORARY SEQUENCE' "
             + "  WHEN 'v' THEN 'TEMPORARY VIEW' "
             + "  ELSE NULL "
             + "  END "
             + " END "
             + " WHEN false THEN CASE c.relkind "
             + " WHEN 'r' THEN 'TABLE' "
             + " WHEN 'p' THEN 'PARTITIONED TABLE' "
             + " WHEN 'i' THEN 'INDEX' "
             + " WHEN 'S' THEN 'SEQUENCE' "
             + " WHEN 'v' THEN 'VIEW' "
             + " WHEN 'c' THEN 'TYPE' "
             + " WHEN 'f' THEN 'FOREIGN TABLE' "
             + " WHEN 'm' THEN 'MATERIALIZED VIEW' "
             + " ELSE NULL "
             + " END "
             + " ELSE NULL "
             + " END "
             + " AS TABLE_TYPE, d.description AS REMARKS, "
             + " '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, "
             + "'' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION "
             + " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c "
             + " LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) "
             + " LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class') "
             + " LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog') "
             + " WHERE c.relnamespace = n.oid ";

    String filterClause = getTableFilterClause(catalog, schemaPattern, tableNamePattern, types, LOCAL_SCHEMA_QUERY, true, null);
    
    String orderby = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ";

    return select + filterClause + orderby;
  }
  
  private String getTableFilterClause(String catalog,
																			String schemaPattern, 
																			String tableNamePattern,
																			String[] types,
																			int schemaPatternType,
																			boolean apiSupportedOnlyForConnectedDatabase,
																			String databaseColName) throws SQLException {
  	String filterClause = "";
    String useSchemas = "SCHEMAS";
  	
    filterClause += getCatalogFilterCondition(catalog, apiSupportedOnlyForConnectedDatabase, databaseColName);
    
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	filterClause += " AND TABLE_SCHEM LIKE " + escapeQuotes(schemaPattern);
    }
    
    if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
    	filterClause += " AND TABLE_NAME LIKE " + escapeQuotes(tableNamePattern);
    }
    if (types != null) {
    	
    	if (schemaPatternType == LOCAL_SCHEMA_QUERY) {
	    	filterClause += " AND (false ";
	      StringBuilder orclause = new StringBuilder();
	      for (String type : types) {
	        Map<String, String> clauses = tableTypeClauses.get(type);
	        if (clauses != null) {
	          String clause = clauses.get(useSchemas);
	          orclause.append(" OR ( ").append(clause).append(" ) ");
	        }
	      }
	      filterClause += orclause.toString() + ") ";
    	}
    	else
    	if (schemaPatternType == NO_SCHEMA_UNIVERSAL_QUERY
    			|| schemaPatternType == EXTERNAL_SCHEMA_QUERY) {
    		filterClause += (" AND TABLE_TYPE IN ( ");
    		int len = types.length;
	      for (String type : types) {
	      	filterClause += escapeQuotes(type);
	      	len--;
	      	if(len > 0)
	      		filterClause +=", ";
	      }
	      
	      filterClause += ") ";
    	}
    }	
    
  	if (schemaPatternType == LOCAL_SCHEMA_QUERY) {
	    if (connection.getHideUnprivilegedObjects()) {
	    	filterClause += " AND has_table_privilege(c.oid, "
	        + " 'SELECT, INSERT, UPDATE, DELETE, RULE, REFERENCES, TRIGGER')";
	    }
  	}
    
    
    
    return filterClause;
  }
  
  private String buildUniversalSchemaTablesQuery(String catalog,
			String schemaPattern, 
			String tableNamePattern,
			String[] types) throws SQLException {
    // Basic query, without the join operation and subquery name appended to the end
    StringBuilder tableQuery = new StringBuilder(2048);
    tableQuery.append("SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT,"
            + " table_schema AS TABLE_SCHEM,"
            + " table_name AS TABLE_NAME,"
            + " CAST("
            + " CASE table_type"
            + " WHEN 'BASE TABLE' THEN CASE"
            + " WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM TABLE'"
            + " WHEN table_schema = 'pg_toast' THEN 'SYSTEM TOAST TABLE'"
            + " WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY TABLE'"
            + " ELSE 'TABLE'"
            + " END"
            + " WHEN 'VIEW' THEN CASE"
            + " WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM VIEW'"
            + " WHEN table_schema = 'pg_toast' THEN NULL"
            + " WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY VIEW'"
            + " ELSE 'VIEW'"
            + " END"
            + " WHEN 'EXTERNAL TABLE' THEN 'EXTERNAL TABLE'"
            + " END"
            + " AS VARCHAR(124)) AS TABLE_TYPE,"
            + " REMARKS,"
            + " '' as TYPE_CAT,"
            + " '' as TYPE_SCHEM,"
            + " '' as TYPE_NAME, "
            + " '' AS SELF_REFERENCING_COL_NAME,"
            + " '' AS REF_GENERATION "
            + " FROM svv_tables)");
    
    tableQuery.append( " WHERE true ");
    
    String filterClause = getTableFilterClause(catalog, schemaPattern, tableNamePattern, types, NO_SCHEMA_UNIVERSAL_QUERY, true, null);
    String orderby = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ";
    
    tableQuery.append(filterClause);
    tableQuery.append(orderby);
    
    return tableQuery.toString();
  }

  // Datashare/Cross-db support svv_all_tables view
  private String buildUniversalAllSchemaTablesQuery(String catalog,
			String schemaPattern, 
			String tableNamePattern,
			String[] types) throws SQLException {
    StringBuilder tableQuery = new StringBuilder(2048);
    tableQuery.append("SELECT * FROM (SELECT CAST(DATABASE_NAME AS VARCHAR(124)) AS TABLE_CAT,"
            + " SCHEMA_NAME AS TABLE_SCHEM,"
            + " TABLE_NAME  AS TABLE_NAME,"
            + " CAST("
            + " CASE "
            + " WHEN SCHEMA_NAME='information_schema' "
            + "    AND TABLE_TYPE='TABLE' THEN 'SYSTEM TABLE' "
            + " WHEN SCHEMA_NAME='information_schema' "
            + "    AND TABLE_TYPE='VIEW' THEN 'SYSTEM VIEW' "
            + " ELSE TABLE_TYPE "
            + " END "             
            + " AS VARCHAR(124)) AS TABLE_TYPE,"
            + " REMARKS,"
            + " '' as TYPE_CAT,"
            + " '' as TYPE_SCHEM,"
            + " '' as TYPE_NAME, "
            + " '' AS SELF_REFERENCING_COL_NAME,"
            + " '' AS REF_GENERATION "
            + " FROM PG_CATALOG.SVV_ALL_TABLES)");
    
    tableQuery.append( " WHERE true ");
    
    String filterClause = getTableFilterClause(catalog, schemaPattern, tableNamePattern, types, NO_SCHEMA_UNIVERSAL_QUERY, false, "TABLE_CAT");
    String orderby = " ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME ";
    
    tableQuery.append(filterClause);
    tableQuery.append(orderby);
    
    return tableQuery.toString();
  }
  
  private String buildExternalSchemaTablesQuery(String catalog,
													String schemaPattern, 
													String tableNamePattern,
													String[] types) throws SQLException {
    // Basic query, without the join operation and subquery name appended to the end
    StringBuilder tableQuery = new StringBuilder(2048);
    tableQuery
        .append("SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT,"
            + " schemaname AS table_schem,"
            + " tablename AS TABLE_NAME,"
            + " 'EXTERNAL TABLE' AS TABLE_TYPE,"
            + " NULL AS REMARKS,"
            + " '' as TYPE_CAT,"
            + " '' as TYPE_SCHEM,"
            + " '' as TYPE_NAME, "
            + " '' AS SELF_REFERENCING_COL_NAME,"
            + " '' AS REF_GENERATION "
            + " FROM svv_external_tables)");

    tableQuery.append( " WHERE true ");
    
    String filterClause = getTableFilterClause(catalog, schemaPattern, tableNamePattern, types, EXTERNAL_SCHEMA_QUERY, true, null);
    String orderby = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ";
    
    tableQuery.append(filterClause);
    tableQuery.append(orderby);
    
    return tableQuery.toString();
  }
  
  private static final Map<String, Map<String, String>> tableTypeClauses;

  static {
    tableTypeClauses = new HashMap<String, Map<String, String>>();
    Map<String, String> ht = new HashMap<String, String>();
    tableTypeClauses.put("TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname !~ '^pg_'");
    
/*  ht = new HashMap<String, String>();
    tableTypeClauses.put("PARTITIONED TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'p' AND c.relname !~ '^pg_'");
*/  
    ht = new HashMap<String, String>();
    tableTypeClauses.put("VIEW", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname !~ '^pg_'");
    
  ht = new HashMap<String, String>();
    tableTypeClauses.put("INDEX", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname !~ '^pg_'");

    ht = new HashMap<String, String>();
    tableTypeClauses.put("SEQUENCE", ht);
    ht.put("SCHEMAS", "c.relkind = 'S'");
    ht.put("NOSCHEMAS", "c.relkind = 'S'");
  
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TYPE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    ht.put("NOSCHEMAS", "c.relkind = 'c' AND c.relname !~ '^pg_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM TABLE", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')");
    ht.put("NOSCHEMAS",
        "c.relkind = 'r' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM TOAST TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'r' AND n.nspname = 'pg_toast'");
    ht.put("NOSCHEMAS", "c.relkind = 'r' AND c.relname ~ '^pg_toast_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM TOAST INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname = 'pg_toast'");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname ~ '^pg_toast_'");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM VIEW", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname ~ '^pg_'");

    
    ht = new HashMap<String, String>();
    tableTypeClauses.put("SYSTEM INDEX", ht);
    ht.put("SCHEMAS",
        "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    ht.put("NOSCHEMAS",
        "c.relkind = 'v' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'");
        
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY TABLE", ht);
    ht.put("SCHEMAS", "c.relkind IN ('r','p') AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind IN ('r','p') AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY INDEX", ht);
    ht.put("SCHEMAS", "c.relkind = 'i' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'i' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY VIEW", ht);
    ht.put("SCHEMAS", "c.relkind = 'v' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'v' AND c.relname ~ '^pg_temp_' ");
    ht = new HashMap<String, String>();
    tableTypeClauses.put("TEMPORARY SEQUENCE", ht);
    ht.put("SCHEMAS", "c.relkind = 'S' AND n.nspname ~ '^pg_temp_' ");
    ht.put("NOSCHEMAS", "c.relkind = 'S' AND c.relname ~ '^pg_temp_' ");

/*    
    ht = new HashMap<String, String>();
    tableTypeClauses.put("FOREIGN TABLE", ht);
    ht.put("SCHEMAS", "c.relkind = 'f'");
    ht.put("NOSCHEMAS", "c.relkind = 'f'");
    
    ht = new HashMap<String, String>();
    tableTypeClauses.put("MATERIALIZED VIEW", ht);
    ht.put("SCHEMAS", "c.relkind = 'm'");
    ht.put("NOSCHEMAS", "c.relkind = 'm'");
*/    
    tableTypeClauses.put("EXTERNAL TABLE", null);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    String sql;
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern);
    
	  if (isSingleDatabaseMetaData()) {
	    sql = "SELECT nspname AS TABLE_SCHEM, current_database() AS TABLE_CATALOG FROM pg_catalog.pg_namespace "
	          + " WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_' "
	          + " OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_' "
	          + " OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) ";
	    
	    sql += getCatalogFilterCondition(catalog);
	    
	    if (schemaPattern != null && !schemaPattern.isEmpty()) {
	      sql += " AND nspname LIKE " + escapeQuotes(schemaPattern);
	    }
	    if (connection.getHideUnprivilegedObjects()) {
	      sql += " AND has_schema_privilege(nspname, 'USAGE, CREATE')";
	    }
	    
	    sql += " ORDER BY TABLE_SCHEM";
	  }
	  else {
	  	sql = "SELECT CAST(schema_name AS varchar(124)) AS TABLE_SCHEM, " 
	  				+ " CAST(database_name AS varchar(124)) AS TABLE_CATALOG " 
	  				+ " FROM PG_CATALOG.SVV_ALL_SCHEMAS " 
	  				+ " WHERE TRUE ";
	  	
	    sql += getCatalogFilterCondition(catalog, false, null);
	    
	    if (schemaPattern != null && !schemaPattern.isEmpty()) {
	      sql += " AND schema_name LIKE " + escapeQuotes(schemaPattern);
	    }

	    sql += " ORDER BY TABLE_CATALOG, TABLE_SCHEM";
	  }

    ResultSet rs = createMetaDataStatement().executeQuery(sql);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);
    
    return rs;
  }

  /**
   * Redshift does not support multiple catalogs from a single connection, so to reduce confusion
   * we only return the current catalog. {@inheritDoc}
   */
  @Override
  public ResultSet getCatalogs() throws SQLException {
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true);
  	
  	String sql;
  	
  	if (isSingleDatabaseMetaData()) {
  		// Behavious same as before i.e. returns only single database.
  		Field[] f = new Field[1];
      List<Tuple> v = new ArrayList<Tuple>();
      f[0] = new Field("TABLE_CAT", Oid.VARCHAR);
      byte[][] tuple = new byte[1][];
      tuple[0] = connection.encodeString(connection.getCatalog());
      v.add(new Tuple(tuple));

      return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  	}
  	else {
  		// Datasharing/federation support enable, so get databases using the new view.
  		sql = "SELECT CAST(database_name AS varchar(124)) AS TABLE_CAT FROM PG_CATALOG.SVV_REDSHIFT_DATABASES ";
  	}
	  	
    sql += " ORDER BY TABLE_CAT";

    ResultSet rs = createMetaDataStatement().executeQuery(sql);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);
    
    return rs;
  	
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    String[] types = tableTypeClauses.keySet().toArray(new String[0]);
    Arrays.sort(types);

    Field[] f = new Field[1];
    List<Tuple> v = new ArrayList<Tuple>();
    f[0] = new Field("TABLE_TYPE", Oid.VARCHAR);
    for (String type : types) {
      byte[][] tuple = new byte[1][];
      tuple[0] = connection.encodeString(type);
      v.add(new Tuple(tuple));
    }

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                              String columnNamePattern) throws SQLException 
  {
    String sql = null;

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    
    int schemaPatternType = getExtSchemaPatternMatch(schemaPattern);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logInfo("schemaPatternType = {0}", schemaPatternType);
    
    if (schemaPatternType == LOCAL_SCHEMA_QUERY) {
    	// Join on pg_catalog union with pg_late_binding_view
    	sql = buildLocalSchemaColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);    	
    }
    else if (schemaPatternType == NO_SCHEMA_UNIVERSAL_QUERY) {
    	if (isSingleDatabaseMetaData()) {
	    	// svv_columns
	    	sql = buildUniversalSchemaColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    	}
    	else {
	    	// svv_all_columns
	    	sql = buildUniversalAllSchemaColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    	}
    }
    else if (schemaPatternType == EXTERNAL_SCHEMA_QUERY) {
    	// svv_external_columns
    	sql = buildExternalSchemaColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }
    
    ResultSet rs = createMetaDataStatement().executeQuery(sql);

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);
    
    return rs;
  }
  
  private String buildLocalSchemaColumnsQuery(String catalog,
															String schemaPattern, 
															String tableNamePattern,
															String columnNamePattern) throws SQLException {

     	StringBuilder result = new StringBuilder();

      result.append("SELECT * FROM ( ");
      result.append("SELECT current_database() AS TABLE_CAT, ");
      result.append("n.nspname AS TABLE_SCHEM, ");
      result.append("c.relname as TABLE_NAME , ");
      result.append("a.attname as COLUMN_NAME, " );
      result.append("CAST(case typname ");
      result.append("when 'text' THEN 12 ");
      result.append("when 'bit' THEN -7 ");
      result.append("when 'bool' THEN -7 ");
      result.append("when 'boolean' THEN -7 ");
      result.append("when 'varchar' THEN 12 ");
      result.append("when 'character varying' THEN 12 ");
      result.append("when 'char' THEN 1 ");
      result.append("when '\"char\"' THEN 1 ");
      result.append("when 'character' THEN 1 ");
      result.append("when 'nchar' THEN 12 ");
      result.append("when 'bpchar' THEN 1 ");
      result.append("when 'nvarchar' THEN 12 ");
      result.append("when 'date' THEN 91 ");
      result.append("when 'time' THEN 92 ");
      result.append("when 'time without time zone' THEN 92 ");
      result.append("when 'timetz' THEN 2013 ");
      result.append("when 'time with time zone' THEN 2013 ");
      result.append("when 'timestamp' THEN 93 ");
      result.append("when 'timestamp without time zone' THEN 93 ");
      result.append("when 'timestamptz' THEN 2014 ");
      result.append("when 'timestamp with time zone' THEN 2014 ");
      result.append("when 'smallint' THEN 5 ");
      result.append("when 'int2' THEN 5 ");
      result.append("when 'integer' THEN 4 ");
      result.append("when 'int' THEN 4 ");
      result.append("when 'int4' THEN 4 ");
      result.append("when 'bigint' THEN -5 ");
      result.append("when 'int8' THEN -5 ");
      result.append("when 'decimal' THEN 3 ");
      result.append("when 'real' THEN 7 ");
      result.append("when 'float4' THEN 7 ");
      result.append("when 'double precision' THEN 8 ");
      result.append("when 'float8' THEN 8 ");
      result.append("when 'float' THEN 6 ");
      result.append("when 'numeric' THEN 2 ");
      result.append("when '_float4' THEN 2003 ");
      result.append("when '_aclitem' THEN 2003 ");
      result.append("when '_text' THEN 2003 ");
      result.append("when 'bytea' THEN -2 ");
      result.append("when 'oid' THEN -5 ");
      result.append("when 'name' THEN 12 ");
      result.append("when '_int4' THEN 2003 ");
      result.append("when '_int2' THEN 2003 ");
      result.append("when 'ARRAY' THEN 2003 ");
      result.append("when 'geometry' THEN -4 ");
      result.append("when 'super' THEN -16 ");
      result.append("when 'varbyte' THEN -4 ");
      result.append("when 'geography' THEN -4 ");
      result.append("else 1111 END as SMALLINT) AS DATA_TYPE, ");
      result.append("t.typname as TYPE_NAME, ");
      result.append("case typname ");
      result.append("when 'int4' THEN 10 ");
      result.append("when 'bit' THEN 1 ");
      result.append("when 'bool' THEN 1 ");
      result.append("when 'varchar' THEN atttypmod -4 ");
      result.append("when 'character varying' THEN atttypmod -4 ");
      result.append("when 'char' THEN atttypmod -4 ");
      result.append("when 'character' THEN atttypmod -4 ");
      result.append("when 'nchar' THEN atttypmod -4 ");
      result.append("when 'bpchar' THEN atttypmod -4 ");
      result.append("when 'nvarchar' THEN atttypmod -4 ");
      result.append("when 'date' THEN 13 ");
      result.append("when 'time' THEN 15 ");
      result.append("when 'time without time zone' THEN 15 ");
      result.append("when 'timetz' THEN 21 ");
      result.append("when 'time with time zone' THEN 21 ");
      result.append("when 'timestamp' THEN 29 ");
      result.append("when 'timestamp without time zone' THEN 29 ");
      result.append("when 'timestamptz' THEN 35 ");
      result.append("when 'timestamp with time zone' THEN 35 ");
      result.append("when 'smallint' THEN 5 ");
      result.append("when 'int2' THEN 5 ");
      result.append("when 'integer' THEN 10 ");
      result.append("when 'int' THEN 10 ");
      result.append("when 'int4' THEN 10 ");
      result.append("when 'bigint' THEN 19 ");
      result.append("when 'int8' THEN 19 ");
      result.append("when 'decimal' then (atttypmod - 4) >> 16 ");
      result.append("when 'real' THEN 8 ");
      result.append("when 'float4' THEN 8 ");
      result.append("when 'double precision' THEN 17 ");
      result.append("when 'float8' THEN 17 ");
      result.append("when 'float' THEN 17 ");
      result.append("when 'numeric' THEN (atttypmod - 4) >> 16 ");
      result.append("when '_float4' THEN 8 ");
      result.append("when 'oid' THEN 10 ");
      result.append("when '_int4' THEN 10 ");
      result.append("when '_int2' THEN 5 ");
      result.append("when 'geometry' THEN NULL ");
      result.append("when 'super' THEN NULL ");
      result.append("when 'varbyte' THEN NULL ");
      result.append("when 'geography' THEN NULL ");
//      if (connSettings.m_unknownLength == null)
      {
          result.append("else 2147483647 end as COLUMN_SIZE , ");
      }
/*      else
      {
          result.append("else ");
          result.append(connSettings.m_unknownLength);
          result.append(" end as COLUMN_SIZE , ");
      } */
      result.append("null as BUFFER_LENGTH , ");
      result.append("case typname ");
      result.append("when 'float4' then 8 ");
      result.append("when 'float8' then 17 ");
      result.append("when 'numeric' then (atttypmod - 4) & 65535 ");
      result.append("when 'time without time zone' then 6 ");
      result.append("when 'timetz' then 6 ");
      result.append("when 'time with time zone' then 6 ");
      result.append("when 'timestamp without time zone' then 6 ");
      result.append("when 'timestamp' then 6 ");
      result.append("when 'geometry' then NULL ");
      result.append("when 'super' then NULL ");
      result.append("when 'varbyte' then NULL ");
      result.append("when 'geography' then NULL ");
      result.append("else 0 end as DECIMAL_DIGITS, ");
      result.append("case typname ");
      result.append("when 'varbyte' then 2 ");
      result.append("when 'geography' then 2 ");
      result.append("else 10 end as NUM_PREC_RADIX, ");
      result.append("case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) ");
      result.append("when 'false' then 1 ");
      result.append("when NULL then 2 ");
      result.append("else 0 end AS NULLABLE , ");
      result.append("dsc.description as REMARKS , ");
      result.append("pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS COLUMN_DEF, ");
      result.append("CAST(case typname ");
      result.append("when 'text' THEN 12 ");
      result.append("when 'bit' THEN -7 ");
      result.append("when 'bool' THEN -7 ");
      result.append("when 'boolean' THEN -7 ");
      result.append("when 'varchar' THEN 12 ");
      result.append("when 'character varying' THEN 12 ");
      result.append("when '\"char\"' THEN 1 ");
      result.append("when 'char' THEN 1 ");
      result.append("when 'character' THEN 1 ");
      result.append("when 'nchar' THEN 1 ");
      result.append("when 'bpchar' THEN 1 ");
      result.append("when 'nvarchar' THEN 12 ");
      result.append("when 'date' THEN 91 ");
      result.append("when 'time' THEN 92 ");
      result.append("when 'time without time zone' THEN 92 ");
      result.append("when 'timetz' THEN 2013 ");
      result.append("when 'time with time zone' THEN 2013 ");
      result.append("when 'timestamp with time zone' THEN 2014 ");
      result.append("when 'timestamp' THEN 93 ");
      result.append("when 'timestamp without time zone' THEN 93 ");
      result.append("when 'smallint' THEN 5 ");
      result.append("when 'int2' THEN 5 ");
      result.append("when 'integer' THEN 4 ");
      result.append("when 'int' THEN 4 ");
      result.append("when 'int4' THEN 4 ");
      result.append("when 'bigint' THEN -5 ");
      result.append("when 'int8' THEN -5 ");
      result.append("when 'decimal' THEN 3 ");
      result.append("when 'real' THEN 7 ");
      result.append("when 'float4' THEN 7 ");
      result.append("when 'double precision' THEN 8 ");
      result.append("when 'float8' THEN 8 ");
      result.append("when 'float' THEN 6 ");
      result.append("when 'numeric' THEN 2 ");
      result.append("when '_float4' THEN 2003 ");
      result.append("when 'timestamptz' THEN 2014 ");
      result.append("when 'timestamp with time zone' THEN 2014 ");
      result.append("when '_aclitem' THEN 2003 ");
      result.append("when '_text' THEN 2003 ");
      result.append("when 'bytea' THEN -2 ");
      result.append("when 'oid' THEN -5 ");
      result.append("when 'name' THEN 12 ");
      result.append("when '_int4' THEN 2003 ");
      result.append("when '_int2' THEN 2003 ");
      result.append("when 'ARRAY' THEN 2003 ");
      result.append("when 'geometry' THEN -4 ");
      result.append("when 'super' THEN -16 ");
      result.append("when 'varbyte' THEN -4 ");
      result.append("when 'geography' THEN -4 ");
      result.append("else 1111 END as SMALLINT) AS SQL_DATA_TYPE, ");
      result.append("CAST(NULL AS SMALLINT) as SQL_DATETIME_SUB , ");
      result.append("case typname ");
      result.append("when 'int4' THEN 10 ");
      result.append("when 'bit' THEN 1 ");
      result.append("when 'bool' THEN 1 ");
      result.append("when 'varchar' THEN atttypmod -4 ");
      result.append("when 'character varying' THEN atttypmod -4 ");
      result.append("when 'char' THEN atttypmod -4 ");
      result.append("when 'character' THEN atttypmod -4 ");
      result.append("when 'nchar' THEN atttypmod -4 ");
      result.append("when 'bpchar' THEN atttypmod -4 ");
      result.append("when 'nvarchar' THEN atttypmod -4 ");
      result.append("when 'date' THEN 13 ");
      result.append("when 'time' THEN 15 ");
      result.append("when 'time without time zone' THEN 15 ");
      result.append("when 'timetz' THEN 21 ");
      result.append("when 'time with time zone' THEN 21 ");
      result.append("when 'timestamp' THEN 29 ");
      result.append("when 'timestamp without time zone' THEN 29 ");
      result.append("when 'timestamptz' THEN 35 ");
      result.append("when 'timestamp with time zone' THEN 35 ");
      result.append("when 'smallint' THEN 5 ");
      result.append("when 'int2' THEN 5 ");
      result.append("when 'integer' THEN 10 ");
      result.append("when 'int' THEN 10 ");
      result.append("when 'int4' THEN 10 ");
      result.append("when 'bigint' THEN 19 ");
      result.append("when 'int8' THEN 19 ");
      result.append("when 'decimal' then ((atttypmod - 4) >> 16) & 65535 ");
      result.append("when 'real' THEN 8 ");
      result.append("when 'float4' THEN 8 ");
      result.append("when 'double precision' THEN 17 ");
      result.append("when 'float8' THEN 17 ");
      result.append("when 'float' THEN 17 ");
      result.append("when 'numeric' THEN ((atttypmod - 4) >> 16) & 65535 ");
      result.append("when '_float4' THEN 8 ");
      result.append("when 'oid' THEN 10 ");
      result.append("when '_int4' THEN 10 ");
      result.append("when '_int2' THEN 5 ");
      result.append("when 'geometry' THEN NULL ");
      result.append("when 'super' THEN NULL ");
      result.append("when 'varbyte' THEN NULL ");
      result.append("when 'geography' THEN NULL ");
//      if (connSettings.m_unknownLength == null)
      {
          result.append("else 2147483647 end as CHAR_OCTET_LENGTH , ");
      }
/*      else
      {
          result.append("else ");
          result.append(connSettings.m_unknownLength);
          result.append(" end as CHAR_OCTET_LENGTH , ");
      } */
      result.append("a.attnum AS ORDINAL_POSITION, ");
      result.append("case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) ");
      result.append("when 'false' then 'YES' ");
      result.append("when NULL then '' ");
      result.append("else 'NO' end AS IS_NULLABLE, ");
      result.append("null as SCOPE_CATALOG , ");
      result.append("null as SCOPE_SCHEMA , ");
      result.append("null as SCOPE_TABLE, ");
      result.append("t.typbasetype AS SOURCE_DATA_TYPE , ");
      result.append("CASE WHEN left(pg_catalog.pg_get_expr(def.adbin, def.adrelid), 16) = 'default_identity' THEN 'YES' ");
      result.append("ELSE 'NO' END AS IS_AUTOINCREMENT, ");
      result.append("IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN ");
      result.append("FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) ");
      result.append("JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) ");
      result.append("JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) ");
      result.append("LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) ");
      result.append("LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) ");
      result.append("LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') ");
      result.append("LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') ");
      result.append("WHERE a.attnum > 0 AND NOT a.attisdropped    ");

      result.append(getCatalogFilterCondition(catalog));
      
      if (schemaPattern != null && !schemaPattern.isEmpty()) {
      	result.append(" AND n.nspname LIKE " + escapeQuotes(schemaPattern));
      }
      if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
      	result.append(" AND c.relname LIKE " + escapeQuotes(tableNamePattern));
      }
  /*    if (connection.haveMinimumServerVersion(ServerVersion.v8_4)) {
        sql += ") c WHERE true ";
      } */
      if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
      	result.append(" AND attname LIKE " + escapeQuotes(columnNamePattern));
      }

      result.append(" ORDER BY TABLE_SCHEM,c.relname,attnum ) ");
      

      // This part uses redshift method PG_GET_LATE_BINDING_VIEW_COLS() to
      // get the column list for late binding view.
      result.append(" UNION ALL ");
      result.append("SELECT current_database()::VARCHAR(128) AS TABLE_CAT, ");
      result.append("schemaname::varchar(128) AS table_schem, ");
      result.append("tablename::varchar(128) AS table_name, ");
      result.append("columnname::varchar(128) AS column_name, ");
      result.append("CAST(CASE columntype_rep ");
      result.append("WHEN 'text' THEN 12 ");
      result.append("WHEN 'bit' THEN -7 ");
      result.append("WHEN 'bool' THEN -7 ");
      result.append("WHEN 'boolean' THEN -7 ");
      result.append("WHEN 'varchar' THEN 12 ");
      result.append("WHEN 'character varying' THEN 12 ");
      result.append("WHEN 'char' THEN 1 ");
      result.append("WHEN 'character' THEN 1 ");
      result.append("WHEN 'nchar' THEN 1 ");
      result.append("WHEN 'bpchar' THEN 1 ");
      result.append("WHEN 'nvarchar' THEN 12 ");
      result.append("WHEN '\"char\"' THEN 1 ");
      result.append("WHEN 'date' THEN 91 ");
      result.append("when 'time' THEN 92 ");
      result.append("when 'time without time zone' THEN 92 ");
      result.append("when 'timetz' THEN 2013 ");
      result.append("when 'time with time zone' THEN 2013 ");
      result.append("WHEN 'timestamp' THEN 93 ");
      result.append("WHEN 'timestamp without time zone' THEN 93 ");
      result.append("when 'timestamptz' THEN 2014 ");
      result.append("WHEN 'timestamp with time zone' THEN 2014 ");
      result.append("WHEN 'smallint' THEN 5 ");
      result.append("WHEN 'int2' THEN 5 ");
      result.append("WHEN 'integer' THEN 4 ");
      result.append("WHEN 'int' THEN 4 ");
      result.append("WHEN 'int4' THEN 4 ");
      result.append("WHEN 'bigint' THEN -5 ");
      result.append("WHEN 'int8' THEN -5 ");
      result.append("WHEN 'decimal' THEN 3 ");
      result.append("WHEN 'real' THEN 7 ");
      result.append("WHEN 'float4' THEN 7 ");
      result.append("WHEN 'double precision' THEN 8 ");
      result.append("WHEN 'float8' THEN 8 ");
      result.append("WHEN 'float' THEN 6 ");
      result.append("WHEN 'numeric' THEN 2 ");
      result.append("WHEN 'timestamptz' THEN 2014 ");
      result.append("WHEN 'bytea' THEN -2 ");
      result.append("WHEN 'oid' THEN -5 ");
      result.append("WHEN 'name' THEN 12 ");
      result.append("WHEN 'ARRAY' THEN 2003 ");
      result.append("WHEN 'geometry' THEN -4 ");
      result.append("WHEN 'super' THEN -16 ");
      result.append("WHEN 'varbyte' THEN -4 ");
      result.append("WHEN 'geography' THEN -4 ");
      result.append("ELSE 1111 END AS SMALLINT) AS DATA_TYPE, ");
      result.append("COALESCE(NULL,CASE columntype WHEN 'boolean' THEN 'bool' ");
      result.append("WHEN 'character varying' THEN 'varchar' ");
      result.append("WHEN '\"char\"' THEN 'char' ");
      result.append("WHEN 'smallint' THEN 'int2' ");
      result.append("WHEN 'integer' THEN 'int4'");
      result.append("WHEN 'bigint' THEN 'int8' ");
      result.append("WHEN 'real' THEN 'float4' ");
      result.append("WHEN 'double precision' THEN 'float8' ");
      result.append("WHEN 'time without time zone' THEN 'time' ");
      result.append("WHEN 'time with time zone' THEN 'timetz' ");
      result.append("WHEN 'timestamp without time zone' THEN 'timestamp' ");
      result.append("WHEN 'timestamp with time zone' THEN 'timestamptz' ");
      result.append("ELSE columntype END) AS TYPE_NAME,  ");
      result.append("CASE columntype_rep ");
      result.append("WHEN 'int4' THEN 10  ");
      result.append("WHEN 'bit' THEN 1    ");
      result.append("WHEN 'bool' THEN 1");
      result.append("WHEN 'boolean' THEN 1");
      result.append("WHEN 'varchar' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN 'character varying' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN 'char' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',4),''),'0')::INTEGER ");
      result.append("WHEN 'character' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',4),''),'0')::INTEGER ");
      result.append("WHEN 'nchar' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN 'bpchar' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN 'nvarchar' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN 'date' THEN 13 ");
      result.append("WHEN 'time' THEN 15 ");
      result.append("WHEN 'time without time zone' THEN 15 ");
      result.append("WHEN 'timetz' THEN 21 ");
      result.append("WHEN 'timestamp' THEN 29 ");
      result.append("WHEN 'timestamp without time zone' THEN 29 ");
      result.append("WHEN 'time with time zone' THEN 21 ");
      result.append("WHEN 'timestamptz' THEN 35 ");
      result.append("WHEN 'timestamp with time zone' THEN 35 ");
      result.append("WHEN 'smallint' THEN 5 ");
      result.append("WHEN 'int2' THEN 5 ");
      result.append("WHEN 'integer' THEN 10 ");
      result.append("WHEN 'int' THEN 10 ");
      result.append("WHEN 'int4' THEN 10 ");
      result.append("WHEN 'bigint' THEN 19 ");
      result.append("WHEN 'int8' THEN 19 ");
      result.append("WHEN 'decimal' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN 'real' THEN 8 ");
      result.append("WHEN 'float4' THEN 8 ");
      result.append("WHEN 'double precision' THEN 17 ");
      result.append("WHEN 'float8' THEN 17 ");
      result.append("WHEN 'float' THEN 17");
      result.append("WHEN 'numeric' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN '_float4' THEN 8 ");
      result.append("WHEN 'oid' THEN 10 ");
      result.append("WHEN '_int4' THEN 10 ");
      result.append("WHEN '_int2' THEN 5 ");
      result.append("WHEN 'geometry' THEN NULL ");
      result.append("WHEN 'super' THEN NULL ");
      result.append("WHEN 'varbyte' THEN NULL ");
      result.append("WHEN 'geography' THEN NULL ");
      result.append("ELSE 2147483647 END AS COLUMN_SIZE, ");
      result.append("NULL AS BUFFER_LENGTH, ");
      result.append("CASE REGEXP_REPLACE(columntype,'[()0-9,]') ");
      result.append("WHEN 'real' THEN 8 ");
      result.append("WHEN 'float4' THEN 8 ");
      result.append("WHEN 'double precision' THEN 17 ");
      result.append("WHEN 'float8' THEN 17 ");
      result.append("WHEN 'timestamp' THEN 6 ");
      result.append("WHEN 'timestamp without time zone' THEN 6 ");
      result.append("WHEN 'geometry' THEN NULL ");
      result.append("WHEN 'super' THEN NULL ");
      result.append("WHEN 'numeric' THEN regexp_substr (columntype,'[0-9]+',charindex (',',columntype))::INTEGER ");      
      result.append("WHEN 'varbyte' THEN NULL ");
      result.append("WHEN 'geography' THEN NULL ");
      result.append("ELSE 0 END AS DECIMAL_DIGITS, ");
      result.append("CASE columntype ");
      result.append("WHEN 'varbyte' THEN 2 ");
      result.append("WHEN 'geography' THEN 2 ");
      result.append("ELSE 10 END AS NUM_PREC_RADIX, ");
      result.append("NULL AS NULLABLE,  NULL AS REMARKS,   NULL AS COLUMN_DEF, ");
      result.append("CAST(CASE columntype_rep ");
      result.append("WHEN 'text' THEN 12 ");
      result.append("WHEN 'bit' THEN -7 ");
      result.append("WHEN 'bool' THEN -7 ");
      result.append("WHEN 'boolean' THEN -7 ");
      result.append("WHEN 'varchar' THEN 12 ");
      result.append("WHEN 'character varying' THEN 12 ");
      result.append("WHEN 'char' THEN 1 ");
      result.append("WHEN 'character' THEN 1 ");
      result.append("WHEN 'nchar' THEN 12 ");
      result.append("WHEN 'bpchar' THEN 1 ");
      result.append("WHEN 'nvarchar' THEN 12 ");
      result.append("WHEN '\"char\"' THEN 1 ");
      result.append("WHEN 'date' THEN 91 ");
      result.append("WHEN 'time' THEN 92 ");
      result.append("WHEN 'time without time zone' THEN 92 ");
      result.append("WHEN 'timetz' THEN 2013 ");
      result.append("WHEN 'time with time zone' THEN 2013 ");
      result.append("WHEN 'timestamp' THEN 93 ");
      result.append("WHEN 'timestamp without time zone' THEN 93 ");
      result.append("WHEN 'timestamptz' THEN 2014 ");
      result.append("WHEN 'timestamp with time zone' THEN 2014 ");
      result.append("WHEN 'smallint' THEN 5 ");
      result.append("WHEN 'int2' THEN 5 ");
      result.append("WHEN 'integer' THEN 4 ");
      result.append("WHEN 'int' THEN 4 ");
      result.append("WHEN 'int4' THEN 4 ");
      result.append("WHEN 'bigint' THEN -5 ");
      result.append("WHEN 'int8' THEN -5 ");
      result.append("WHEN 'decimal' THEN 3 ");
      result.append("WHEN 'real' THEN 7 ");
      result.append("WHEN 'float4' THEN 7 ");
      result.append("WHEN 'double precision' THEN 8 ");
      result.append("WHEN 'float8' THEN 8 ");
      result.append("WHEN 'float' THEN 6 ");
      result.append("WHEN 'numeric' THEN 2 ");
      result.append("WHEN 'bytea' THEN -2 ");
      result.append("WHEN 'oid' THEN -5 ");
      result.append("WHEN 'name' THEN 12 ");
      result.append("WHEN 'ARRAY' THEN 2003 ");
      result.append("WHEN 'geometry' THEN -4 ");
      result.append("WHEN 'super' THEN -16 ");
      result.append("WHEN 'varbyte' THEN -4 ");
      result.append("WHEN 'geography' THEN -4 ");
      result.append("ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE, ");
      result.append("CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, CASE ");
      result.append("WHEN LEFT (columntype,7) = 'varchar' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',7),''),'0')::INTEGER ");
      result.append("WHEN LEFT (columntype,4) = 'char' THEN isnull(nullif(regexp_substr (columntype,'[0-9]+',4),''),'0')::INTEGER ");
      result.append("WHEN columntype = 'string' THEN 16383  ELSE NULL ");
      result.append("END AS CHAR_OCTET_LENGTH, columnnum AS ORDINAL_POSITION, ");
      result.append("NULL AS IS_NULLABLE,  NULL AS SCOPE_CATALOG,  NULL AS SCOPE_SCHEMA, ");
      result.append("NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, 'NO' AS IS_AUTOINCREMENT, ");
      result.append("'NO' as IS_GENERATEDCOLUMN ");
      result.append("FROM (select lbv_cols.schemaname, ");
      result.append("lbv_cols.tablename, lbv_cols.columnname,");
      result.append("REGEXP_REPLACE(REGEXP_REPLACE(lbv_cols.columntype,'\\\\(.*\\\\)'),'^_.+','ARRAY') as columntype_rep,");
      result.append("columntype, ");
      result.append("lbv_cols.columnnum ");
      result.append("from pg_get_late_binding_view_cols() lbv_cols( ");
      result.append("schemaname name, tablename name, columnname name, ");
      result.append("columntype text, columnnum int)) lbv_columns  ");
      result.append(" WHERE true ");

      // Apply the filters to the column list for late binding view.
      
      result.append(getCatalogFilterCondition(catalog));
      
      if (schemaPattern != null && !schemaPattern.isEmpty()) {
      	result.append(" AND schemaname LIKE " + escapeQuotes(schemaPattern));
      }
      if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
      	result.append(" AND tablename LIKE " + escapeQuotes(tableNamePattern));
      }
  /*    if (connection.haveMinimumServerVersion(ServerVersion.v8_4)) {
        sql += ") c WHERE true ";
      } */
      if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
      	result.append(" AND columnname LIKE " + escapeQuotes(columnNamePattern));
      }

      return result.toString();
  }
  
  private String buildUniversalAllSchemaColumnsQuery(String catalog,
												String schemaPattern, 
												String tableNamePattern,
												String columnNamePattern) throws SQLException {
  	final String unknownColumnSize = "2147483647";
    
	  StringBuilder result = new StringBuilder(8192);
	  
	  result.append("SELECT database_name AS TABLE_CAT, "
    + " schema_name AS TABLE_SCHEM, "
    + " table_name, "
    + " COLUMN_NAME, "
    + " CAST(CASE regexp_replace(data_type, '^_.', 'ARRAY') "
    + " WHEN 'text' THEN 12 "
    + " WHEN 'bit' THEN -7 "
    + " WHEN 'bool' THEN -7 "
    + " WHEN 'boolean' THEN -7 "
    + " WHEN 'varchar' THEN 12 "
    + " WHEN 'character varying' THEN 12 "
    + " WHEN 'char' THEN 1 "
    + " WHEN 'character' THEN 1 "
    + " WHEN 'nchar' THEN 1 "
    + " WHEN 'bpchar' THEN 1 "
    + " WHEN 'nvarchar' THEN 12 "
    + " WHEN '\"char\"' THEN 1 "
    + " WHEN 'date' THEN 91 "
    + " WHEN 'time' THEN 92 "
    + " WHEN 'time without time zone' THEN 92 "
    + " WHEN 'timetz' THEN 2013 "
    + " WHEN 'time with time zone' THEN 2013 "
    + " WHEN 'timestamp' THEN 93 "
    + " WHEN 'timestamp without time zone' THEN 93 "
    + " WHEN 'timestamptz' THEN 2014 "
    + " WHEN 'timestamp with time zone' THEN 2014 "
    + " WHEN 'smallint' THEN 5 "
    + " WHEN 'int2' THEN 5 "
    + " WHEN 'integer' THEN 4 "
    + " WHEN 'int' THEN 4 "
    + " WHEN 'int4' THEN 4 "
    + " WHEN 'bigint' THEN -5 "
    + " WHEN 'int8' THEN -5 "
    + " WHEN 'decimal' THEN 3 "
    + " WHEN 'real' THEN 7 "
    + " WHEN 'float4' THEN 7 "
    + " WHEN 'double precision' THEN 8 "
    + " WHEN 'float8' THEN 8 "
    + " WHEN 'float' THEN 6 "
    + " WHEN 'numeric' THEN 2 "
    + " WHEN 'bytea' THEN -2 "
    + " WHEN 'oid' THEN -5 "
    + " WHEN 'name' THEN 12 "
    + " WHEN 'ARRAY' THEN 2003 "
    + " WHEN 'geometry' THEN -4 "
    + " WHEN 'super' THEN -16 "
    + " WHEN 'varbyte' THEN -4 "
    + " WHEN 'geography' THEN -4 "
    + " ELSE 1111 END AS SMALLINT) AS DATA_TYPE, "
    + " CASE data_type "
    + " WHEN 'boolean' THEN 'bool' "
    + " WHEN 'character varying' THEN 'varchar' "
    + " WHEN '\"char\"' THEN 'char' "
    + " WHEN 'smallint' THEN 'int2' "
    + " WHEN 'integer' THEN 'int4' "
    + " WHEN 'bigint' THEN 'int8' "
    + " WHEN 'real' THEN 'float4' "
    + " WHEN 'double precision' THEN 'float8' "
    + " WHEN 'time without time zone' THEN 'time' "
    + " WHEN 'time with time zone' THEN 'timetz' "
    + " WHEN 'timestamp without time zone' THEN 'timestamp' "
    + " WHEN 'timestamp with time zone' THEN 'timestamptz' "
    + " ELSE data_type "
    + " END AS TYPE_NAME, "
    + " CASE data_type "
    + " WHEN 'int4' THEN 10 "
    + " WHEN 'bit' THEN 1 "
    + " WHEN 'bool' THEN 1 "
    + " WHEN 'boolean' THEN 1 "
    + " WHEN 'varchar' THEN character_maximum_length "
    + " WHEN 'character varying' THEN character_maximum_length "
    + " WHEN 'char' THEN character_maximum_length "
    + " WHEN 'character' THEN character_maximum_length "
    + " WHEN 'nchar' THEN character_maximum_length "
    + " WHEN 'bpchar' THEN character_maximum_length "
    + " WHEN 'nvarchar' THEN character_maximum_length "
    + " WHEN 'date' THEN 13 "
    + " WHEN 'time' THEN 15 "
    + " WHEN 'time without time zone' THEN 15 "
    + " WHEN 'timetz' THEN 21 "
    + " WHEN 'time with time zone' THEN 21 "
    + " WHEN 'timestamp' THEN 29 "
    + " WHEN 'timestamp without time zone' THEN 29 "
    + " WHEN 'timestamptz' THEN 35 "
    + " WHEN 'timestamp with time zone' THEN 35 "
    + " WHEN 'smallint' THEN 5 "
    + " WHEN 'int2' THEN 5 "
    + " WHEN 'integer' THEN 10 "
    + " WHEN 'int' THEN 10 "
    + " WHEN 'int4' THEN 10 "
    + " WHEN 'bigint' THEN 19 "
    + " WHEN 'int8' THEN 19 "
    + " WHEN 'decimal' THEN numeric_precision "
    + " WHEN 'real' THEN 8 "
    + " WHEN 'float4' THEN 8 "
    + " WHEN 'double precision' THEN 17 "
    + " WHEN 'float8' THEN 17 "
    + " WHEN 'float' THEN 17 "
    + " WHEN 'numeric' THEN numeric_precision "
    + " WHEN '_float4' THEN 8 "
    + " WHEN 'oid' THEN 10 "
    + " WHEN '_int4' THEN 10 "
    + " WHEN '_int2' THEN 5 "
    + " WHEN 'geometry' THEN NULL "
    + " WHEN 'super' THEN NULL "
    + " WHEN 'varbyte' THEN NULL "
    + " WHEN 'geography' THEN NULL "
    + " ELSE   2147483647 "
    + " END AS COLUMN_SIZE, "
    + " NULL AS BUFFER_LENGTH, "
    + " CASE data_type "
    + " WHEN 'real' THEN 8 "
    + " WHEN 'float4' THEN 8 "
    + " WHEN 'double precision' THEN 17 "
    + " WHEN 'float8' THEN 17 "
    + " WHEN 'numeric' THEN numeric_scale "
    + " WHEN 'time' THEN 6 "
    + " WHEN 'time without time zone' THEN 6 "
    + " WHEN 'timetz' THEN 6 "
    + " WHEN 'time with time zone' THEN 6 "
    + " WHEN 'timestamp' THEN 6 "
    + " WHEN 'timestamp without time zone' THEN 6 "
    + " WHEN 'timestamptz' THEN 6 " 
    + " WHEN 'timestamp with time zone' THEN 6 " 
    + " WHEN 'geometry' THEN NULL "
    + " WHEN 'super' THEN NULL "
    + " WHEN 'varbyte' THEN NULL "
    + " WHEN 'geography' THEN NULL "
    + " ELSE 0 "
    + " END AS DECIMAL_DIGITS, "
    + " CASE data_type "
    + " WHEN 'varbyte' THEN 2 "
    + " WHEN 'geography' THEN 2 "
    + " ELSE 10 "
    + " END AS NUM_PREC_RADIX, "
    + " CASE is_nullable WHEN 'YES' THEN 1 "
    + " WHEN 'NO' THEN 0 "
    + " ELSE 2 end AS NULLABLE, "
    + " REMARKS, "
    + " column_default AS COLUMN_DEF, "
    + " CAST(CASE regexp_replace(data_type, '^_.', 'ARRAY') "
    + " WHEN 'text' THEN 12 "
    + " WHEN 'bit' THEN -7 "
    + " WHEN 'bool' THEN -7 "
    + " WHEN 'boolean' THEN -7 "
    + " WHEN 'varchar' THEN 12 "
    + " WHEN 'character varying' THEN 12 "
    + " WHEN 'char' THEN 1 "
    + " WHEN 'character' THEN 1 "
    + " WHEN 'nchar' THEN 1 "
    + " WHEN 'bpchar' THEN 1 "
    + " WHEN 'nvarchar' THEN 12 "
    + " WHEN '\"char\"' THEN 1 "
    + " WHEN 'date' THEN 91 "
    + " WHEN 'time' THEN 92 "
    + " WHEN 'time without time zone' THEN 92 "
    + " WHEN 'timetz' THEN 2013 "
    + " WHEN 'time with time zone' THEN 2013 "
    + " WHEN 'timestamp' THEN 93 "
    + " WHEN 'timestamp without time zone' THEN 93 "
    + " WHEN 'timestamptz' THEN 2014 "
    + " WHEN 'timestamp with time zone' THEN 2014 "
    + " WHEN 'smallint' THEN 5 "
    + " WHEN 'int2' THEN 5 "
    + " WHEN 'integer' THEN 4 "
    + " WHEN 'int' THEN 4 "
    + " WHEN 'int4' THEN 4 "
    + " WHEN 'bigint' THEN -5 "
    + " WHEN 'int8' THEN -5 "
    + " WHEN 'decimal' THEN 3 "
    + " WHEN 'real' THEN 7 "
    + " WHEN 'float4' THEN 7 "
    + " WHEN 'double precision' THEN 8 "
    + " WHEN 'float8' THEN 8 "
    + " WHEN 'float' THEN 6 "
    + " WHEN 'numeric' THEN 2 "
    + " WHEN 'bytea' THEN -2 "
    + " WHEN 'oid' THEN -5 "
    + " WHEN 'name' THEN 12 "
    + " WHEN 'ARRAY' THEN 2003 "
    + " WHEN 'geometry' THEN -4 "
    + " WHEN 'super' THEN -16 "
    + " WHEN 'varbyte' THEN -4 "
    + " WHEN 'geography' THEN -4 "
    + " ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE, "
    + " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, "
    + " CASE data_type "
    + " WHEN 'int4' THEN 10 "
    + " WHEN 'bit' THEN 1 "
    + " WHEN 'bool' THEN 1 "
    + " WHEN 'boolean' THEN 1 "
    + " WHEN 'varchar' THEN character_maximum_length "
    + " WHEN 'character varying' THEN character_maximum_length "
    + " WHEN 'char' THEN character_maximum_length "
    + " WHEN 'character' THEN character_maximum_length "
    + " WHEN 'nchar' THEN character_maximum_length "
    + " WHEN 'bpchar' THEN character_maximum_length "
    + " WHEN 'nvarchar' THEN character_maximum_length "
    + " WHEN 'date' THEN 13 "
    + " WHEN 'time' THEN 15 "
    + " WHEN 'time without time zone' THEN 15 "
    + " WHEN 'timetz' THEN 21 "
    + " WHEN 'time with time zone' THEN 21 "
    + " WHEN 'timestamp' THEN 29 "
    + " WHEN 'timestamp without time zone' THEN 29 "
    + " WHEN 'timestamptz' THEN 35 "
    + " WHEN 'timestamp with time zone' THEN 35 "
    + " WHEN 'smallint' THEN 5 "
    + " WHEN 'int2' THEN 5 "
    + " WHEN 'integer' THEN 10 "
    + " WHEN 'int' THEN 10 "
    + " WHEN 'int4' THEN 10 "
    + " WHEN 'bigint' THEN 19 "
    + " WHEN 'int8' THEN 19 "
    + " WHEN 'decimal' THEN numeric_precision "
    + " WHEN 'real' THEN 8 "
    + " WHEN 'float4' THEN 8 "
    + " WHEN 'double precision' THEN 17 "
    + " WHEN 'float8' THEN 17 "
    + " WHEN 'float' THEN 17 "
    + " WHEN 'numeric' THEN numeric_precision "
    + " WHEN '_float4' THEN 8 "
    + " WHEN 'oid' THEN 10 "
    + " WHEN '_int4' THEN 10 "
    + " WHEN '_int2' THEN 5 "
    + " WHEN 'geometry' THEN NULL "
    + " WHEN 'super' THEN NULL "
    + " WHEN 'varbyte' THEN NULL "
    + " WHEN 'geography' THEN NULL "
    + " ELSE   2147483647 "
    + " END AS CHAR_OCTET_LENGTH, "
    + " ordinal_position AS ORDINAL_POSITION, "
    + " is_nullable AS IS_NULLABLE, "
    + " NULL AS SCOPE_CATALOG, "
    + " NULL AS SCOPE_SCHEMA, "
    + " NULL AS SCOPE_TABLE, "
    + " data_type as SOURCE_DATA_TYPE, "
    + " CASE WHEN left(column_default, 10) = '\"identity\"' THEN 'YES' "
    + " WHEN left(column_default, 16) = 'default_identity' THEN 'YES' "
    + " ELSE 'NO' END AS IS_AUTOINCREMENT, "
    + " IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN "
    + " FROM PG_CATALOG.svv_all_columns ");	  
	  
	  result.append( " WHERE true ");
	  
	  result.append(getCatalogFilterCondition(catalog, false, null));
	  
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	result.append(" AND schema_name LIKE " + escapeQuotes(schemaPattern));
    }
    if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
    	result.append(" AND table_name LIKE " + escapeQuotes(tableNamePattern));
    }
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
    	result.append(" AND COLUMN_NAME LIKE " + escapeQuotes(columnNamePattern));
    }

    result.append(" ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION ");
	  
	  return result.toString();
  }
  
  private String buildUniversalSchemaColumnsQuery(String catalog,
												String schemaPattern, 
												String tableNamePattern,
												String columnNamePattern) throws SQLException {
  	final String unknownColumnSize = "2147483647";
  
	  StringBuilder result = new StringBuilder(8192);
	
	  // NOTE: Explicit cast on current_database() prevents bug where data returned from server
	  // has incorrect length and displays random characters. [JDBC-529]
	  result.append("SELECT current_database()::varchar(128) AS TABLE_CAT,"
	      + " table_schema AS TABLE_SCHEM,"
	      + " table_name,"
	      + " COLUMN_NAME,"
	      + " CAST(CASE regexp_replace(data_type, '^_.+', 'ARRAY')"
	      + " WHEN 'text' THEN 12"
	      + " WHEN 'bit' THEN -7"
	      + " WHEN 'bool' THEN -7"
	      + " WHEN 'boolean' THEN -7"
	      + " WHEN 'varchar' THEN 12"
	      + " WHEN 'character varying' THEN 12"
	      + " WHEN 'char' THEN 1"
	      + " WHEN 'character' THEN 1"
	      + " WHEN 'nchar' THEN 1"
	      + " WHEN 'bpchar' THEN 1"
	      + " WHEN 'nvarchar' THEN 12"
	      + " WHEN '\"char\"' THEN 1"
	      + " WHEN 'date' THEN 91"
        + " WHEN 'time' THEN 92 "
        + " WHEN 'time without time zone' THEN 92 "
        + " WHEN 'timetz' THEN 2013 "
        + " WHEN 'time with time zone' THEN 2013 "
	      + " WHEN 'timestamp' THEN 93"
	      + " WHEN 'timestamp without time zone' THEN 93"
	      + " WHEN 'timestamptz' THEN 2014"
	      + " WHEN 'timestamp with time zone' THEN 2014"
	      + " WHEN 'smallint' THEN 5"
	      + " WHEN 'int2' THEN 5"
	      + " WHEN 'integer' THEN 4"
	      + " WHEN 'int' THEN 4"
	      + " WHEN 'int4' THEN 4"
	      + " WHEN 'bigint' THEN -5"
	      + " WHEN 'int8' THEN -5"
	      + " WHEN 'decimal' THEN 3"
	      + " WHEN 'real' THEN 7"
	      + " WHEN 'float4' THEN 7"
	      + " WHEN 'double precision' THEN 8"
	      + " WHEN 'float8' THEN 8"
	      + " WHEN 'float' THEN 6"
	      + " WHEN 'numeric' THEN 2"
	      + " WHEN 'bytea' THEN -2"
	      + " WHEN 'oid' THEN -5"
	      + " WHEN 'name' THEN 12"
	      + " WHEN 'ARRAY' THEN 2003"
        + " WHEN 'geometry' THEN -4 "
        + " WHEN 'super' THEN -16 "
        + " WHEN 'varbyte' THEN -4 "
        + " WHEN 'geography' THEN -4 "
	      + " ELSE 1111 END AS SMALLINT) AS DATA_TYPE,"
	      + " COALESCE("
	      + " domain_name,"
	      + " CASE data_type"
	      + " WHEN 'boolean' THEN 'bool'"
	      + " WHEN 'character varying' THEN 'varchar'"
	      + " WHEN '\"char\"' THEN 'char'"
	      + " WHEN 'smallint' THEN 'int2'"
	      + " WHEN 'integer' THEN 'int4'"
	      + " WHEN 'bigint' THEN 'int8'"
	      + " WHEN 'real' THEN 'float4'"
	      + " WHEN 'double precision' THEN 'float8'"
        + " WHEN 'time without time zone' THEN 'time'"
        + " WHEN 'time with time zone' THEN 'timetz'"
	      + " WHEN 'timestamp without time zone' THEN 'timestamp'"
	      + " WHEN 'timestamp with time zone' THEN 'timestamptz'"
	      + " ELSE data_type"
	      + " END) AS TYPE_NAME,"
	      + " CASE data_type"
	      + " WHEN 'int4' THEN 10"
	      + " WHEN 'bit' THEN 1"
	      + " WHEN 'bool' THEN 1"
	      + " WHEN 'boolean' THEN 1"
	      + " WHEN 'varchar' THEN character_maximum_length"
	      + " WHEN 'character varying' THEN character_maximum_length"
	      + " WHEN 'char' THEN character_maximum_length"
	      + " WHEN 'character' THEN character_maximum_length"
	      + " WHEN 'nchar' THEN character_maximum_length"
	      + " WHEN 'bpchar' THEN character_maximum_length"
	      + " WHEN 'nvarchar' THEN character_maximum_length"
	      + " WHEN 'date' THEN 13"
        + " WHEN 'time' THEN 15 "
        + " WHEN 'time without time zone' THEN 15 "
        + " WHEN 'timetz' THEN 21 "
        + " WHEN 'time with time zone' THEN 21 "
	      + " WHEN 'timestamp' THEN 29"
	      + " WHEN 'timestamp without time zone' THEN 29"
	      + " WHEN 'timestamptz' THEN 35"
	      + " WHEN 'timestamp with time zone' THEN 35"
	      + " WHEN 'smallint' THEN 5"
	      + " WHEN 'int2' THEN 5"
	      + " WHEN 'integer' THEN 10"
	      + " WHEN 'int' THEN 10"
	      + " WHEN 'int4' THEN 10"
	      + " WHEN 'bigint' THEN 19"
	      + " WHEN 'int8' THEN 19"
	      + " WHEN 'decimal' THEN numeric_precision"
	      + " WHEN 'real' THEN 8"
	      + " WHEN 'float4' THEN 8"
	      + " WHEN 'double precision' THEN 17"
	      + " WHEN 'float8' THEN 17"
	      + " WHEN 'float' THEN 17"
	      + " WHEN 'numeric' THEN numeric_precision"
	      + " WHEN '_float4' THEN 8"
	      + " WHEN 'oid' THEN 10"
	      + " WHEN '_int4' THEN 10"
	      + " WHEN '_int2' THEN 5"
        + " WHEN 'geometry' THEN NULL"
        + " WHEN 'super' THEN NULL"
        + " WHEN 'varbyte' THEN NULL"
        + " WHEN 'geography' THEN NULL"
	      + " ELSE " + unknownColumnSize
	      + " END AS COLUMN_SIZE,"
	      + " NULL AS BUFFER_LENGTH,"
	      + " CASE data_type"
	      + " WHEN 'real' THEN 8"
	      + " WHEN 'float4' THEN 8"
	      + " WHEN 'double precision' THEN 17"
	      + " WHEN 'float8' THEN 17"
	      + " WHEN 'numeric' THEN numeric_scale"
        + " WHEN 'time' THEN 6"
        + " WHEN 'time without time zone' THEN 6"
        + " WHEN 'timetz' THEN 6"
        + " WHEN 'time with time zone' THEN 6"
	      + " WHEN 'timestamp' THEN 6"
	      + " WHEN 'timestamp without time zone' THEN 6"
	      + " WHEN 'timestamptz' THEN 6"
	      + " WHEN 'timestamp with time zone' THEN 6"
        + " WHEN 'geometry' THEN NULL"
        + " WHEN 'super' THEN NULL"
        + " WHEN 'varbyte' THEN NULL"
        + " WHEN 'geography' THEN NULL"
	      + " ELSE 0"
	      + " END AS DECIMAL_DIGITS,"
	      + " CASE data_type"
	      + " WHEN 'varbyte' THEN 2"
          + " WHEN 'geography' THEN 2"
	      + " ELSE 10"
	      + " END AS NUM_PREC_RADIX,"
	      + " CASE is_nullable WHEN 'YES' THEN 1"
        + " WHEN 'NO' THEN 0"
        + " ELSE 2 end AS NULLABLE,"
	      + " REMARKS,"
	      + " column_default AS COLUMN_DEF,"
	      + " CAST(CASE regexp_replace(data_type, '^_.+', 'ARRAY')"
	      + " WHEN 'text' THEN 12"
	      + " WHEN 'bit' THEN -7"
	      + " WHEN 'bool' THEN -7"
	      + " WHEN 'boolean' THEN -7"
	      + " WHEN 'varchar' THEN 12"
	      + " WHEN 'character varying' THEN 12"
	      + " WHEN 'char' THEN 1"
	      + " WHEN 'character' THEN 1"
	      + " WHEN 'nchar' THEN 1"
	      + " WHEN 'bpchar' THEN 1"
	      + " WHEN 'nvarchar' THEN 12"
	      + " WHEN '\"char\"' THEN 1"
	      + " WHEN 'date' THEN 91"
        + " WHEN 'time' THEN 92 "
        + " WHEN 'time without time zone' THEN 92 "
        + " WHEN 'timetz' THEN 2013 "
        + " WHEN 'time with time zone' THEN 2013 "
	      + " WHEN 'timestamp' THEN 93"
	      + " WHEN 'timestamp without time zone' THEN 93"
	      + " WHEN 'timestamptz' THEN 2014"
	      + " WHEN 'timestamp with time zone' THEN 2014"
	      + " WHEN 'smallint' THEN 5"
	      + " WHEN 'int2' THEN 5"
	      + " WHEN 'integer' THEN 4"
	      + " WHEN 'int' THEN 4"
	      + " WHEN 'int4' THEN 4"
	      + " WHEN 'bigint' THEN -5"
	      + " WHEN 'int8' THEN -5"
	      + " WHEN 'decimal' THEN 3"
	      + " WHEN 'real' THEN 7"
	      + " WHEN 'float4' THEN 7"
	      + " WHEN 'double precision' THEN 8"
	      + " WHEN 'float8' THEN 8"
	      + " WHEN 'float' THEN 6"
	      + " WHEN 'numeric' THEN 2"
	      + " WHEN 'bytea' THEN -2"
	      + " WHEN 'oid' THEN -5"
	      + " WHEN 'name' THEN 12"
	      + " WHEN 'ARRAY' THEN 2003"
        + " WHEN 'geometry' THEN -4"
        + " WHEN 'super' THEN -16"
        + " WHEN 'varbyte' THEN -4"
        + " WHEN 'geography' THEN -4"
	      + " ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE,"
	      + " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,"
	      + " CASE data_type"
	      + " WHEN 'int4' THEN 10"
	      + " WHEN 'bit' THEN 1"
	      + " WHEN 'bool' THEN 1"
	      + " WHEN 'boolean' THEN 1"
	      + " WHEN 'varchar' THEN character_maximum_length"
	      + " WHEN 'character varying' THEN character_maximum_length"
	      + " WHEN 'char' THEN character_maximum_length"
	      + " WHEN 'character' THEN character_maximum_length"
	      + " WHEN 'nchar' THEN character_maximum_length"
	      + " WHEN 'bpchar' THEN character_maximum_length"
	      + " WHEN 'nvarchar' THEN character_maximum_length"
	      + " WHEN 'date' THEN 13"
        + " WHEN 'time' THEN 15"
        + " WHEN 'time without time zone' THEN 15"
        + " WHEN 'timetz' THEN 21"
        + " WHEN 'time with time zone' THEN 21"
	      + " WHEN 'timestamp' THEN 29"
	      + " WHEN 'timestamp without time zone' THEN 29"
	      + " WHEN 'timestamptz' THEN 35"
	      + " WHEN 'timestamp with time zone' THEN 35"
	      + " WHEN 'smallint' THEN 5"
	      + " WHEN 'int2' THEN 5"
	      + " WHEN 'integer' THEN 10"
	      + " WHEN 'int' THEN 10"
	      + " WHEN 'int4' THEN 10"
	      + " WHEN 'bigint' THEN 19"
	      + " WHEN 'int8' THEN 19"
	      + " WHEN 'decimal' THEN numeric_precision"
	      + " WHEN 'real' THEN 8"
	      + " WHEN 'float4' THEN 8"
	      + " WHEN 'double precision' THEN 17"
	      + " WHEN 'float8' THEN 17"
	      + " WHEN 'float' THEN 17"
	      + " WHEN 'numeric' THEN numeric_precision"
	      + " WHEN '_float4' THEN 8"
	      + " WHEN 'oid' THEN 10"
	      + " WHEN '_int4' THEN 10"
	      + " WHEN '_int2' THEN 5"
        + " WHEN 'geometry' THEN NULL"
        + " WHEN 'super' THEN NULL"
        + " WHEN 'varbyte' THEN NULL"
        + " WHEN 'geography' THEN NULL"
	      + " ELSE " + unknownColumnSize
	      + " END AS CHAR_OCTET_LENGTH,"
	      + " ordinal_position AS ORDINAL_POSITION,"
	      + " is_nullable AS IS_NULLABLE,"
	      + " NULL AS SCOPE_CATALOG,"
	      + " NULL AS SCOPE_SCHEMA,"
	      + " NULL AS SCOPE_TABLE,"
	      + " CASE"
	      + " WHEN domain_name is not null THEN data_type"
	      + " END AS SOURCE_DATA_TYPE,"
        + " CASE WHEN left(column_default, 10) = '\\\"identity\\\"' THEN 'YES'"
        + " WHEN left(column_default, 16) = 'default_identity' THEN 'YES' "
        + " ELSE 'NO' END AS IS_AUTOINCREMENT,"
        + " IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN"
	      + " FROM svv_columns");
	  
	  result.append( " WHERE true ");
	  
	  result.append(getCatalogFilterCondition(catalog));
	  
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	result.append(" AND table_schema LIKE " + escapeQuotes(schemaPattern));
    }
    if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
    	result.append(" AND table_name LIKE " + escapeQuotes(tableNamePattern));
    }
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
    	result.append(" AND COLUMN_NAME LIKE " + escapeQuotes(columnNamePattern));
    }

    result.append(" ORDER BY table_schem,table_name,ORDINAL_POSITION ");
	  
	  return result.toString();
  }
  
  private String buildExternalSchemaColumnsQuery(String catalog,
																				String schemaPattern, 
																				String tableNamePattern,
																				String columnNamePattern) throws SQLException {
  	final String unknownColumnSize = "2147483647";
  	
    StringBuilder result = new StringBuilder(8192);
    
    // NOTE: Explicit cast on current_database() prevents bug where data returned from server
    // has incorrect length and displays random characters. [JDBC-529]
    result.append("SELECT current_database()::varchar(128) AS TABLE_CAT," 
    + " schemaname AS TABLE_SCHEM," 
    + " tablename AS TABLE_NAME," 
    + " columnname AS COLUMN_NAME," 
    + " CAST(CASE WHEN external_type = 'text' THEN 12" 
    + " WHEN external_type = 'bit' THEN -7" 
    + " WHEN external_type = 'bool' THEN -7" 
    + " WHEN external_type = 'boolean' THEN -7"
    + " WHEN left(external_type, 7) = 'varchar' THEN 12" 
    + " WHEN left(external_type, 17) = 'character varying' THEN 12" 
    + " WHEN left(external_type, 4) = 'char' THEN 1"
    + " WHEN left(external_type, 9) = 'character' THEN 1" 
    + " WHEN left(external_type, 5) = 'nchar' THEN 1" 
    + " WHEN left(external_type, 6) = 'bpchar' THEN 1" 
    + " WHEN left(external_type, 8) = 'nvarchar' THEN 12" 
    + " WHEN external_type = '\"char\"' THEN 1" 
    + " WHEN external_type = 'date' THEN 91" 
    + " WHEN external_type = 'time' THEN 92 "
    + " WHEN external_type = 'time without time zone' THEN 92 "
    + " WHEN external_type = 'timetz' THEN 2013 "
    + " WHEN external_type = 'time with time zone' THEN 2013 "
    + " WHEN external_type = 'timestamp' THEN 93" 
    + " WHEN external_type = 'timestamp without time zone' THEN 93" 
    + " WHEN external_type = 'timestamptz' THEN 2014" 
    + " WHEN external_type = 'timestamp with time zone' THEN 2014"
    + " WHEN external_type = 'smallint' THEN 5"
    + " WHEN external_type = 'int2' THEN 5"
    + " WHEN external_type = '_int2' THEN 5" 
    + " WHEN external_type = 'integer' THEN 4" 
    + " WHEN external_type = 'int' THEN 4"
    + " WHEN external_type = 'int4' THEN 4" 
    + " WHEN external_type = '_int4' THEN 4" 
    + " WHEN external_type = 'bigint' THEN -5" 
    + " WHEN external_type = 'int8' THEN -5" 
    + " WHEN left(external_type, 7) = 'decimal' THEN 2" 
    + " WHEN external_type = 'real' THEN 7" 
    + " WHEN external_type = 'float4' THEN 7" 
    + " WHEN external_type = '_float4' THEN 7" 
    + " WHEN external_type = 'double' THEN 8" 
    + " WHEN external_type = 'double precision' THEN 8" 
    + " WHEN external_type = 'float8' THEN 8" 
    + " WHEN external_type = '_float8' THEN 8" 
    + " WHEN external_type = 'float' THEN 6" 
    + " WHEN left(external_type, 7) = 'numeric' THEN 2" 
    + " WHEN external_type = 'bytea' THEN -2" 
    + " WHEN external_type = 'oid' THEN -5" 
    + " WHEN external_type = 'name' THEN 12" 
    + " WHEN external_type = 'ARRAY' THEN 2003" 
    + " WHEN external_type = 'geometry' THEN -4"
    + " WHEN external_type = 'super' THEN -16"
    + " WHEN external_type = 'varbyte' THEN -4"
    + " WHEN external_type = 'geography' THEN -4"
    + " ELSE 1111 END AS SMALLINT) AS DATA_TYPE," 
    + " CASE WHEN left(external_type, 17) = 'character varying' THEN 'varchar'" 
    + " WHEN left(external_type, 7) = 'varchar' THEN 'varchar'" 
    + " WHEN left(external_type, 4) = 'char' THEN 'char'" 
    + " WHEN left(external_type, 7) = 'decimal' THEN 'numeric'" 
    + " WHEN left(external_type, 7) = 'numeric' THEN 'numeric'" 
    + " WHEN external_type = 'double' THEN 'double precision'" 
    + " WHEN external_type = 'time without time zone' THEN 'time'"
    + " WHEN external_type = 'time with time zone' THEN 'timetz'"
    + " WHEN external_type = 'timestamp without time zone' THEN 'timestamp'" 
    + " WHEN external_type = 'timestamp with time zone' THEN 'timestamptz'" 
    + " ELSE external_type END AS TYPE_NAME," 
    + " CASE WHEN external_type = 'int4' THEN 10" 
    + " WHEN external_type = 'bit' THEN 1" 
    + " WHEN external_type = 'bool' THEN 1" 
    + " WHEN external_type = 'boolean' THEN 1" 
    + " WHEN left(external_type, 7) = 'varchar' "
    + "  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 7) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 7) "
    + "  END::integer "
    + " WHEN left(external_type, 17) = 'character varying' "
    +	"  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 17) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 17) "
    + "  END::integer "
    + " WHEN left(external_type, 4) = 'char' "
    + "  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 4) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 4) "
    + "  END::integer "
    + " WHEN left(external_type, 9) = 'character' "
    + "	 THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 9) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 9) "
    + "  END::integer "
    + " WHEN left(external_type, 5) = 'nchar' "
    + "  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 5) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 5) "
    + "  END::integer "
    + " WHEN left(external_type, 6) = 'bpchar' "
    + "	 THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 6) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 6) "
    + "  END::integer "
    + " WHEN left(external_type, 8) = 'nvarchar' "
    + "  THEN CASE "
    + "    WHEN regexp_instr(external_type, '\\\\(', 8) = 0 THEN '0' "
    + "    ELSE regexp_substr(external_type, '[0-9]+', 8) "
    + "  END::integer "
    + " WHEN external_type = 'date' THEN 13 "
    + " WHEN external_type = 'time' THEN 15 "
    + " WHEN external_type = 'time without time zone' THEN 15 "
    + " WHEN external_type = 'timetz' THEN 21 "
    + " WHEN external_type = 'time with time zone' THEN 21 "
    + " WHEN external_type = 'timestamp' THEN 29 " 
    + " WHEN external_type = 'timestamp without time zone' THEN 29" 
    + " WHEN external_type = 'timestamptz' THEN 35" 
    + " WHEN external_type = 'timestamp with time zone' THEN 35" 
    + " WHEN external_type = 'smallint' THEN 5" 
    + " WHEN external_type = 'int2' THEN 5" 
    + " WHEN external_type = 'integer' THEN 10" 
    + " WHEN external_type = 'int' THEN 10" 
    + " WHEN external_type = 'int4' THEN 10" 
    + " WHEN external_type = 'bigint' THEN 19" 
    + " WHEN external_type = 'int8' THEN 19" 
    + " WHEN left(external_type, 7) = 'decimal' THEN isnull(nullif(regexp_substr(external_type, '[0-9]+', 7),''),'0')::integer" 
    + " WHEN external_type = 'real' THEN 8" 
    + " WHEN external_type = 'float4' THEN 8" 
    + " WHEN external_type = '_float4' THEN 8" 
    + " WHEN external_type = 'double' THEN 17" 
    + " WHEN external_type = 'double precision' THEN 17" 
    + " WHEN external_type = 'float8' THEN 17" 
    + " WHEN external_type = '_float8' THEN 17" 
    + " WHEN external_type = 'float' THEN 17" 
    + " WHEN left(external_type, 7) = 'numeric' THEN isnull(nullif(regexp_substr(external_type, '[0-9]+', 7),''),'0')::integer" 
    + " WHEN external_type = '_float4' THEN 8" 
    + " WHEN external_type = 'oid' THEN 10" 
    + " WHEN external_type = '_int4' THEN 10" 
    + " WHEN external_type = '_int2' THEN 5" 
    + " WHEN external_type = 'geometry' THEN NULL"
    + " WHEN external_type = 'super' THEN NULL"
    + " WHEN external_type = 'varbyte' THEN NULL"
    + " WHEN external_type = 'geography' THEN NULL"
    + " ELSE 2147483647 END AS COLUMN_SIZE," 
    + " NULL AS BUFFER_LENGTH," 
    + " CASE WHEN external_type = 'real'THEN 8" 
    + " WHEN external_type = 'float4' THEN 8" 
    + " WHEN external_type = 'double' THEN 17" 
    + " WHEN external_type = 'double precision' THEN 17" 
    + " WHEN external_type = 'float8' THEN 17" 
    + " WHEN left(external_type, 7) = 'numeric' THEN isnull(nullif(regexp_substr(external_type, '[0-9]+', 11),''),'0')::integer" 
    + " WHEN left(external_type, 7) = 'decimal' THEN isnull(nullif(regexp_substr(external_type, '[0-9]+', 11),''),'0')::integer" 
    + " WHEN external_type = 'time' THEN 6 "
    + " WHEN external_type = 'time without time zone' THEN 6 "
    + " WHEN external_type = 'timetz' THEN 6 "
    + " WHEN external_type = 'time with time zone' THEN 6 "
    + " WHEN external_type = 'timestamp' THEN 6" 
    + " WHEN external_type = 'timestamp without time zone' THEN 6" 
    + " WHEN external_type = 'timestamptz' THEN 6" 
    + " WHEN external_type = 'timestamp with time zone' THEN 6" 
    + " WHEN external_type = 'geometry' THEN NULL"
    + " WHEN external_type = 'super' THEN NULL"
    + " WHEN external_type = 'varbyte' THEN NULL"
    + " WHEN external_type = 'geography' THEN NULL"
    + " ELSE 0 END AS DECIMAL_DIGITS," 
    + " CASE WHEN external_type = 'varbyte' THEN 2"
    + " WHEN external_type = 'geography' THEN 2"
    + " ELSE 10"
    + " END AS NUM_PREC_RADIX,"
    + " NULL AS NULLABLE," 
    + " NULL AS REMARKS," 
    + " NULL AS COLUMN_DEF," 
    + " CAST(CASE WHEN external_type = 'text' THEN 12" 
    + " WHEN external_type = 'bit' THEN -7" 
    + " WHEN external_type = 'bool' THEN -7" 
    + " WHEN external_type = 'boolean' THEN -7" 
    + " WHEN left(external_type, 7) = 'varchar' THEN 12" 
    + " WHEN left(external_type, 17) = 'character varying' THEN 12" 
    + " WHEN left(external_type, 4) = 'char' THEN 1" 
    + " WHEN left(external_type, 9) = 'character' THEN 1" 
    + " WHEN left(external_type, 5) = 'nchar' THEN 1" 
    + " WHEN left(external_type, 6) = 'bpchar' THEN 1" 
    + " WHEN left(external_type, 8) = 'nvarchar' THEN 12" 
    + " WHEN external_type = '\"char\"' THEN 1" 
    + " WHEN external_type = 'date' THEN 91" 
    + " WHEN external_type = 'time' THEN 92 "
    + " WHEN external_type = 'time without time zone' THEN 92 "
    + " WHEN external_type = 'timetz' THEN 2013 "
    + " WHEN external_type = 'time with time zone' THEN 2013 "
    + " WHEN external_type = 'timestamp' THEN 93" 
    + " WHEN external_type = 'timestamp without time zone' THEN 93" 
    + " WHEN external_type = 'timestamptz' THEN 2014" 
    + " WHEN external_type = 'timestamp with time zone' THEN 2014" 
    + " WHEN external_type = 'smallint' THEN 5" 
    + " WHEN external_type = 'int2' THEN 5" 
    + " WHEN external_type = '_int2' THEN 5" 
    + " WHEN external_type = 'integer' THEN 4" 
    + " WHEN external_type = 'int' THEN 4" 
    + " WHEN external_type = 'int4' THEN 4" 
    + " WHEN external_type = '_int4' THEN 4" 
    + " WHEN external_type = 'bigint' THEN -5" 
    + " WHEN external_type = 'int8' THEN -5" 
    + " WHEN left(external_type, 7) = 'decimal' THEN 3" 
    + " WHEN external_type = 'real' THEN 7" 
    + " WHEN external_type = 'float4' THEN 7" 
    + " WHEN external_type = '_float4' THEN 7" 
    + " WHEN external_type = 'double' THEN 8" 
    + " WHEN external_type = 'double precision' THEN 8" 
    + " WHEN external_type = 'float8' THEN 8" 
    + " WHEN external_type = '_float8' THEN 8" 
    + " WHEN external_type = 'float' THEN 6" 
    + " WHEN left(external_type, 7) = 'numeric' THEN 2" 
    + " WHEN external_type = 'bytea' THEN -2" 
    + " WHEN external_type = 'oid' THEN -5" 
    + " WHEN external_type = 'name' THEN 12" 
    + " WHEN external_type = 'ARRAY' THEN 2003" 
    + " WHEN external_type = 'geometry' THEN -4"
    + " WHEN external_type = 'super' THEN -16"
    + " WHEN external_type = 'varbyte' THEN -4"
    + " WHEN external_type = 'geography' THEN -4"
    + " ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE," 
    + " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB," 
    + " CASE WHEN left(external_type, 7) = 'varchar' "
    + "  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 7) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 7) "
    + "  END::integer "
    + " WHEN left(external_type, 17) = 'character varying' "
    +	"  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 17) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 17) "
    + "  END::integer "
    + " WHEN left(external_type, 4) = 'char' "
    + "  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 4) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 4) "
    + "  END::integer "
    + " WHEN left(external_type, 9) = 'character' "
    + "	 THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 9) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 9) "
    + "  END::integer "
    + " WHEN left(external_type, 5) = 'nchar' "
    + "  THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 5) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 5) "
    + "  END::integer "
    + " WHEN left(external_type, 6) = 'bpchar' "
    + "	 THEN CASE "
    + "   WHEN regexp_instr(external_type, '\\\\(', 6) = 0 THEN '0' "
    + "   ELSE regexp_substr(external_type, '[0-9]+', 6) "
    + "  END::integer "
    + " WHEN left(external_type, 8) = 'nvarchar' "
    + "  THEN CASE "
    + "    WHEN regexp_instr(external_type, '\\\\(', 8) = 0 THEN '0' "
    + "    ELSE regexp_substr(external_type, '[0-9]+', 8) "
    + "  END::integer "
    + " WHEN external_type = 'string' THEN 16383" 
    + " ELSE NULL END AS CHAR_OCTET_LENGTH," 
    + " columnnum AS ORDINAL_POSITION," 
    + " NULL AS IS_NULLABLE," 
    + " NULL AS SCOPE_CATALOG," 
    + " NULL AS SCOPE_SCHEMA," 
    + " NULL AS SCOPE_TABLE," 
    + " NULL AS SOURCE_DATA_TYPE," 
    + " 'NO' AS IS_AUTOINCREMENT," 
    + " 'NO' AS IS_GENERATEDCOLUMN" 
    + " FROM svv_external_columns");
  	
	  result.append( " WHERE true ");
	  
	  result.append(getCatalogFilterCondition(catalog));
	  
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	result.append(" AND schemaname LIKE " + escapeQuotes(schemaPattern));
    }
    if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
    	result.append(" AND tablename LIKE " + escapeQuotes(tableNamePattern));
    }
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
    	result.append(" AND columnname LIKE " + escapeQuotes(columnNamePattern));
    }

    result.append(" ORDER BY table_schem,table_name,ORDINAL_POSITION ");
	  
	  return result.toString();
  }
  

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
      String columnNamePattern) throws SQLException {
    Field[] f = new Field[8];
    List<Tuple> v = new ArrayList<Tuple>();

    f[0] = new Field("TABLE_CAT", Oid.VARCHAR);
    f[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    f[2] = new Field("TABLE_NAME", Oid.VARCHAR);
    f[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
    f[4] = new Field("GRANTOR", Oid.VARCHAR);
    f[5] = new Field("GRANTEE", Oid.VARCHAR);
    f[6] = new Field("PRIVILEGE", Oid.VARCHAR);
    f[7] = new Field("IS_GRANTABLE", Oid.VARCHAR);

    String sql;
    sql = "SELECT n.nspname,c.relname,u.usename,c.relacl, "
//          + (connection.haveMinimumServerVersion(ServerVersion.v8_4) ? "a.attacl, " : "")
          + " a.attname "
          + " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, "
          + " pg_catalog.pg_user u, pg_catalog.pg_attribute a "
          + " WHERE c.relnamespace = n.oid "
          + " AND c.relowner = u.usesysid "
          + " AND c.oid = a.attrelid "
          + " AND c.relkind = 'r' "
          + " AND a.attnum > 0 AND NOT a.attisdropped ";

    sql += getCatalogFilterCondition(catalog);
    
    if (schema != null && !schema.isEmpty()) {
      sql += " AND n.nspname = " + escapeQuotes(schema);
    }
    if (table != null && !table.isEmpty()) {
      sql += " AND c.relname = " + escapeQuotes(table);
    }
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
      sql += " AND a.attname LIKE " + escapeQuotes(columnNamePattern);
    }
    sql += " ORDER BY attname ";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      byte[] schemaName = rs.getBytes("nspname");
      byte[] tableName = rs.getBytes("relname");
      byte[] column = rs.getBytes("attname");
      String owner = rs.getString("usename");
      String relAcl = rs.getString("relacl");

      // For instance: SELECT -> user1 -> list of [grantor, grantable]
      Map<String, Map<String, List<String[]>>> permissions = parseACL(relAcl, owner);

/*      if (connection.haveMinimumServerVersion(ServerVersion.v8_4)) {
        String acl = rs.getString("attacl");
        Map<String, Map<String, List<String[]>>> relPermissions = parseACL(acl, owner);
        permissions.putAll(relPermissions);
      } */
      String[] permNames = permissions.keySet().toArray(new String[0]);
      Arrays.sort(permNames);
      for (String permName : permNames) {
        byte[] privilege = connection.encodeString(permName);
        Map<String, List<String[]>> grantees = permissions.get(permName);
        for (Map.Entry<String, List<String[]>> userToGrantable : grantees.entrySet()) {
          List<String[]> grantor = userToGrantable.getValue();
          String grantee = userToGrantable.getKey();
          for (String[] grants : grantor) {
            String grantable = owner.equals(grantee) ? "YES" : grants[1];
            byte[][] tuple = new byte[8][];
            tuple[0] = connection.encodeString(connection.getCatalog());
            tuple[1] = schemaName;
            tuple[2] = tableName;
            tuple[3] = column;
            tuple[4] = connection.encodeString(grants[0]);
            tuple[5] = connection.encodeString(grantee);
            tuple[6] = privilege;
            tuple[7] = connection.encodeString(grantable);
            v.add(new Tuple(tuple));
          }
        }
      }
    }
    rs.close();
    stmt.close();

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    Field[] f = new Field[7];
    List<Tuple> v = new ArrayList<Tuple>();

    f[0] = new Field("TABLE_CAT", Oid.VARCHAR);
    f[1] = new Field("TABLE_SCHEM", Oid.VARCHAR);
    f[2] = new Field("TABLE_NAME", Oid.VARCHAR);
    f[3] = new Field("GRANTOR", Oid.VARCHAR);
    f[4] = new Field("GRANTEE", Oid.VARCHAR);
    f[5] = new Field("PRIVILEGE", Oid.VARCHAR);
    f[6] = new Field("IS_GRANTABLE", Oid.VARCHAR);

    String sql;
    sql = "SELECT n.nspname,c.relname,u.usename,c.relacl "
          + " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_user u "
          + " WHERE c.relnamespace = n.oid "
          + " AND c.relowner = u.usesysid "
          + " AND c.relkind IN ('r','p','v','m','f') ";

    sql += getCatalogFilterCondition(catalog);
    
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
      sql += " AND n.nspname LIKE " + escapeQuotes(schemaPattern);
    }

    if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
      sql += " AND c.relname LIKE " + escapeQuotes(tableNamePattern);
    }
    sql += " ORDER BY nspname, relname ";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      byte[] schema = rs.getBytes("nspname");
      byte[] table = rs.getBytes("relname");
      String owner = rs.getString("usename");
      String acl = rs.getString("relacl");
      Map<String, Map<String, List<String[]>>> permissions = parseACL(acl, owner);
      String[] permNames = permissions.keySet().toArray(new String[0]);
      Arrays.sort(permNames);
      for (String permName : permNames) {
        byte[] privilege = connection.encodeString(permName);
        Map<String, List<String[]>> grantees = permissions.get(permName);
        for (Map.Entry<String, List<String[]>> userToGrantable : grantees.entrySet()) {
          List<String[]> grants = userToGrantable.getValue();
          String granteeUser = userToGrantable.getKey();
          for (String[] grantTuple : grants) {
            // report the owner as grantor if it's missing
            String grantor = grantTuple[0] == null ? owner : grantTuple[0];
            // owner always has grant privileges
            String grantable = owner.equals(granteeUser) ? "YES" : grantTuple[1];
            byte[][] tuple = new byte[7][];
            tuple[0] = connection.encodeString(connection.getCatalog());
            tuple[1] = schema;
            tuple[2] = table;
            tuple[3] = connection.encodeString(grantor);
            tuple[4] = connection.encodeString(granteeUser);
            tuple[5] = privilege;
            tuple[6] = connection.encodeString(grantable);
            v.add(new Tuple(tuple));
          }
        }
      }
    }
    rs.close();
    stmt.close();

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  /**
   * Parse an String of ACLs into a List of ACLs.
   */
  private static List<String> parseACLArray(String aclString) {
    List<String> acls = new ArrayList<String>();
    if (aclString == null || aclString.isEmpty()) {
      return acls;
    }
    boolean inQuotes = false;
    // start at 1 because of leading "{"
    int beginIndex = 1;
    char prevChar = ' ';
    for (int i = beginIndex; i < aclString.length(); i++) {

      char c = aclString.charAt(i);
      if (c == '"' && prevChar != '\\') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        acls.add(aclString.substring(beginIndex, i));
        beginIndex = i + 1;
      }
      prevChar = c;
    }
    // add last element removing the trailing "}"
    acls.add(aclString.substring(beginIndex, aclString.length() - 1));

    // Strip out enclosing quotes, if any.
    for (int i = 0; i < acls.size(); i++) {
      String acl = acls.get(i);
      if (acl.startsWith("\"") && acl.endsWith("\"")) {
        acl = acl.substring(1, acl.length() - 1);
        acls.set(i, acl);
      }
    }
    return acls;
  }

  /**
   * Add the user described by the given acl to the Lists of users with the privileges described by
   * the acl.
   */
  private static void addACLPrivileges(String acl, Map<String, Map<String, List<String[]>>> privileges) {
    int equalIndex = acl.lastIndexOf("=");
    int slashIndex = acl.lastIndexOf("/");
    if (equalIndex == -1) {
      return;
    }

    String user = acl.substring(0, equalIndex);
    String grantor = null;
    if (user.isEmpty()) {
      user = "PUBLIC";
    }
    String privs;
    if (slashIndex != -1) {
      privs = acl.substring(equalIndex + 1, slashIndex);
      grantor = acl.substring(slashIndex + 1, acl.length());
    } else {
      privs = acl.substring(equalIndex + 1, acl.length());
    }

    for (int i = 0; i < privs.length(); i++) {
      char c = privs.charAt(i);
      if (c != '*') {
        String sqlpriv;
        String grantable;
        if (i < privs.length() - 1 && privs.charAt(i + 1) == '*') {
          grantable = "YES";
        } else {
          grantable = "NO";
        }
        switch (c) {
          case 'a':
            sqlpriv = "INSERT";
            break;
          case 'r':
          case 'p':
            sqlpriv = "SELECT";
            break;
          case 'w':
            sqlpriv = "UPDATE";
            break;
          case 'd':
            sqlpriv = "DELETE";
            break;
          case 'D':
            sqlpriv = "TRUNCATE";
            break;
          case 'R':
            sqlpriv = "RULE";
            break;
          case 'x':
            sqlpriv = "REFERENCES";
            break;
          case 't':
            sqlpriv = "TRIGGER";
            break;
          // the following can't be granted to a table, but
          // we'll keep them for completeness.
          case 'X':
            sqlpriv = "EXECUTE";
            break;
          case 'U':
            sqlpriv = "USAGE";
            break;
          case 'C':
            sqlpriv = "CREATE";
            break;
          case 'T':
            sqlpriv = "CREATE TEMP";
            break;
          default:
            sqlpriv = "UNKNOWN";
        }

        Map<String, List<String[]>> usersWithPermission = privileges.get(sqlpriv);
        String[] grant = {grantor, grantable};

        if (usersWithPermission == null) {
          usersWithPermission = new HashMap<String, List<String[]>>();
          List<String[]> permissionByGrantor = new ArrayList<String[]>();
          permissionByGrantor.add(grant);
          usersWithPermission.put(user, permissionByGrantor);
          privileges.put(sqlpriv, usersWithPermission);
        } else {
          List<String[]> permissionByGrantor = usersWithPermission.get(user);
          if (permissionByGrantor == null) {
            permissionByGrantor = new ArrayList<String[]>();
            permissionByGrantor.add(grant);
            usersWithPermission.put(user, permissionByGrantor);
          } else {
            permissionByGrantor.add(grant);
          }
        }
      }
    }
  }

  /**
   * Take the a String representing an array of ACLs and return a Map mapping the SQL permission
   * name to a List of usernames who have that permission.
   * For instance: {@code SELECT -> user1 -> list of [grantor, grantable]}
   *
   * @param aclArray ACL array
   * @param owner owner
   * @return a Map mapping the SQL permission name
   */
  public Map<String, Map<String, List<String[]>>> parseACL(String aclArray, String owner) {
    if (aclArray == null) {
      // arwdxt -- 8.2 Removed the separate RULE permission
      // arwdDxt -- 8.4 Added a separate TRUNCATE permission
      String perms = "arwdxt"; // connection.haveMinimumServerVersion(ServerVersion.v8_4) ? "arwdDxt" : "arwdxt";

      aclArray = "{" + owner + "=" + perms + "/" + owner + "}";
    }

    List<String> acls = parseACLArray(aclArray);
    Map<String, Map<String, List<String[]>>> privileges =
        new HashMap<String, Map<String, List<String[]>>>();
    for (String acl : acls) {
      addACLPrivileges(acl, privileges);
    }
    return privileges;
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
      int scope, boolean nullable) throws SQLException {
    Field[] f = new Field[8];
    List<Tuple> v = new ArrayList<Tuple>(); // The new ResultSet tuple stuff

    f[0] = new Field("SCOPE", Oid.INT2);
    f[1] = new Field("COLUMN_NAME", Oid.VARCHAR);
    f[2] = new Field("DATA_TYPE", Oid.INT2);
    f[3] = new Field("TYPE_NAME", Oid.VARCHAR);
    f[4] = new Field("COLUMN_SIZE", Oid.INT4);
    f[5] = new Field("BUFFER_LENGTH", Oid.INT4);
    f[6] = new Field("DECIMAL_DIGITS", Oid.INT2);
    f[7] = new Field("PSEUDO_COLUMN", Oid.INT2);

    /*
     * At the moment this simply returns a table's primary key, if there is one. I believe other
     * unique indexes, ctid, and oid should also be considered. -KJ
     */

    String sql;
    
    sql =
        "SELECT a.attname, a.atttypid, a.atttypmod " +
        "FROM  " +
           "pg_catalog.pg_namespace n,  " +
           "pg_catalog.pg_class ct,  " +
           "pg_catalog.pg_class ci, " +
           "pg_catalog.pg_attribute a, " +
           "pg_catalog.pg_index i " +
       "WHERE " +
           "ct.oid=i.indrelid AND " +
           "ci.oid=i.indexrelid  AND " +
           "a.attrelid=ci.oid AND " +
           "i.indisprimary  AND " +
           "ct.relnamespace = n.oid ";
    
    sql += getCatalogFilterCondition(catalog);
    
    if (schema != null && !schema.isEmpty()) {
      sql += " AND n.nspname = " + escapeQuotes(schema);
    }

    if (table != null && !table.isEmpty()) {
      sql += " AND ct.relname = " + escapeQuotes(table);
    }
    
    sql += " ORDER BY a.attnum ";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      byte[][] tuple = new byte[8][];
      int typeOid = (int) rs.getLong("atttypid");
      int typeMod = rs.getInt("atttypmod");
      int decimalDigits = connection.getTypeInfo().getScale(typeOid, typeMod);
      int columnSize = connection.getTypeInfo().getPrecision(typeOid, typeMod);
      if (columnSize == 0) {
        columnSize = connection.getTypeInfo().getDisplaySize(typeOid, typeMod);
      }
      tuple[0] = connection.encodeString(Integer.toString(scope));
      tuple[1] = rs.getBytes("attname");
      tuple[2] =
          connection.encodeString(Integer.toString(connection.getTypeInfo().getSQLType(typeOid)));
      tuple[3] = connection.encodeString(connection.getTypeInfo().getRSType(typeOid));
      tuple[4] = connection.encodeString(Integer.toString(columnSize));
      tuple[5] = null; // unused
      tuple[6] = connection.encodeString(Integer.toString(decimalDigits));
      tuple[7] =
          connection.encodeString(Integer.toString(java.sql.DatabaseMetaData.bestRowNotPseudo));
      v.add(new Tuple(tuple));
    }
    rs.close();
    stmt.close();

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    Field[] f = new Field[8];
    List<Tuple> v = new ArrayList<Tuple>(); // The new ResultSet tuple stuff

    f[0] = new Field("SCOPE", Oid.INT2);
    f[1] = new Field("COLUMN_NAME", Oid.VARCHAR);
    f[2] = new Field("DATA_TYPE", Oid.INT2);
    f[3] = new Field("TYPE_NAME", Oid.VARCHAR);
    f[4] = new Field("COLUMN_SIZE", Oid.INT4);
    f[5] = new Field("BUFFER_LENGTH", Oid.INT4);
    f[6] = new Field("DECIMAL_DIGITS", Oid.INT2);
    f[7] = new Field("PSEUDO_COLUMN", Oid.INT2);

    byte[][] tuple = new byte[8][];

    /*
     * Redshift does not have any column types that are automatically updated like some databases'
     * timestamp type. We can't tell what rules or triggers might be doing, so we are left with the
     * system columns that change on an update. An update may change all of the following system
     * columns: ctid, xmax, xmin, cmax, and cmin. Depending on if we are in a transaction and
     * whether we roll it back or not the only guaranteed change is to ctid. -KJ
     */

    tuple[0] = null;
    tuple[1] = connection.encodeString("ctid");
    tuple[2] =
        connection.encodeString(Integer.toString(connection.getTypeInfo().getSQLType("tid")));
    tuple[3] = connection.encodeString("tid");
    tuple[4] = null;
    tuple[5] = null;
    tuple[6] = null;
    tuple[7] =
        connection.encodeString(Integer.toString(java.sql.DatabaseMetaData.versionColumnPseudo));
    v.add(new Tuple(tuple));

    /*
     * Perhaps we should check that the given catalog.schema.table actually exists. -KJ
     */
    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {
    String sql;
    
    sql =
        "SELECT " +
           "current_database() AS TABLE_CAT, " +
           "n.nspname AS TABLE_SCHEM,  " +
           "ct.relname AS TABLE_NAME,   " +
           "a.attname AS COLUMN_NAME,   " +
           "a.attnum AS KEY_SEQ,   " +
           "ci.relname AS PK_NAME   " +
        "FROM  " +
           "pg_catalog.pg_namespace n,  " +
           "pg_catalog.pg_class ct,  " +
           "pg_catalog.pg_class ci, " +
           "pg_catalog.pg_attribute a, " +
           "pg_catalog.pg_index i " +
       "WHERE " +
           "ct.oid=i.indrelid AND " +
           "ci.oid=i.indexrelid  AND " +
           "a.attrelid=ci.oid AND " +
           "i.indisprimary  AND " +
           "ct.relnamespace = n.oid ";

    sql += getCatalogFilterCondition(catalog);
    
    if (schema != null && !schema.isEmpty()) {
      sql += " AND n.nspname = " + escapeQuotes(schema);
    }

    if (table != null && !table.isEmpty()) {
      sql += " AND ct.relname = " + escapeQuotes(table);
    }

    sql += " ORDER BY table_name, pk_name, key_seq";

    return createMetaDataStatement().executeQuery(sql);
  }

  /**
   * @param primaryCatalog primary catalog
   * @param primarySchema primary schema
   * @param primaryTable if provided will get the keys exported by this table
   * @param foreignCatalog foreign catalog
   * @param foreignSchema foreign schema
   * @param foreignTable if provided will get the keys imported by this table
   * @return ResultSet
   * @throws SQLException if something wrong happens
   */
  protected ResultSet getImportedExportedKeys(String primaryCatalog, String primarySchema,
      String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable)
          throws SQLException {

    /*
     * The addition of the pg_constraint in 7.3 table should have really helped us out here, but it
     * comes up just a bit short. - The conkey, confkey columns aren't really useful without
     * contrib/array unless we want to issues separate queries. - Unique indexes that can support
     * foreign keys are not necessarily added to pg_constraint. Also multiple unique indexes
     * covering the same keys can be created which make it difficult to determine the PK_NAME field.
     */

    String sql =
        "SELECT current_database() AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, "
            + "current_database() AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, "
            + "pos.n AS KEY_SEQ, "
            + "CASE con.confupdtype "
            + " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade
            + " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull
            + " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault
            + " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'p' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction
            + " ELSE NULL END AS UPDATE_RULE, "
            + "CASE con.confdeltype "
            + " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade
            + " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull
            + " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault
            + " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'p' THEN " + DatabaseMetaData.importedKeyRestrict
            + " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction
            + " ELSE NULL END AS DELETE_RULE, "
            + "con.conname AS FK_NAME, pkic.relname AS PK_NAME, "
            + "CASE "
            + " WHEN con.condeferrable AND con.condeferred THEN "
            + DatabaseMetaData.importedKeyInitiallyDeferred
            + " WHEN con.condeferrable THEN " + DatabaseMetaData.importedKeyInitiallyImmediate
            + " ELSE " + DatabaseMetaData.importedKeyNotDeferrable
            + " END AS DEFERRABILITY "
            + " FROM "
            + " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, "
            + " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, "
            + " pg_catalog.pg_constraint con, "
            + " pg_catalog.generate_series(1, " + getMaxIndexKeys() + ") pos(n), "
            + " pg_catalog.pg_class pkic";
    // Starting in Postgres 9.0, pg_constraint was augmented with the conindid column, which
    // contains the oid of the index supporting the constraint. This makes it unnecessary to do a
    // further join on pg_depend.
//    if (!connection.haveMinimumServerVersion(ServerVersion.v9_0)) {
      sql += ", pg_catalog.pg_depend dep ";
//    }
    sql +=
        " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid "
            + " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid "
            + " AND con.contype = 'f' AND pkic.relkind = 'i' ";
//    if (!connection.haveMinimumServerVersion(ServerVersion.v9_0)) {
      sql += " AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid ";
/*    } else {
      sql += " AND pkic.oid = con.conindid ";
    } */

    sql += getCatalogFilterCondition(primaryCatalog);
    
    if (primarySchema != null && !primarySchema.isEmpty()) {
      sql += " AND pkn.nspname = " + escapeQuotes(primarySchema);
    }
    if (foreignSchema != null && !foreignSchema.isEmpty()) {
      sql += " AND fkn.nspname = " + escapeQuotes(foreignSchema);
    }
    if (primaryTable != null && !primaryTable.isEmpty()) {
      sql += " AND pkc.relname = " + escapeQuotes(primaryTable);
    }
    if (foreignTable != null && !foreignTable.isEmpty()) {
      sql += " AND fkc.relname = " + escapeQuotes(foreignTable);
    }

    if (primaryTable != null) {
      sql += " ORDER BY fkn.nspname,fkc.relname,con.conname,pos.n";
    } else {
      sql += " ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n";
    }

    return createMetaDataStatement().executeQuery(sql);
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return getImportedExportedKeys(null, null, null, catalog, schema, table);
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return getImportedExportedKeys(catalog, schema, table, null, null, null);
  }

  public ResultSet getCrossReference(String primaryCatalog, String primarySchema,
      String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable)
          throws SQLException {
    return getImportedExportedKeys(primaryCatalog, primarySchema, primaryTable, foreignCatalog,
        foreignSchema, foreignTable);
  }

  public ResultSet getTypeInfo() throws SQLException {

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true);
  	
    Field[] f = new Field[18];
    List<Tuple> v = new ArrayList<Tuple>(); // The new ResultSet tuple stuff

    f[0] = new Field("TYPE_NAME", Oid.VARCHAR);
    f[1] = new Field("DATA_TYPE", Oid.INT2);
    f[2] = new Field("PRECISION", Oid.INT4);
    f[3] = new Field("LITERAL_PREFIX", Oid.VARCHAR);
    f[4] = new Field("LITERAL_SUFFIX", Oid.VARCHAR);
    f[5] = new Field("CREATE_PARAMS", Oid.VARCHAR);
    f[6] = new Field("NULLABLE", Oid.INT2);
    f[7] = new Field("CASE_SENSITIVE", Oid.BOOL);
    f[8] = new Field("SEARCHABLE", Oid.INT2);
    f[9] = new Field("UNSIGNED_ATTRIBUTE", Oid.BOOL);
    f[10] = new Field("FIXED_PREC_SCALE", Oid.BOOL);
    f[11] = new Field("AUTO_INCREMENT", Oid.BOOL);
    f[12] = new Field("LOCAL_TYPE_NAME", Oid.VARCHAR);
    f[13] = new Field("MINIMUM_SCALE", Oid.INT2);
    f[14] = new Field("MAXIMUM_SCALE", Oid.INT2);
    f[15] = new Field("SQL_DATA_TYPE", Oid.INT4);
    f[16] = new Field("SQL_DATETIME_SUB", Oid.INT4);
    f[17] = new Field("NUM_PREC_RADIX", Oid.INT4);

    String sql;
    sql = "SELECT t.typname,t.oid FROM pg_catalog.pg_type t"
//          + " JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
//          + " WHERE n.nspname  != 'pg_toast'"
//          + " AND "
    		  + " WHERE "
          + " t.typname in ("
          + "'bool','char','int8','int2','int4','float4','float8','bpchar','varchar','date','time','timestamp','timestamptz','numeric','refcursor','geometry','super','varbyte','geography')";
//          + " AND "
//          + " (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))";

/*    if (connection.getHideUnprivilegedObjects() && connection.haveMinimumServerVersion(ServerVersion.v9_2)) {
      sql += " AND has_type_privilege(t.oid, 'USAGE')";
    } */

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    // cache some results, this will keep memory usage down, and speed
    // things up a little.
    byte[] bZero = connection.encodeString("0");
    byte[] b10 = connection.encodeString("10");
    byte[] b2 = connection.encodeString("2");
    byte[] bf = connection.encodeString("f");
    byte[] bt = connection.encodeString("t");
    byte[] bliteral = connection.encodeString("'");
    byte[] bNullable =
              connection.encodeString(Integer.toString(java.sql.DatabaseMetaData.typeNullable));
    byte[] bSearchable =
              connection.encodeString(Integer.toString(java.sql.DatabaseMetaData.typeSearchable));

    TypeInfo ti = connection.getTypeInfo();
    if (ti instanceof TypeInfoCache) {
      ((TypeInfoCache) ti).cacheSQLTypes(connection.getLogger());
    }

    while (rs.next()) {
      byte[][] tuple = new byte[19][];
      String typname = rs.getString(1);
      int typeOid = (int) rs.getLong(2);

      tuple[0] = connection.encodeString(typname);
      int sqlType = connection.getTypeInfo().getSQLType(typname);
      tuple[1] =
          connection.encodeString(Integer.toString(sqlType));

      /* this is just for sorting below, the result set never sees this */
      tuple[18] = BigInteger.valueOf(sqlType).toByteArray();

      tuple[2] = connection
          .encodeString(Integer.toString(connection.getTypeInfo().getMaximumPrecision(typeOid)));

      // Using requiresQuoting(oid) would might trigger select statements that might fail with NPE
      // if oid in question is being dropped.
      // requiresQuotingSqlType is not bulletproof, however, it solves the most visible NPE.
      if (connection.getTypeInfo().requiresQuotingSqlType(sqlType)) {
        tuple[3] = bliteral;
        tuple[4] = bliteral;
      }

      tuple[6] = bNullable; // all types can be null
      tuple[7] = connection.getTypeInfo().isCaseSensitive(typeOid) ? bt : bf;
      tuple[8] = bSearchable; // any thing can be used in the WHERE clause
      tuple[9] = connection.getTypeInfo().isSigned(typeOid) ? bf : bt;
      tuple[10] = bf; // false for now - must handle money
      tuple[11] = bf; // false - it isn't autoincrement
      tuple[13] = bZero; // min scale is zero
      // only numeric can supports a scale.
      tuple[14] = (typeOid == Oid.NUMERIC) ? connection.encodeString("1000") : bZero;

      // 12 - LOCAL_TYPE_NAME is null
      // 15 & 16 are unused so we return null

      // VARBYTE and GEOGRAPHY is base2,everything else is base 10      
      tuple[17] = (typeOid == Oid.VARBYTE || typeOid == Oid.GEOGRAPHY) ? b2 : b10; 
      v.add(new Tuple(tuple));

      // add pseudo-type serial, bigserial
/*      if (typname.equals("int4")) {
        byte[][] tuple1 = tuple.clone();

        tuple1[0] = connection.encodeString("serial");
        tuple1[11] = bt;
        v.add(new Tuple(tuple1));
      } else if (typname.equals("int8")) {
        byte[][] tuple1 = tuple.clone();

        tuple1[0] = connection.encodeString("bigserial");
        tuple1[11] = bt;
        v.add(new Tuple(tuple1));
      } */

    }
    rs.close();
    stmt.close();

    Collections.sort(v, new Comparator<Tuple>() {
      @Override
      public int compare(Tuple o1, Tuple o2) {
        int i1 = ByteConverter.bytesToInt(o1.get(18));
        int i2 = ByteConverter.bytesToInt(o2.get(18));
        return (i1 < i2) ? -1 : ((i1 == i2) ? 0 : 1);
      }
    });
    
    ResultSet rc = ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  public ResultSet getIndexInfo(String catalog, String schema, String tableName,
      boolean unique, boolean approximate) throws SQLException {
    /*
     * This is a complicated function because we have three possible situations: <= 7.2 no schemas,
     * single column functional index 7.3 schemas, single column functional index >= 7.4 schemas,
     * multi-column expressional index >= 8.3 supports ASC/DESC column info >= 9.0 no longer renames
     * index columns on a table column rename, so we must look at the table attribute names
     *
     * with the single column functional index we need an extra join to the table's pg_attribute
     * data to get the column the function operates on.
     */
    String sql;
    
/*    if (connection.haveMinimumServerVersion(ServerVersion.v8_3)) {
      sql = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, "
            + "  ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, "
            + "  NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, "
            + "  CASE i.indisclustered "
            + "    WHEN true THEN " + java.sql.DatabaseMetaData.tableIndexClustered
            + "    ELSE CASE am.amname "
            + "      WHEN 'hash' THEN " + java.sql.DatabaseMetaData.tableIndexHashed
            + "      ELSE " + java.sql.DatabaseMetaData.tableIndexOther
            + "    END "
            + "  END AS TYPE, "
            + "  (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION, "
            + "  ci.reltuples AS CARDINALITY, "
            + "  ci.relpages AS PAGES, "
            + "  pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION, "
            + "  ci.oid AS CI_OID, "
            + "  i.indoption AS I_INDOPTION, "
            + (connection.haveMinimumServerVersion(ServerVersion.v9_6) ? "  am.amname AS AM_NAME " : "  am.amcanorder AS AM_CANORDER ")
            + "FROM pg_catalog.pg_class ct "
            + "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
            + "  JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid) "
            + "  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) "
            + "  JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) "
            + "WHERE true ";

      if (schema != null && !schema.isEmpty()) {
        sql += " AND n.nspname = " + escapeQuotes(schema);
      }

      sql += " AND ct.relname = " + escapeQuotes(tableName);

      if (unique) {
        sql += " AND i.indisunique ";
      }

      sql = "SELECT "
                + "    tmp.TABLE_CAT, "
                + "    tmp.TABLE_SCHEM, "
                + "    tmp.TABLE_NAME, "
                + "    tmp.NON_UNIQUE, "
                + "    tmp.INDEX_QUALIFIER, "
                + "    tmp.INDEX_NAME, "
                + "    tmp.TYPE, "
                + "    tmp.ORDINAL_POSITION, "
                + "    trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS COLUMN_NAME, "
                + (connection.haveMinimumServerVersion(ServerVersion.v9_6)
                        ? "  CASE tmp.AM_NAME "
                        + "    WHEN 'btree' THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1 "
                        + "      WHEN 1 THEN 'D' "
                        + "      ELSE 'A' "
                        + "    END "
                        + "    ELSE NULL "
                        + "  END AS ASC_OR_DESC, "
                        : "  CASE tmp.AM_CANORDER "
                        + "    WHEN true THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1 "
                        + "      WHEN 1 THEN 'D' "
                        + "      ELSE 'A' "
                        + "    END "
                        + "    ELSE NULL "
                        + "  END AS ASC_OR_DESC, ")
                + "    tmp.CARDINALITY, "
                + "    tmp.PAGES, "
                + "    tmp.FILTER_CONDITION "
                + "FROM ("
                + sql
                + ") AS tmp";
    } 
    else */
/*    {
      String select;
      String from;
      String where;

      select = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ";
      from = " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class ct, pg_catalog.pg_class ci, "
             + " pg_catalog.pg_attribute a, pg_catalog.pg_am am ";
      where = " AND n.oid = ct.relnamespace ";
      from += ", pg_catalog.pg_index i ";

      if (schema != null && !schema.isEmpty()) {
        where += " AND n.nspname = " + escapeQuotes(schema);
      }

      sql = select
            + " ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, "
            + " CASE i.indisclustered "
            + " WHEN true THEN " + java.sql.DatabaseMetaData.tableIndexClustered
            + " ELSE CASE am.amname "
            + " WHEN 'hash' THEN " + java.sql.DatabaseMetaData.tableIndexHashed
            + " ELSE " + java.sql.DatabaseMetaData.tableIndexOther
            + " END "
            + " END AS TYPE, "
            + " a.attnum AS ORDINAL_POSITION, "
            + " CASE WHEN i.indexprs IS NULL THEN a.attname "
            + " ELSE pg_catalog.pg_get_indexdef(ci.oid,a.attnum,false) END AS COLUMN_NAME, "
            + " NULL AS ASC_OR_DESC, "
            + " ci.reltuples AS CARDINALITY, "
            + " ci.relpages AS PAGES, "
            + " pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION "
            + from
            + " WHERE ct.oid=i.indrelid AND ci.oid=i.indexrelid AND a.attrelid=ci.oid AND ci.relam=am.oid "
            + where;

      sql += " AND ct.relname = " + escapeQuotes(tableName);

      if (unique) {
        sql += " AND i.indisunique ";
      }
    }

    sql += " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION "; */
    	
    //	Disable, Redshift doesn't do indexes...
    //	we'll just execute a dummy query to return an empty resultset
    	
    sql =
			"SELECT '' AS TABLE_CAT, " +
			"'' AS TABLE_SCHEM, " +
			"'' AS TABLE_NAME, " +
			"cast('t' as boolean) AS NON_UNIQUE, " +
			"'' AS INDEX_QUALIFIER, " +
			"'' AS INDEX_NAME, " +
			"cast(0 as smallint) AS TYPE, " +
			"cast(1 as smallint) AS ORDINAL_POSITION, " +
			"'' AS COLUMN_NAME, " +
			"NULL AS ASC_OR_DESC, " +
			"0 AS CARDINALITY, " +
			"0 AS PAGES, " +
			"'' AS FILTER_CONDITION " +
			"WHERE (1 = 0)";

    return createMetaDataStatement().executeQuery(sql);
  }

  // ** JDBC 2 Extensions **

  public boolean supportsResultSetType(int type) throws SQLException {
    // The only type we don't support
    return type != ResultSet.TYPE_SCROLL_SENSITIVE;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    // These combinations are not supported!
    if (type == ResultSet.TYPE_SCROLL_SENSITIVE) {
      return false;
    }

    // We do not support Updateable ResultSets
    if (concurrency == ResultSet.CONCUR_UPDATABLE) {
      return false; // true
    }

    // Everything else we do
    return true;
  }

  /* lots of unsupported stuff... */
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return true;
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return true;
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    // indicates that
    return true;
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersDeletesAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean deletesAreDetected(int i) throws SQLException {
    return false;
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
      int[] types) throws SQLException {
    String sql = "select "
        + "current_database() as type_cat, n.nspname as type_schem, t.typname as type_name,  null as class_name, "
        + "CASE WHEN t.typtype='c' then " + java.sql.Types.STRUCT + " else "
        + java.sql.Types.DISTINCT
        + " end as data_type, pg_catalog.obj_description(t.oid, 'pg_type')  "
        + "as remarks, CASE WHEN t.typtype = 'd' then  (select CASE";

    StringBuilder sqlwhen = new StringBuilder();
    for (Iterator<String> i = connection.getTypeInfo().getRSTypeNamesWithSQLTypes(); i.hasNext(); ) {
      String pgType = i.next();
      int sqlType = connection.getTypeInfo().getSQLType(pgType);
      sqlwhen.append(" when typname = ").append(escapeQuotes(pgType)).append(" then ").append(sqlType);
    }
    sql += sqlwhen.toString();

    sql += " else " + java.sql.Types.OTHER + " end from pg_type where oid=t.typbasetype) "
        + "else null end as base_type "
        + "from pg_catalog.pg_type t, pg_catalog.pg_namespace n where t.typnamespace = n.oid and n.nspname != 'pg_catalog' and n.nspname != 'pg_toast'";

    StringBuilder toAdd = new StringBuilder();
    if (types != null) {
      toAdd.append(" and (false ");
      for (int type : types) {
        switch (type) {
          case Types.STRUCT:
            toAdd.append(" or t.typtype = 'c'");
            break;
          case Types.DISTINCT:
            toAdd.append(" or t.typtype = 'd'");
            break;
        }
      }
      toAdd.append(" ) ");
    } else {
      toAdd.append(" and t.typtype IN ('c','d') ");
    }
    // spec says that if typeNamePattern is a fully qualified name
    // then the schema and catalog are ignored

    if (typeNamePattern != null) {
      // search for qualifier
      int firstQualifier = typeNamePattern.indexOf('.');
      int secondQualifier = typeNamePattern.lastIndexOf('.');

      if (firstQualifier != -1) {
        // if one of them is -1 they both will be
        if (firstQualifier != secondQualifier) {
          // we have a catalog.schema.typename, ignore catalog
          schemaPattern = typeNamePattern.substring(firstQualifier + 1, secondQualifier);
        } else {
          // we just have a schema.typename
          schemaPattern = typeNamePattern.substring(0, firstQualifier);
        }
        // strip out just the typeName
        typeNamePattern = typeNamePattern.substring(secondQualifier + 1);
      }
      toAdd.append(" and t.typname like ").append(escapeQuotes(typeNamePattern));
    }

    toAdd.append(getCatalogFilterCondition(catalog));
    
    // schemaPattern may have been modified above
    if (schemaPattern != null) {
      toAdd.append(" and n.nspname like ").append(escapeQuotes(schemaPattern));
    }
    sql += toAdd.toString();

/*    if (connection.getHideUnprivilegedObjects()
        && connection.haveMinimumServerVersion(ServerVersion.v9_2)) {
      sql += " AND has_type_privilege(t.oid, 'USAGE')";
    } */

    sql += " order by data_type, type_schem, type_name";
    return createMetaDataStatement().executeQuery(sql);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  protected Statement createMetaDataStatement() throws SQLException {
    return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
  }

  public long getMaxLogicalLobSize() throws SQLException {
    return 0;
  }

  public boolean supportsRefCursors() throws SQLException {
    return true;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getRowIdLifetime()");
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    Field[] f = new Field[4];
    f[0] = new Field("NAME", Oid.VARCHAR);
    f[1] = new Field("MAX_LEN", Oid.INT4);
    f[2] = new Field("DEFAULT_VALUE", Oid.VARCHAR);
    f[3] = new Field("DESCRIPTION", Oid.VARCHAR);

    List<Tuple> v = new ArrayList<Tuple>();

/*    if (connection.haveMinimumServerVersion(ServerVersion.v9_0)) */
    {
      byte[][] tuple = new byte[4][];
      tuple[0] = connection.encodeString("ApplicationName");
      tuple[1] = connection.encodeString(Integer.toString(getMaxNameLength()));
      tuple[2] = null; // connection.encodeString("");
      tuple[3] = connection
          .encodeString("The name of the application currently utilizing the connection.");
      v.add(new Tuple(tuple));
    } 

    return ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern, functionNamePattern);
  	
    // The pg_get_function_result only exists 8.4 or later
//    boolean pgFuncResultExists = connection.haveMinimumServerVersion(ServerVersion.v8_4);

    // Use query that support pg_get_function_result to get function result, else unknown is defaulted
//    String funcTypeSql = DatabaseMetaData.functionResultUnknown + " ";
/*    if (pgFuncResultExists) {
      funcTypeSql = " CASE "
              + "   WHEN (format_type(p.prorettype, null) = 'unknown') THEN " + DatabaseMetaData.functionResultUnknown
              + "   WHEN "
              + "     (substring(pg_get_function_result(p.oid) from 0 for 6) = 'TABLE') OR "
              + "     (substring(pg_get_function_result(p.oid) from 0 for 6) = 'SETOF') THEN " + DatabaseMetaData.functionReturnsTable
              + "   ELSE " + DatabaseMetaData.functionNoTable
              + " END ";
    } */

    // Build query and result
    String sql;
    sql = "SELECT routine_catalog AS FUNCTION_CAT, "
        + " routine_schema AS FUNCTION_SCHEM, "
        + " routine_name AS FUNCTION_NAME,"
        + " CAST('' AS VARCHAR(256)) AS REMARKS, "
        + " CASE data_type"
        + " WHEN 'USER-DEFINED' THEN 0"
        + " WHEN 'record' THEN 2"
        + " ELSE 1"
        + " END AS FUNCTION_TYPE, "
        + " specific_name AS SPECIFIC_NAME"
        + " FROM INFORMATION_SCHEMA.ROUTINES"
        + " WHERE routine_type LIKE 'FUNCTION' ";
    
/*    sql = "SELECT current_database() AS FUNCTION_CAT, n.nspname AS FUNCTION_SCHEM, p.proname AS FUNCTION_NAME, "
        + " d.description AS REMARKS, "
        + funcTypeSql + " AS FUNCTION_TYPE, "
        + " p.proname || '_' || p.prooid AS SPECIFIC_NAME "
        + "FROM pg_catalog.pg_proc_info p "
        + "INNER JOIN pg_catalog.pg_namespace n ON p.pronamespace=n.oid "
        + "LEFT JOIN pg_catalog.pg_description d ON p.prooid=d.objoid "
        + "WHERE true  "; */
    
    sql += getCatalogFilterCondition(catalog);
    
    /*
    if the user provides a schema then search inside the schema for it
     */
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
//      sql += " AND n.nspname LIKE " + escapeQuotes(schemaPattern);
    		sql += " AND  routine_schema LIKE " + escapeQuotes(schemaPattern);
    } 
/*    else {
      // if no schema is provided then limit the search inside the search_path 
      sql += "and pg_function_is_visible(p.prooid)";
    } */
    
    if (functionNamePattern != null && !functionNamePattern.isEmpty()) {
//      sql += " AND p.proname LIKE " + escapeQuotes(functionNamePattern);
      sql += " AND  routine_name LIKE " + escapeQuotes(functionNamePattern);;
    }
    
/*    if (connection.getHideUnprivilegedObjects()) {
      sql += " AND has_function_privilege(p.prooid,'EXECUTE')";
    } */
    
//    sql += " ORDER BY FUNCTION_SCHEM, FUNCTION_NAME, p.prooid::text ";
     sql += " ORDER BY routine_catalog, routine_schema, routine_name ";

    ResultSet rs = createMetaDataStatement().executeQuery(sql);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);
    
    return rs;
  }

  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern)
      throws SQLException {
    int columns = 17;

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, catalog, schemaPattern, functionNamePattern, columnNamePattern);
    
/*    Field[] f = new Field[columns];
    List<Tuple> v = new ArrayList<Tuple>();

    f[0] = new Field("FUNCTION_CAT", Oid.VARCHAR);
    f[1] = new Field("FUNCTION_SCHEM", Oid.VARCHAR);
    f[2] = new Field("FUNCTION_NAME", Oid.VARCHAR);
    f[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
    f[4] = new Field("COLUMN_TYPE", Oid.INT2);
    f[5] = new Field("DATA_TYPE", Oid.INT2);
    f[6] = new Field("TYPE_NAME", Oid.VARCHAR);
    f[7] = new Field("PRECISION", Oid.INT2);
    f[8] = new Field("LENGTH", Oid.INT4);
    f[9] = new Field("SCALE", Oid.INT2);
    f[10] = new Field("RADIX", Oid.INT2);
    f[11] = new Field("NULLABLE", Oid.INT2);
    f[12] = new Field("REMARKS", Oid.VARCHAR);
    f[13] = new Field("CHAR_OCTET_LENGTH", Oid.INT4);
    f[14] = new Field("ORDINAL_POSITION", Oid.INT4);
    f[15] = new Field("IS_NULLABLE", Oid.VARCHAR);
    f[16] = new Field("SPECIFIC_NAME", Oid.VARCHAR);
*/    

/*    String sql;
    	sql = "SELECT n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype,t.typrelid, "
        + " p.proargnames, p.proargmodes, p.proallargtypes, p.prooid "
        + " FROM pg_catalog.pg_proc_info p, pg_catalog.pg_namespace n, pg_catalog.pg_type t "
        + " WHERE p.pronamespace=n.oid AND p.prorettype=t.oid ";
*/
	final String unknownColumnSize = "2147483647";
	final String superMaxLength = "4194304 ";
	final String varbyteMaxLength = "1000000 ";
    final String geographyMaxLength = "1000000 ";
	
	StringBuilder functionColumnQuery = new StringBuilder();
	
	functionColumnQuery.append(
	    "SELECT PROCEDURE_CAT AS FUNCTION_CAT, "
			+ " PROCEDURE_SCHEM AS FUNCTION_SCHEM, "
	    + " PROCEDURE_NAME AS FUNCTION_NAME,"
			+ " COLUMN_NAME, "
	    + " COLUMN_TYPE, "
			+ " DATA_TYPE, "
	    + " TYPE_NAME, "
			+ " COLUMN_SIZE AS PRECISION, "
	    + " LENGTH , "
			+ " DECIMAL_DIGITS AS SCALE, "
	    + " NUM_PREC_RADIX AS RADIX, "
			+ " NULLABLE, "
	    + " REMARKS, "
	    + " CHAR_OCTET_LENGTH, "
	    + " ORDINAL_POSITION, "
	    + " IS_NULLABLE, "
	    + " SPECIFIC_NAME  "
	    + " FROM (");
	
	functionColumnQuery.append("SELECT current_database() AS PROCEDURE_CAT, "
	        + " n.nspname as PROCEDURE_SCHEM, "
	        + " p.proname AS PROCEDURE_NAME, "
	        + " CAST(CASE ((array_upper(proargnames, 0) - array_lower(proargnames, 0)) > 0) "
	        + " WHEN 't' THEN proargnames[array_upper(proargnames, 1)] "
	        + " ELSE '' "
	        + " END AS VARCHAR(256)) AS COLUMN_NAME, "
	
	        + " CAST(CASE p.proretset "
	        + " WHEN 't' THEN 3 "
	        + " ELSE 5 "
	        + " END AS SMALLINT) AS COLUMN_TYPE, "
	        + " CAST(CASE pg_catalog.format_type(p.prorettype, NULL) "
	        + " WHEN 'text' THEN 12 "
	        + " WHEN 'bit' THEN  -7 "
	        + " WHEN 'bool' THEN  -7 "
	        + " WHEN 'boolean' THEN  -7 "
	        + " WHEN 'varchar' THEN 12 "
	        + " WHEN 'character varying' THEN  12 "
	        + " WHEN '\"char\"' THEN 1"
	        + " WHEN 'char' THEN  1 "
	        + " WHEN 'character' THEN  1 "
	        + " WHEN 'nchar' THEN 1 "
	        + " WHEN 'bpchar' THEN 1 "
	        + " WHEN 'nvarchar' THEN 12 "
	        + " WHEN 'date' THEN 91 "
	        + " WHEN 'timestamp' THEN 93 "
	        + " WHEN 'timestamp without time zone' THEN 93 "
	        + " WHEN 'timestamptz' THEN 2014 "
	        + " WHEN 'timestamp with time zone' THEN 2014 "
	        + " WHEN 'smallint' THEN 5 "
	        + " WHEN 'int2' THEN 5 "
	        + " WHEN 'integer' THEN 4 "
	        + " WHEN 'int' THEN 4 "
	        + " WHEN 'int4' THEN 4 "
	        + " WHEN 'bigint' THEN -5 "
	        + " WHEN 'int8' THEN -5 "
	        + " WHEN 'real' THEN 7 "
	        + " WHEN 'float4' THEN 7 "
	        + " WHEN 'double precision' THEN 6 "
	        + " WHEN 'float8' THEN 6 "
	        + " WHEN 'float' THEN 6 "
	        + " WHEN 'decimal' THEN 3 "
	        + " WHEN 'numeric' THEN 2 "
	        + " WHEN '_float4' THEN 2003 "
	        + " WHEN '_aclitem' THEN 2003 "
	        + " WHEN '_text' THEN 2003 "
	        + " WHEN 'bytea' THEN -2 "
	        + " WHEN 'oid' THEN -5 "
	        + " WHEN 'name' THEN 12 "
	        + " WHEN '_int4' THEN 2003 "
	        + " WHEN '_int2' THEN 2003 "
	        + " WHEN 'ARRAY' THEN 2003 "
	        + " WHEN 'geometry' THEN -4 "
	        + " WHEN 'super' THEN -1 "
	        + " WHEN 'varbyte' THEN -4 "
            + " WHEN 'geography' THEN -4 "
	        + " ELSE 1111 "
	        + " END AS SMALLINT) AS DATA_TYPE, "
	        + " pg_catalog.format_type(p.prorettype, NULL) AS TYPE_NAME, "
	        + " CASE pg_catalog.format_type(p.prorettype, NULL) "
	        + " WHEN 'text' THEN NULL "
	        + " WHEN 'varchar' THEN NULL "
	        + " WHEN 'character varying' THEN NULL "
	        + " WHEN '\"char\"' THEN NULL "
	        + " WHEN 'character' THEN NULL "
	        + " WHEN 'nchar' THEN NULL "
	        + " WHEN 'bpchar' THEN NULL "
	        + " WHEN 'nvarchar' THEN NULL "
	        + " WHEN 'text' THEN NULL "
	        + " WHEN 'date' THEN NULL "
	        + " WHEN 'timestamp' THEN 6 "
	        + " WHEN 'smallint' THEN 5 "
	        + " WHEN 'int2' THEN 5 "
	        + " WHEN 'integer' THEN 10 "
	        + " WHEN 'int' THEN 10 "
	        + " WHEN 'int4' THEN 10 "
	        + " WHEN 'bigint' THEN 19 "
	        + " WHEN 'int8' THEN 19 "
	        + " WHEN 'decimal' THEN 38 "
	        + " WHEN 'real' THEN 24 "
	        + " WHEN 'float4' THEN 53 "
	        + " WHEN 'double precision' THEN 53 "
	        + " WHEN 'float8' THEN 53 "
	        + " WHEN 'float' THEN 53 "
	        + " WHEN 'geometry' THEN NULL "
	        + " WHEN 'super' THEN " + superMaxLength
	        + " WHEN 'varbyte' THEN " + varbyteMaxLength
            + " WHEN 'geography' THEN " + geographyMaxLength
	        + " ELSE " + unknownColumnSize
	        + " END AS COLUMN_SIZE, "
	        + " CASE pg_catalog.format_type(p.prorettype, NULL) "
	        + " WHEN 'text' THEN NULL "
	        + " WHEN 'varchar' THEN NULL "
	        + " WHEN 'character varying' THEN NULL "
	        + " WHEN '\"char\"' THEN NULL "
	        + " WHEN 'character' THEN NULL "
	        + " WHEN 'nchar' THEN NULL "
	        + " WHEN 'bpchar' THEN NULL "
	        + " WHEN 'nvarchar' THEN NULL "
	        + " WHEN 'date' THEN 6 "
	        + " WHEN 'timestamp' THEN 6 "
	        + " WHEN 'smallint' THEN 2 "
	        + " WHEN 'int2' THEN 2 "
	        + " WHEN 'integer' THEN 4 "
	        + " WHEN 'int' THEN 4 "
	        + " WHEN 'int4' THEN 4 "
	        + " WHEN 'bigint' THEN 20 "
	        + " WHEN 'int8' THEN 20 "
	        + " WHEN 'decimal' THEN 8 "
	        + " WHEN 'real' THEN 4 "
	        + " WHEN 'float4' THEN 8 "
	        + " WHEN 'double precision' THEN 8 "
	        + " WHEN 'float8' THEN 8 "
	        + " WHEN 'float' THEN  8 "
	        + " WHEN 'geometry' THEN NULL "
	        + " WHEN 'super' THEN " + superMaxLength
	        + " WHEN 'varbyte' THEN " + varbyteMaxLength
            + " WHEN 'geography' THEN " + geographyMaxLength
	        + " END AS LENGTH, "
	        + " CAST(CASE pg_catalog.format_type(p.prorettype, NULL) "
	        + " WHEN 'smallint' THEN 0 "
	        + " WHEN 'int2' THEN 0 "
	        + " WHEN 'integer' THEN 0 "
	        + " WHEN 'int' THEN 0 "
	        + " WHEN 'int4' THEN 0 "
	        + " WHEN 'bigint' THEN 0 "
	        + " WHEN 'int8' THEN 0 "
	        + " WHEN 'decimal' THEN 0 "
	        + " WHEN 'real' THEN 8 "
	        + " WHEN 'float4' THEN 8 "
	        + " WHEN 'double precision' THEN 17 "
	        + " WHEN 'float' THEN 17 "
	        + " WHEN 'float8' THEN 17 "
	        + " WHEN 'numeric' THEN 0 "
	        + " WHEN 'timestamp' THEN 6 "
	        + " WHEN 'timestamp without time zone' THEN 6 "
	        + " WHEN 'timestamptz' THEN 6 "
	        + " WHEN 'timestamp with time zone' THEN 6 "
	        + " ELSE NULL END AS SMALLINT) AS DECIMAL_DIGITS, "
	        + " 10 AS NUM_PREC_RADIX, "
	        + " CAST(2 AS SMALLINT) AS NULLABLE, "
	        + " CAST('' AS VARCHAR(256)) AS REMARKS, "
	        + " CAST(NULL AS SMALLINT) AS CHAR_OCTET_LENGTH, "
	        + " CAST(0 AS SMALLINT) AS ORDINAL_POSITION, "
	        + " CAST('' AS VARCHAR(256)) AS IS_NULLABLE, "
	        + " p.proname || '_' || p.prooid AS SPECIFIC_NAME, "
	        + " p.prooid as PROOID, "
	        + " -1 AS PROARGINDEX "
	
	        + " FROM pg_catalog.pg_proc_info p LEFT JOIN pg_namespace n ON n.oid = p.pronamespace "
	        + " WHERE pg_catalog.format_type(p.prorettype, NULL) != 'void' "
	        + " AND prokind = 'f' ");
    
		functionColumnQuery.append(getCatalogFilterCondition(catalog));
	
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
//      sql += " AND n.nspname LIKE " + escapeQuotes(schemaPattern);
    	functionColumnQuery.append(" AND n.nspname LIKE " + escapeQuotes(schemaPattern));
    }
    if (functionNamePattern != null && !functionNamePattern.isEmpty()) {
//      sql += " AND p.proname LIKE " + escapeQuotes(functionNamePattern);
    	functionColumnQuery.append(" AND p.proname LIKE " + escapeQuotes(functionNamePattern));    	
    }
    if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
    	functionColumnQuery.append(" AND COLUMN_NAME LIKE " + escapeQuotes(columnNamePattern));    	
    }
    
    functionColumnQuery.append(" UNION ALL ");

    functionColumnQuery.append(" SELECT DISTINCT current_database() AS PROCEDURE_CAT, "
            + " PROCEDURE_SCHEM, "
            + " PROCEDURE_NAME, "
            + "CAST(CASE (char_length(COLUMN_NAME) > 0) WHEN 't' THEN COLUMN_NAME "
            + "ELSE '' "
            + "END AS VARCHAR(256)) AS COLUMN_NAME, "

            + " CAST( CASE COLUMN_TYPE "
            + " WHEN 105 THEN 1 "
            + " WHEN 98 THEN 2 "
            + " WHEN 111 THEN 4 "
            + " ELSE 5 END AS SMALLINT) AS COLUMN_TYPE, "
            + " CAST(CASE DATA_TYPE "
            + " WHEN 'text' THEN 12 "
            + " WHEN 'bit' THEN  -7 "
            + " WHEN 'bool' THEN  -7 "
            + " WHEN 'boolean' THEN  -7 "
            + " WHEN 'varchar' THEN 12 "
            + " WHEN 'character varying' THEN  12 "
            + " WHEN '\"char\"' THEN  1 "
            + " WHEN 'char' THEN  1 "
            + " WHEN 'character' THEN  1 "
            + " WHEN 'nchar' THEN 1 "
            + " WHEN 'bpchar' THEN 1 "
            + " WHEN 'nvarchar' THEN 12 "
            + " WHEN 'date' THEN 91 "
            + " WHEN 'timestamp' THEN 93 "
            + " WHEN 'timestamp without time zone' THEN 93 "
            + " WHEN 'timestamptz' THEN 2014 "
            + " WHEN 'timestamp with time zone' THEN 2014 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN -5 "
            + " WHEN 'int8' THEN -5 "
            + " WHEN 'real' THEN 7 "
            + " WHEN 'float4' THEN 7 "
            + " WHEN 'double precision' THEN 6 "
            + " WHEN 'float8' THEN 6 "
            + " WHEN 'float' THEN 6 "
            + " WHEN 'decimal' THEN 3 "
            + " WHEN 'numeric' THEN 2 "
            + " WHEN 'bytea' THEN -2 "
            + " WHEN 'oid' THEN -5 "
            + " WHEN 'name' THEN 12 "
            + " WHEN 'ARRAY' THEN 2003 "
            + " WHEN 'geometry' THEN -4 "
            + " WHEN 'super' THEN -1 "
            + " WHEN 'varbyte' THEN -4 "
            + " WHEN 'geography' THEN -4 "
            + " ELSE 1111 "
            + " END AS SMALLINT) AS DATA_TYPE, "
            + " TYPE_NAME, "
            + " CASE COLUMN_SIZE "
            + " WHEN 'text' THEN NULL "
            + " WHEN 'varchar' THEN NULL "
            + " WHEN 'character varying' THEN NULL "
            + " WHEN '\"char\"' THEN NULL "
            + " WHEN 'character' THEN NULL "
            + " WHEN 'nchar' THEN NULL "
            + " WHEN 'bpchar' THEN NULL "
            + " WHEN 'nvarchar' THEN NULL "
            + " WHEN 'text' THEN NULL "
            + " WHEN 'date' THEN NULL "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'smallint' THEN 5 "
            + " WHEN 'int2' THEN 5 "
            + " WHEN 'integer' THEN 10 "
            + " WHEN 'int' THEN 10 "
            + " WHEN 'int4' THEN 10 "
            + " WHEN 'bigint' THEN 19 "
            + " WHEN 'int8' THEN 19 "
            + " WHEN 'decimal' THEN 38 "
            + " WHEN 'real' THEN 24 "
            + " WHEN 'float4' THEN 53 "
            + " WHEN 'double precision' THEN 53 "
            + " WHEN 'float8' THEN 53 "
            + " WHEN 'float' THEN 53 "
            + " WHEN 'geometry' THEN NULL "
            + " WHEN 'super' THEN " + superMaxLength
            + " WHEN 'varbyte' THEN " + varbyteMaxLength
            + " WHEN 'geography' THEN " + geographyMaxLength
            + " ELSE " + unknownColumnSize
            + " END AS COLUMN_SIZE, "
            + " CASE LENGTH "
            + " WHEN 'text' THEN NULL "
            + " WHEN 'varchar' THEN NULL "
            + " WHEN 'character varying' THEN NULL "
            + " WHEN '\"char\"' THEN NULL "
            + " WHEN 'character' THEN NULL "
            + " WHEN 'nchar' THEN NULL "
            + " WHEN 'bpchar' THEN NULL "
            + " WHEN 'nvarchar' THEN NULL "
            + " WHEN 'date' THEN 6 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'smallint' THEN 2 "
            + " WHEN 'int2' THEN 2 "
            + " WHEN 'integer' THEN 4 "
            + " WHEN 'int' THEN 4 "
            + " WHEN 'int4' THEN 4 "
            + " WHEN 'bigint' THEN 20 "
            + " WHEN 'int8' THEN 20 "
            + " WHEN 'decimal' THEN 8 "
            + " WHEN 'real' THEN 4 "
            + " WHEN 'float4' THEN 8 "
            + " WHEN 'double precision' THEN 8 "
            + " WHEN 'float8' THEN 8 "
            + " WHEN 'float' THEN  8 "
            + " WHEN 'geometry' THEN NULL "
            + " WHEN 'super' THEN " + superMaxLength
            + " WHEN 'varbyte' THEN " + varbyteMaxLength
            + " WHEN 'geography' THEN " + geographyMaxLength
            + " END AS LENGTH, "
            + " CAST(CASE DECIMAL_DIGITS "
            + " WHEN 'smallint' THEN 0 "
            + " WHEN 'int2' THEN 0 "
            + " WHEN 'integer' THEN 0 "
            + " WHEN 'int' THEN 0 "
            + " WHEN 'int4' THEN 0 "
            + " WHEN 'bigint' THEN 0 "
            + " WHEN 'int8' THEN 0 "
            + " WHEN 'decimal' THEN 0 "
            + " WHEN 'real' THEN 8 "
            + " WHEN 'float4' THEN 8 "
            + " WHEN 'double precision' THEN 17 "
            + " WHEN 'float' THEN 17 "
            + " WHEN 'float8' THEN 17 "
            + " WHEN 'numeric' THEN 0 "
            + " WHEN 'timestamp' THEN 6 "
            + " WHEN 'timestamp without time zone' THEN 6 "
            + " WHEN 'timestamptz' THEN 6 "
            + " WHEN 'timestamp with time zone' THEN 6 "
            + " ELSE NULL END AS SMALLINT) AS DECIMAL_DIGITS, "
            + " 10 AS NUM_PREC_RADIX, "
            + " CAST(2 AS SMALLINT) AS NULLABLE, "
            + " CAST(''AS VARCHAR(256)) AS REMARKS, "
            + " CAST(NULL AS SMALLINT) AS CHAR_OCTET_LENGTH, "
            + " PROARGINDEX AS ORDINAL_POSITION, "
            + " CAST(''AS VARCHAR(256)) AS IS_NULLABLE, "
            + " SPECIFIC_NAME, PROOID, PROARGINDEX "
            + " FROM ( "
            + " SELECT current_database() AS PROCEDURE_CAT,"
            + " n.nspname AS PROCEDURE_SCHEM, "
            + " proname AS PROCEDURE_NAME, "
            + " CASE WHEN (proallargtypes is NULL) THEN proargnames[pos+1] "
            + " ELSE proargnames[pos] END AS COLUMN_NAME,"
            + " CASE WHEN proargmodes is NULL THEN 105 "
            + " ELSE CAST(proargmodes[pos] AS INT) END AS COLUMN_TYPE, "
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS DATA_TYPE,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL) "
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS TYPE_NAME,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS COLUMN_SIZE,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS LENGTH,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS DECIMAL_DIGITS,"
            + " CASE WHEN proallargtypes is NULL THEN pg_catalog.format_type(proargtypes[pos], NULL)"
            + " ELSE pg_catalog.format_type(proallargtypes[pos], NULL) END AS RADIX,"
            + " CAST(2 AS SMALLINT) AS NULLABLE,"
            + " CAST(''AS VARCHAR(256)) AS REMARKS,"
            + " pg_catalog.format_type(proargtypes[pos], NULL) AS CHAR_OCTET_LENGTH,"
            + " CASE WHEN (proallargtypes is NULL) THEN pos+1"
            + " WHEN pos = array_upper(proallargtypes, 1) THEN 0"
            + " ELSE pos END AS ORDINAL_POSITION,"
            + " CAST('' AS VARCHAR(256)) AS IS_NULLABLE,"
            + " p.prooid AS PROOID,"
            + " CASE WHEN (proallargtypes is NULL) THEN pos+1"
            + " WHEN prokind = 'f' AND pos = array_upper(proallargtypes, 1) THEN 0"
            + " ELSE pos END AS PROARGINDEX, "
            + " p.proname || '_' || p.prooid AS SPECIFIC_NAME "
            + " FROM (pg_catalog.pg_proc_info p LEFT JOIN pg_namespace n"
            + " ON n.oid = p.pronamespace)"
            + " LEFT JOIN (SELECT "
            + " CASE WHEN (proallargtypes IS NULL) "
            + " THEN generate_series(array_lower(proargnames, 1), array_upper(proargnames, 1))-1"
            + " ELSE generate_series(array_lower(proargnames, 1), array_upper(proargnames, 1)+1)-1 "
            + " END AS pos"
            + " FROM pg_catalog.pg_proc_info p ) AS s ON (pos >= 0 AND pos <= pronargs+1)"
            + " WHERE prokind = 'f' ");
    
    if (schemaPattern != null && !schemaPattern.isEmpty()) {
    	functionColumnQuery.append(" AND n.nspname LIKE " + escapeQuotes(schemaPattern));
    }
    if (functionNamePattern != null && !functionNamePattern.isEmpty()) {
    	functionColumnQuery.append(" AND p.proname LIKE " + escapeQuotes(functionNamePattern));    	
    }
	  if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
	  	functionColumnQuery.append(" AND COLUMN_NAME LIKE " + escapeQuotes(columnNamePattern));    	
	  }
    
    functionColumnQuery.append(" ) AS INPUT_PARAM_TABLE"
        + " WHERE ORDINAL_POSITION IS NOT NULL"
        + " ) AS RESULT_SET WHERE (DATA_TYPE != 1111 OR (TYPE_NAME IS NOT NULL AND TYPE_NAME != '-'))"
        + " ORDER BY PROCEDURE_CAT ,PROCEDURE_SCHEM,"
        + " PROCEDURE_NAME, PROOID, PROARGINDEX, COLUMN_TYPE DESC");
	  
//    sql += " ORDER BY n.nspname, p.proname, p.prooid::text ";

//    byte[] isnullableUnknown = new byte[0];

//    Statement stmt = connection.createStatement();
//    ResultSet rs = stmt.executeQuery(functionColumnQuery.toString()); // sql
    
/*    
    while (rs.next()) {
      byte[] schema = rs.getBytes("nspname");
      byte[] functionName = rs.getBytes("proname");
      byte[] specificName =
          connection.encodeString(rs.getString("proname") + "_" + rs.getString("prooid"));
      int returnType = (int) rs.getLong("prorettype");
      String returnTypeType = rs.getString("typtype");
      int returnTypeRelid = (int) rs.getLong("typrelid");

      String strArgTypes = rs.getString("proargtypes");
      StringTokenizer st = new StringTokenizer(strArgTypes);
      List<Long> argTypes = new ArrayList<Long>();
      while (st.hasMoreTokens()) {
        argTypes.add(Long.valueOf(st.nextToken()));
      }

      String[] argNames = null;
      Array argNamesArray = rs.getArray("proargnames");
      if (argNamesArray != null) {
        argNames = (String[]) argNamesArray.getArray();
      }

      String[] argModes = null;
      Array argModesArray = rs.getArray("proargmodes");
      if (argModesArray != null) {
        argModes = (String[]) argModesArray.getArray();
      }

      int numArgs = argTypes.size();

      Long[] allArgTypes = null;
      Array allArgTypesArray = rs.getArray("proallargtypes");
      if (allArgTypesArray != null) {
        allArgTypes = (Long[]) allArgTypesArray.getArray();
        numArgs = allArgTypes.length;
      }

      // decide if we are returning a single column result.
      if (returnTypeType.equals("b") || returnTypeType.equals("d") || returnTypeType.equals("e")
          || (returnTypeType.equals("p") && argModesArray == null)) {
        byte[][] tuple = new byte[columns][];
        tuple[0] = connection.encodeString(connection.getCatalog());
        tuple[1] = schema;
        tuple[2] = functionName;
        tuple[3] = connection.encodeString("returnValue");
        tuple[4] = connection
            .encodeString(Integer.toString(java.sql.DatabaseMetaData.functionReturn));
        tuple[5] = connection
            .encodeString(Integer.toString(connection.getTypeInfo().getSQLType(returnType)));
        tuple[6] = connection.encodeString(connection.getTypeInfo().getRSType(returnType));
        tuple[7] = null;
        tuple[8] = null;
        tuple[9] = null;
        tuple[10] = null;
        tuple[11] = connection
            .encodeString(Integer.toString(java.sql.DatabaseMetaData.functionNullableUnknown));
        tuple[12] = null;
        tuple[14] = connection.encodeString(Integer.toString(0));
        tuple[15] = isnullableUnknown;
        tuple[16] = specificName;

        v.add(new Tuple(tuple));
      }

      // Add a row for each argument.
      for (int i = 0; i < numArgs; i++) {
        byte[][] tuple = new byte[columns][];
        tuple[0] = connection.encodeString(connection.getCatalog());
        tuple[1] = schema;
        tuple[2] = functionName;

        if (argNames != null) {
          tuple[3] = connection.encodeString(argNames[i]);
        } else {
          tuple[3] = connection.encodeString("$" + (i + 1));
        }

        int columnMode = DatabaseMetaData.functionColumnIn;
        if (argModes != null && argModes[i] != null) {
          if (argModes[i].equals("o")) {
            columnMode = DatabaseMetaData.functionColumnOut;
          } else if (argModes[i].equals("b")) {
            columnMode = DatabaseMetaData.functionColumnInOut;
          } else if (argModes[i].equals("t")) {
            columnMode = DatabaseMetaData.functionReturn;
          }
        }

        tuple[4] = connection.encodeString(Integer.toString(columnMode));

        int argOid;
        if (allArgTypes != null) {
          argOid = allArgTypes[i].intValue();
        } else {
          argOid = argTypes.get(i).intValue();
        }

        tuple[5] =
            connection.encodeString(Integer.toString(connection.getTypeInfo().getSQLType(argOid)));
        tuple[6] = connection.encodeString(connection.getTypeInfo().getRSType(argOid));
        tuple[7] = null;
        tuple[8] = null;
        tuple[9] = null;
        tuple[10] = null;
        tuple[11] =
            connection.encodeString(Integer.toString(DatabaseMetaData.functionNullableUnknown));
        tuple[12] = null;
        tuple[14] = connection.encodeString(Integer.toString(i + 1));
        tuple[15] = isnullableUnknown;
        tuple[16] = specificName;

        v.add(new Tuple(tuple));
      }

      // if we are returning a multi-column result.
      if (returnTypeType.equals("c") || (returnTypeType.equals("p") && argModesArray != null)) {
        String columnsql = "SELECT a.attname,a.atttypid FROM pg_catalog.pg_attribute a "
            + " WHERE a.attrelid = " + returnTypeRelid
            + " AND NOT a.attisdropped AND a.attnum > 0 ORDER BY a.attnum ";
        Statement columnstmt = connection.createStatement();
        ResultSet columnrs = columnstmt.executeQuery(columnsql);
        while (columnrs.next()) {
          int columnTypeOid = (int) columnrs.getLong("atttypid");
          byte[][] tuple = new byte[columns][];
          tuple[0] = connection.encodeString(connection.getCatalog());
          tuple[1] = schema;
          tuple[2] = functionName;
          tuple[3] = columnrs.getBytes("attname");
          tuple[4] = connection
              .encodeString(Integer.toString(java.sql.DatabaseMetaData.functionColumnResult));
          tuple[5] = connection
              .encodeString(Integer.toString(connection.getTypeInfo().getSQLType(columnTypeOid)));
          tuple[6] = connection.encodeString(connection.getTypeInfo().getRSType(columnTypeOid));
          tuple[7] = null;
          tuple[8] = null;
          tuple[9] = null;
          tuple[10] = null;
          tuple[11] = connection
              .encodeString(Integer.toString(java.sql.DatabaseMetaData.functionNullableUnknown));
          tuple[12] = null;
          tuple[14] = connection.encodeString(Integer.toString(0));
          tuple[15] = isnullableUnknown;
          tuple[16] = specificName;

          v.add(new Tuple(tuple));
        }
        columnrs.close();
        columnstmt.close();
      }
    }
*/    
//    rs.close();
//    stmt.close();

//    ResultSet rc = ((BaseStatement) createMetaDataStatement()).createDriverResultSet(f, v);
    	ResultSet rc = createMetaDataStatement().executeQuery(functionColumnQuery.toString());
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "getPseudoColumns(String, String, String, String)");
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return true;
  }

  public boolean supportsSavepoints() throws SQLException {
    return true;
  }

  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    // We don't support returning generated keys by column index,
    // but that should be a rarer case than the ones we do support.
    //
    return true;
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "getSuperTypes(String,String,String)");
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "getSuperTables(String,String,String,String)");
  }

  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "getAttributes(String,String,String,String)");
  }

  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return true;
  }

  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return connection.getServerMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return connection.getServerMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() {
    return com.amazon.redshift.util.DriverInfo.JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() {
    return com.amazon.redshift.util.DriverInfo.JDBC_MINOR_VERSION;
  }

  public int getSQLStateType() throws SQLException {
    return sqlStateSQL;
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    /*
     * Currently LOB's aren't updateable at all, so it doesn't matter what we return. We don't throw
     * the notImplemented Exception because the 1.5 JDK's CachedRowSet calls this method regardless
     * of whether large objects are used.
     */
    return true;
  }

  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }
  
  /**
   * Helper method to determine if there is a possible external schema pattern match.
   *
   * @throws SQLException   If an error occurs.
   */
  private int getExtSchemaPatternMatch(String schemaPattern)
      throws SQLException
  {
      if (null != schemaPattern && !schemaPattern.equals(""))
      {
      	if (isSingleDatabaseMetaData()) {
	      	String sql = "select 1 from svv_external_schemas where schemaname like " 
	      									+ escapeQuotes(schemaPattern); 
	      	Statement stmt = null;
	        ResultSet rs = null;
	
	      	try {
		        stmt = connection.createStatement();
		        rs = stmt.executeQuery(sql);
		        if (rs.next()) {
		        	return EXTERNAL_SCHEMA_QUERY; // Optimized query
		        }
		        else
		        	return LOCAL_SCHEMA_QUERY; // Only local schema
	      	}
	      	finally {
	      		if (rs != null)
	      			rs.close();
	      		if (stmt != null) 
	      			stmt.close();
	      	}
      	}
      	else {
      		// Datashare or cross-db support always go through
      		// svv_all* view.
          return NO_SCHEMA_UNIVERSAL_QUERY; // Query both external and local schema
      	}
      }
      else
      {
          // If the schema filter is null or empty, treat it as if there was a
          // matching schema found.
          return NO_SCHEMA_UNIVERSAL_QUERY; // Query both external and local schema
      }
  }
  
  private boolean isSingleDatabaseMetaData() {
  	return (isDatabaseMetadataCurrentDbOnly()
  						|| !isMultiDatabasesCatalogEnableInServer());
  }
  private boolean isDatabaseMetadataCurrentDbOnly() {
  	return connection.isDatabaseMetadataCurrentDbOnly();
  }
  
  private boolean isMultiDatabasesCatalogEnableInServer() {
  	return connection.getQueryExecutor().isDatashareEnabled();
  }

  private String getCatalogFilterCondition(String catalog) throws SQLException {
  	return getCatalogFilterCondition(catalog, true, null);
  }

  private String getCatalogFilterCondition(String catalog, boolean apiSupportedOnlyForConnectedDatabase, String databaseColName) throws SQLException {
  	String catalogFilter = "";
    if (catalog != null && !catalog.isEmpty()) {
		  if (isSingleDatabaseMetaData()
		  		 || apiSupportedOnlyForConnectedDatabase) {
		    	// Catalog parameter is not a pattern.
		    	catalogFilter = " AND current_database() = " + escapeOnlyQuotes(catalog);
		    }
		  else {
		  	if (databaseColName == null)
		  		databaseColName = "database_name";
		  	
	    	catalogFilter = " AND " + databaseColName + " = " + escapeOnlyQuotes(catalog);
		  }
    }
    
	  return catalogFilter;
  }
}
