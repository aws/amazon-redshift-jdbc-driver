/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides constants for well-known backend OIDs for the types we commonly use.
 */
public class Oid {
  public static final int INT2 = 21;
  public static final int INT2_ARRAY = 1005;
  public static final int INT4 = 23;
  public static final int INT4_ARRAY = 1007;
  public static final int INT8 = 20;
  public static final int INT8_ARRAY = 1016;
  public static final int TEXT = 25;
  public static final int TEXT_ARRAY = 1009;
  public static final int NUMERIC = 1700;
  public static final int NUMERIC_ARRAY = 1231;
  public static final int FLOAT4 = 700;
  public static final int FLOAT4_ARRAY = 1021;
  public static final int FLOAT8 = 701;
  public static final int FLOAT8_ARRAY = 1022;
  public static final int BOOL = 16;
  public static final int BOOL_ARRAY = 1000;
  public static final int DATE = 1082;
  public static final int DATE_ARRAY = 1182;
  public static final int TIME = 1083;
  public static final int TIME_ARRAY = 1183;
  public static final int TIMETZ = 1266;
  public static final int TIMETZ_ARRAY = 1270;
  public static final int TIMESTAMP = 1114;
  public static final int TIMESTAMP_ARRAY = 1115;
  public static final int TIMESTAMPTZ = 1184;
  public static final int TIMESTAMPTZ_ARRAY = 1185;
  public static final int BYTEA = 17;
  public static final int BYTEA_ARRAY = 1001;
  public static final int VARCHAR = 1043;
  public static final int VARCHAR_ARRAY = 1015;
  public static final int OID = 26;
  public static final int OID_ARRAY = 1028;
  public static final int BPCHAR = 1042;
  public static final int BPCHAR_ARRAY = 1014;
  public static final int MONEY = 790;
  public static final int MONEY_ARRAY = 791;
  public static final int NAME = 19;
  public static final int NAME_ARRAY = 1003;
  public static final int BIT = 1560;
  public static final int BIT_ARRAY = 1561;
  public static final int VOID = 2278;
  public static final int INTERVAL = 1186;
  public static final int INTERVAL_ARRAY = 1187;
  public static final int CHAR = 18; // This is not char(N), this is "char" a single byte type.
  public static final int CHAR_ARRAY = 1002;
  public static final int VARBIT = 1562;
  public static final int VARBIT_ARRAY = 1563;
  public static final int UUID = 2950;
  public static final int UUID_ARRAY = 2951;
  public static final int XML = 142;
  public static final int XML_ARRAY = 143;
  public static final int POINT = 600;
  public static final int POINT_ARRAY = 1017;
  public static final int BOX = 603;
  public static final int JSONB_ARRAY = 3807;
  public static final int JSON = 114;
  public static final int JSON_ARRAY = 199;
  public static final int REF_CURSOR = 1790;
  public static final int REF_CURSOR_ARRAY = 2201;
  public static final int GEOMETRY = 3000;
  public static final int GEOMETRY_ARRAY = 0; // UNSPECIFIED
  public static final int GEOMETRYHEX = 3999;
  public static final int GEOMETRYHEX_ARRAY = 0; // UNSPECIFIED
  public static final int SUPER = 4000;
  public static final int SUPER_ARRAY = 0; // UNSPECIFIED
  public static final int VARBYTE = 6551;
  public static final int VARBYTE_ARRAY = 0; // UNSPECIFIED
  public static final int TIDOID = 27; // VARCHAR
  public static final int TIDARRAYOID = 1010;

  public static final int XIDOID = 28; // INTEGER
  public static final int XIDARRAYOID = 1011;

  public static final int ACLITEM = 1033; // In Binary mode treat it as VARCHAR, as data comes from server same as VARCHAR.
  public static final int ACLITEM_ARRAY = 1034;
  
  public static final int ABSTIMEOID = 702; // validuntil col in pg_user
  public static final int ABSTIMEARRAYOID = 1023; // UNSPECIFIED

  // Keep this as last field to log correctly. As we have many UNSPECIFIED values.
  public static final int UNSPECIFIED = 0;

  private static final Map<Integer, String> OID_TO_NAME = new HashMap<Integer, String>(100);
  private static final Map<String, Integer> NAME_TO_OID = new HashMap<String, Integer>(100);

  static {
    for (Field field : Oid.class.getFields()) {
      try {
        int oid = field.getInt(null);
        String name = field.getName().toUpperCase();
        OID_TO_NAME.put(oid, name);
        NAME_TO_OID.put(name, oid);
      } catch (IllegalAccessException e) {
        // ignore
      }
    }
  }

  /**
   * Returns the name of the oid as string.
   *
   * @param oid The oid to convert to name.
   * @return The name of the oid or {@code "<unknown>"} if oid no constant for oid value has been
   *         defined.
   */
  public static String toString(int oid) {
    String name = OID_TO_NAME.get(oid);
    if (name == null) {
      name = "<unknown:" + oid + ">";
    }
    return name;
  }

  public static int valueOf(String oid) throws RedshiftException {
    if (oid.length() > 0 && !Character.isDigit(oid.charAt(0))) {
      Integer id = NAME_TO_OID.get(oid);
      if (id == null) {
        id = NAME_TO_OID.get(oid.toUpperCase());
      }
      if (id != null) {
        return id;
      }
    } else {
      try {
        // OID are unsigned 32bit integers, so Integer.parseInt is not enough
        return (int) Long.parseLong(oid);
      } catch (NumberFormatException ex) {
      }
    }
    throw new RedshiftException(GT.tr("oid type {0} not known and not a number", oid),
        RedshiftState.INVALID_PARAMETER_VALUE);
  }
}
