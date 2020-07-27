/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.xa;

import com.amazon.redshift.ds.common.RedshiftObjectFactory;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;

/**
 * An ObjectFactory implementation for RedshiftXADataSource-objects.
 */

public class RedshiftXADataSourceFactory extends RedshiftObjectFactory {
  /*
   * All the other Redshift DataSource use RedshiftObjectFactory directly, but we can't do that with
   * RedshiftXADataSource because referencing RedshiftXADataSource from RedshiftObjectFactory would break
   * "JDBC2 Enterprise" edition build which doesn't include RedshiftXADataSource.
   */

  public Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    if (className.equals("com.amazon.redshift.xa.RedshiftXADataSource")) {
      return loadXADataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadXADataSource(Reference ref) {
    RedshiftXADataSource ds = new RedshiftXADataSource();
    return loadBaseDataSource(ds, ref);
  }
}
