/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.ds.common;

import com.amazon.redshift.ds.RedshiftConnectionPoolDataSource;
import com.amazon.redshift.ds.RedshiftPoolingDataSource;
import com.amazon.redshift.ds.RedshiftSimpleDataSource;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * Returns a DataSource-ish thing based on a JNDI reference. In the case of a SimpleDataSource or
 * ConnectionPool, a new instance is created each time, as there is no connection state to maintain.
 * In the case of a PoolingDataSource, the same DataSource will be returned for every invocation
 * within the same VM/ClassLoader, so that the state of the connections in the pool will be
 * consistent.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class RedshiftObjectFactory implements ObjectFactory {
  /**
   * Dereferences a Redshift DataSource. Other types of references are ignored.
   */
  public Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    // Old names are here for those who still use them
    if (className.equals("com.amazon.redshift.ds.RedddddddshiftSimpleDataSource")
        || className.equals("com.amazon.redshift.jdbc2.optional.SimpleDataSource")
        || className.equals("com.amazon.redshift.jdbc3.Jdbc3SimpleDataSource")) {
      return loadSimpleDataSource(ref);
    } else if (className.equals("com.amazon.redshift.ds.RedshiftConnectionPoolDataSource")
        || className.equals("com.amazon.redshift.jdbc2.optional.ConnectionPool")
        || className.equals("com.amazon.redshift.jdbc3.Jdbc3ConnectionPool")) {
      return loadConnectionPool(ref);
    } else if (className.equals("com.amazon.redshift.ds.RedshiftPoolingDataSource")
        || className.equals("com.amazon.redshift.jdbc2.optional.PoolingDataSource")
        || className.equals("com.amazon.redshift.jdbc3.Jdbc3PoolingDataSource")) {
      return loadPoolingDataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadPoolingDataSource(Reference ref) {
    // If DataSource exists, return it
    String name = getProperty(ref, "dataSourceName");
    RedshiftPoolingDataSource pds = RedshiftPoolingDataSource.getDataSource(name);
    if (pds != null) {
      return pds;
    }
    // Otherwise, create a new one
    pds = new RedshiftPoolingDataSource();
    pds.setDataSourceName(name);
    loadBaseDataSource(pds, ref);
    String min = getProperty(ref, "initialConnections");
    if (min != null) {
      pds.setInitialConnections(Integer.parseInt(min));
    }
    String max = getProperty(ref, "maxConnections");
    if (max != null) {
      pds.setMaxConnections(Integer.parseInt(max));
    }
    return pds;
  }

  private Object loadSimpleDataSource(Reference ref) {
    RedshiftSimpleDataSource ds = new RedshiftSimpleDataSource();
    return loadBaseDataSource(ds, ref);
  }

  private Object loadConnectionPool(Reference ref) {
    RedshiftConnectionPoolDataSource cp = new RedshiftConnectionPoolDataSource();
    return loadBaseDataSource(cp, ref);
  }

  protected Object loadBaseDataSource(BaseDataSource ds, Reference ref) {
    ds.setFromReference(ref);

    return ds;
  }

  protected String getProperty(Reference ref, String s) {
    RefAddr addr = ref.get(s);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

}
