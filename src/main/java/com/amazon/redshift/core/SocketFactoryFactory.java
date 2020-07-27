/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.ssl.LibPQFactory;
import com.amazon.redshift.ssl.NonValidatingFactory;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.ObjectFactory;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Properties;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

/**
 * Instantiates {@link SocketFactory} based on the {@link RedshiftProperty#SOCKET_FACTORY}.
 */
public class SocketFactoryFactory {

  /**
   * Instantiates {@link SocketFactory} based on the {@link RedshiftProperty#SOCKET_FACTORY}.
   *
   * @param info connection properties
   * @return socket factory
   * @throws RedshiftException if something goes wrong
   */
  public static SocketFactory getSocketFactory(Properties info) throws RedshiftException {
    // Socket factory
    String socketFactoryClassName = RedshiftProperty.SOCKET_FACTORY.get(info);
    if (socketFactoryClassName == null) {
      return SocketFactory.getDefault();
    }
    try {
      return (SocketFactory) ObjectFactory.instantiate(socketFactoryClassName, info, true,
          RedshiftProperty.SOCKET_FACTORY_ARG.get(info));
    } catch (Exception e) {
      throw new RedshiftException(
          GT.tr("The SocketFactory class provided {0} could not be instantiated.",
              socketFactoryClassName),
          RedshiftState.CONNECTION_FAILURE, e);
    }
  }

  /**
   * Instantiates {@link SSLSocketFactory} based on the {@link RedshiftProperty#SSL_FACTORY}.
   *
   * @param info connection properties
   * @return SSL socket factory
   * @throws RedshiftException if something goes wrong
   */
  public static SSLSocketFactory getSslSocketFactory(Properties info) throws RedshiftException {
    String classname = RedshiftProperty.SSL_FACTORY.get(info);
    if (classname == null
        || "com.amazon.redshift.ssl.jdbc4.LibPQFactory".equals(classname)
        || "com.amazon.redshift.ssl.LibPQFactory".equals(classname)) {
      return new LibPQFactory(info);
    }
    try {
      if (classname.equals(RedshiftConnectionImpl.NON_VALIDATING_SSL_FACTORY))
      		classname = NonValidatingFactory.class.getName();
    	
      return (SSLSocketFactory) ObjectFactory.instantiate(classname, info, true,
          RedshiftProperty.SSL_FACTORY_ARG.get(info));
    } catch (Exception e) {
      throw new RedshiftException(
          GT.tr("The SSLSocketFactory class provided {0} could not be instantiated.", classname),
          RedshiftState.CONNECTION_FAILURE, e);
    }
  }

}
