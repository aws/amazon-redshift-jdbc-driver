package com.amazon.redshift.httpclient.log;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.commons.logging.impl.NoOpLog;

/**
 * This class provides an implementation of LogFactoryImpl that will prevent any http wire logging.
 * This was requested as a security measure to prevent possible interception of user names and
 * passwords when connecting with IAM.
 */
public class IamCustomLogFactory extends LogFactoryImpl
{
	/**
	 * The class to block logging for.
	 */
    private static String BANNED = "org.apache.http.wire";

    /**
     * Get the Log indicated by the class name. If trying to get wire logs, block by returning
     * new NoOpLog instance.
     *
     * @param clazz     The log class to return.
     */
    @Override
    public Log getInstance(Class clazz) throws LogConfigurationException
    {
        if (clazz.getName().equals(BANNED))
        {
            return new NoOpLog();
        }
        else
        {
            return super.getInstance(clazz);
        }
    }

    /**
     * Get the Log indicated by the name. If trying to get wire logs, block by returning
     * new NoOpLog instance.
     *
     * @param name     The name of the log class to return.
     */
    @Override
    public Log getInstance(String name) throws LogConfigurationException
    {

        if (name.equals(BANNED))
        {
            return new NoOpLog();
        }
        else
        {
            return super.getInstance(name);
        }
    }

}
