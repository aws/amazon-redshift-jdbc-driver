package com.amazon.redshift.plugin.utils;

import com.amazon.redshift.core.Utils;
import com.amazon.redshift.plugin.InternalPluginException;

/**
 * All for plugin parameters check.
 */
public class CheckUtils
{
    private CheckUtils()
    {
    }

    public static void checkMissingAndThrows(String parameter, String parameterName)
        throws InternalPluginException
    {
        if (Utils.isNullOrEmpty(parameter))
        {
            throw new InternalPluginException("Missing required property: " + parameterName);
        }
    }

    public static void checkInvalidAndThrows(boolean condition, String parameterName)
        throws InternalPluginException
    {
        if (condition)
        {
            throw new InternalPluginException("Invalid property value: " + parameterName);
        }
    }

    public static void checkAndThrowsWithMessage(boolean condition, String message)
        throws InternalPluginException
    {
        if (condition)
        {
            throw new InternalPluginException(message);
        }
    }
}
