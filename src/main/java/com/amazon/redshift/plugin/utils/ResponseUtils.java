package com.amazon.redshift.plugin.utils;

import org.apache.http.NameValuePair;

import java.util.List;

/**
 * Http Request/Response utils.
 */
public class ResponseUtils
{

    private ResponseUtils()
    {
    }

    /**
     * Find parameter by name in http request/response  {@link NameValuePair} List.
     * 
     * @param name name of the parameter
     * @param list list of parameters
     * @return returns value of the found parameter, otherwise null.
     */
    public static String findParameter(String name, List<NameValuePair> list)
    {
        for (NameValuePair pair : list)
        {
            if (name.equals(pair.getName()))
            {
                return pair.getValue();
            }
        }
        return null;
    }
}
