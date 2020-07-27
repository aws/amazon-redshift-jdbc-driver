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
     * <p>
     * During build in BuildDriver.sh:124 SimbaPackageRenamer.jar makes class packages rename.
     * This tool can`t work with JAVA8 lambdas. So instead of using stream api to search params, I have to iterate.
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
