package com.amazon.redshift;

import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;

public interface INativePlugin 
{
    void addParameter(String key, String value);
    void setLogger(RedshiftLogger log);
    String getPluginSpecificCacheKey();
    String getIdpToken() throws RedshiftException;
    String getCacheKey();
    
    NativeTokenHolder getCredentials() throws RedshiftException;  
    void refresh() throws RedshiftException;
}

