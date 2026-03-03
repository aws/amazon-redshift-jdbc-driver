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
    
    /**
     * Check if this plugin is using identity-enhanced credentials.
     * Default implementation returns false for backward compatibility.
     * @return true if using identity-enhanced credentials, false otherwise
     */
    default boolean isUsingIdentityEnhancedCredentials() {
        return false;
    }
}

