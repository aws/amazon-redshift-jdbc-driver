package com.amazon.redshift;

import com.amazon.redshift.logger.RedshiftLogger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public interface IPlugin extends AwsCredentialsProvider
{
    void addParameter(String key, String value);
    void setLogger(RedshiftLogger log);
    String getPluginSpecificCacheKey();
    void setGroupFederation(boolean groupFederation);
    String getIdpToken();
    String getCacheKey();
    int getSubType();
}
