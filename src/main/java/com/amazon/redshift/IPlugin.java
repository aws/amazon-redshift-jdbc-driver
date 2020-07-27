package com.amazon.redshift;

import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.auth.AWSCredentialsProvider;

public interface IPlugin extends AWSCredentialsProvider
{
    void addParameter(String key, String value);
    void setLogger(RedshiftLogger log);
}

