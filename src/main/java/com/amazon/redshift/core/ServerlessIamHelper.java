package com.amazon.redshift.core;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazon.redshift.core.IamHelper.CredentialProviderType;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.redshiftserverless.RedshiftServerlessClient;
import software.amazon.awssdk.services.redshiftserverless.RedshiftServerlessClientBuilder;
import software.amazon.awssdk.services.redshiftserverless.model.Endpoint;
import software.amazon.awssdk.services.redshiftserverless.model.GetCredentialsRequest;
import software.amazon.awssdk.services.redshiftserverless.model.GetCredentialsResponse;
import software.amazon.awssdk.services.redshiftserverless.model.GetWorkgroupRequest;
import software.amazon.awssdk.services.redshiftserverless.model.GetWorkgroupResponse;

// In Serverless there is no V2 API.
// If user specify group_federation with serverless,
// it will call Provision V2 API.
public final class ServerlessIamHelper {

    private RedshiftLogger log;
    private RedshiftServerlessClient client;

    private static Map<String, GetCredentialsResponse> credentialsCache = new HashMap<String, GetCredentialsResponse>();

    ServerlessIamHelper(RedshiftJDBCSettings settings,
                        RedshiftLogger log,
                        AwsCredentialsProvider credProvider) {
        this.log = log;
        RedshiftServerlessClientBuilder builder = RedshiftServerlessClient.builder();

        builder = (RedshiftServerlessClientBuilder) IamHelper.setBuilderConfiguration(settings, log, builder);

        client = builder.credentialsProvider(credProvider).build();
    }

    /* package */ ServerlessIamHelper(RedshiftServerlessClient client, RedshiftLogger log) {
        this.client = client;
        this.log = log;
    }

    synchronized void describeConfiguration(RedshiftJDBCSettings settings) {
        GetWorkgroupRequest.Builder requestBuilder = GetWorkgroupRequest.builder();

        if(settings.m_workGroup != null && settings.m_workGroup.length() > 0) {
            // Set workgroup in the request
            requestBuilder.workgroupName(settings.m_workGroup);
        }
        else
        {
            throw SdkClientException.create("Serverless workgroup is not set.");
        }

        GetWorkgroupResponse resp = client.getWorkgroup(requestBuilder.build());

        Endpoint endpoint = resp.workgroup().endpoint();

        if (null == endpoint)
        {
            throw SdkClientException.create("Serverless endpoint is not available yet.");
        }

        settings.m_host = endpoint.address();
        settings.m_port = endpoint.port();
    }

    synchronized void getCredentialsResult(RedshiftJDBCSettings settings,
                                           CredentialProviderType providerType,
                                           boolean idpCredentialsRefresh
    ) throws SdkClientException {
        String key = null;
        GetCredentialsResponse credentials = null;

        if (!settings.m_iamDisableCache) {
            key = IamHelper.getCredentialsCacheKey(settings, providerType, true);
            credentials = credentialsCache.get(key);
        }

        if (credentials == null
                || (providerType == CredentialProviderType.PLUGIN
                && idpCredentialsRefresh)
                || RequestUtils.isCredentialExpired(credentials.expiration()))
        {
            if (RedshiftLogger.isEnable()) {
                log.logInfo("GetCredentials NOT from cache");
            }
            if (!settings.m_iamDisableCache) {
                credentialsCache.remove(key);
            }

            GetCredentialsRequest.Builder requestBuilder = GetCredentialsRequest.builder();
            if (settings.m_iamDuration > 0) {
                requestBuilder.durationSeconds(settings.m_iamDuration);
            }

            requestBuilder.dbName(settings.m_Schema);
            if (settings.m_workGroup != null && settings.m_workGroup.length() > 0) {
                // Set workgroup in the request
                requestBuilder.workgroupName(settings.m_workGroup);
            } else {
                if (settings.m_isCname) {
                    requestBuilder.customDomainName(settings.m_host);
                }
            }

            if (RedshiftLogger.isEnable()) {
                log.logInfo(requestBuilder.toString());
            }

            for (int i = 0; i < IamHelper.MAX_AMAZONCLIENT_RETRY; ++i) {
                try {
                    credentials = client.getCredentials(requestBuilder.build());
                    break;
                }
                catch (SdkClientException sdkClientException) {
                    IamHelper.checkForApiCallRateExceedError(sdkClientException, i, "getCredentialsResult", log);
                }
            }

            if (!settings.m_iamDisableCache) {
                credentialsCache.put(key, credentials);
            }
        }
        else {
            if (RedshiftLogger.isEnable()) {
                log.logInfo("GetCredentials from cache");
            }
        }

        settings.m_username = credentials.dbUser();
        settings.m_password = credentials.dbPassword();

        if (RedshiftLogger.isEnable()) {
            Date now = new Date();
            log.logInfo(now + ": Using GetCredentialsResult with expiration " + credentials.expiration());
            log.logInfo(now + ": Using GetCredentialsResultV2 with TimeToRefresh " + credentials.nextRefreshTime());
        }
    }
}
