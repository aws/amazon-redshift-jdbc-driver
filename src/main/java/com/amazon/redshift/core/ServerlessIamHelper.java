package com.amazon.redshift.core;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazon.redshift.core.IamHelper.CredentialProviderType;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.redshiftarcadiainternal.RedshiftArcadiaInternalClient;
import com.amazonaws.services.redshiftarcadiainternal.RedshiftArcadiaInternalClientBuilder;
import com.amazonaws.services.redshiftarcadiainternal.model.DescribeConfigurationRequest;
import com.amazonaws.services.redshiftarcadiainternal.model.DescribeConfigurationResult;
import com.amazonaws.services.redshiftarcadiainternal.model.Endpoint;
import com.amazonaws.services.redshiftarcadiainternal.model.GetCredentialsRequest;
import com.amazonaws.services.redshiftarcadiainternal.model.GetCredentialsResult;

// In Serverless there is no V2 API.
// If user specify group_federation with serverless,
// it will call Provision V2 API.
public final class ServerlessIamHelper {
	
	private RedshiftLogger log;
	private RedshiftArcadiaInternalClient client;
	
	private static Map<String, GetCredentialsResult> credentialsCache = new HashMap<String, GetCredentialsResult>();
	
	ServerlessIamHelper(RedshiftJDBCSettings settings,
											RedshiftLogger log,
											AWSCredentialsProvider credProvider) {
		
		this.log = log;
		RedshiftArcadiaInternalClientBuilder builder = RedshiftArcadiaInternalClientBuilder.standard();
		
		builder = (RedshiftArcadiaInternalClientBuilder) IamHelper.setBuilderConfiguration(settings, log, builder);		
		
		client = (RedshiftArcadiaInternalClient) builder.withCredentials(credProvider).build();
	}
	
	synchronized void describeConfiguration(RedshiftJDBCSettings settings) {
		DescribeConfigurationRequest req = new DescribeConfigurationRequest();
		DescribeConfigurationResult  resp = client.describeConfiguration(req);
		
		Endpoint endpoint = resp.getEndpoint();	
		
    if (null == endpoint)
    {
        throw new AmazonClientException("Serverless endpoint is not available yet.");
    }
		
    settings.m_host = endpoint.getAddress();
    settings.m_port = endpoint.getPort();
	}
	
	synchronized void getCredentialsResult(RedshiftJDBCSettings settings,
									CredentialProviderType providerType,
									boolean idpCredentialsRefresh
			) throws AmazonClientException {
    String key = null;
    GetCredentialsResult credentials = null;
    		
    if(!settings.m_iamDisableCache) {
      key = IamHelper.getCredentialsCacheKey(settings, providerType, true);
      credentials = credentialsCache.get(key);
    }

    if (credentials == null
    			|| (providerType == CredentialProviderType.PLUGIN
    						&& idpCredentialsRefresh)
    			|| credentials.getExpiration().before(new Date(System.currentTimeMillis() - 60 * 1000 * 5)))
    {
        if (RedshiftLogger.isEnable())
          log.logInfo("GetCredentials NOT from cache");
    	
        if(!settings.m_iamDisableCache)	          
        	credentialsCache.remove(key);
        
        GetCredentialsRequest request = new GetCredentialsRequest();
        if (settings.m_iamDuration > 0)
        {
            request.setDurationSeconds(settings.m_iamDuration);
        }

        request.setDbName(settings.m_Schema);
//        request.setDbUser(settings.m_dbUser == null ? settings.m_username : settings.m_dbUser);
//        request.setAutoCreate(settings.m_autocreate);
//        request.setDbGroups(settings.m_dbGroups);

        if (RedshiftLogger.isEnable()) {
            log.logInfo(request.toString());
//            log.logInfo(" settings.m_dbUser=" + settings.m_dbUser
//                + " settings.m_username=" + settings.m_username);  
        }

        for (int i = 0; i < IamHelper.MAX_AMAZONCLIENT_RETRY; ++i)
        {
            try
            {
                credentials = client.getCredentials(request);
                break;
            } 
            catch (AmazonClientException ace)
            {
    					IamHelper.checkForApiCallRateExceedError(ace, i, "getCredentialsResult", log);
              continue;
            }
        }

        if(!settings.m_iamDisableCache)
        	credentialsCache.put(key, credentials);
    }
    else {
      if (RedshiftLogger.isEnable())
        log.logInfo("GetCredentials from cache");
    }
    
    settings.m_username = credentials.getDbUser();
    settings.m_password = credentials.getDbPassword();
    
    if(RedshiftLogger.isEnable()) {
        Date now = new Date();
        log.logInfo(now + ": Using GetCredentialsResult with expiration " + credentials.getExpiration());
        log.logInfo(now + ": Using GetCredentialsResultV2 with TimeToRefresh " + credentials.getNextRefreshTime());
    }
	}
}
