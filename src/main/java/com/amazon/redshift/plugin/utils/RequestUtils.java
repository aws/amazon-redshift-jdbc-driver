package com.amazon.redshift.plugin.utils;

import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import java.net.URL;
import java.util.Date;

/**
 * Http Request utils.
 */
public class RequestUtils
{

    private RequestUtils()
    {
    }

    public static AWSSecurityTokenService buildSts(String stsEndpoint, 
    																						String region,
    																						AWSSecurityTokenServiceClientBuilder builder,
    																						AWSCredentialsProvider p,
    																						RedshiftLogger log) 
    																					throws Exception {
	    
	    AWSSecurityTokenService stsSvc;
	    ClientConfiguration clientConfig = getProxyClientConfig(log);
	    
	    if (clientConfig != null) {
	    	builder.setClientConfiguration(clientConfig);
	    }
	    
	    if (isCustomStsEndpointUrl(stsEndpoint)) {
	    	EndpointConfiguration endpointConfiguration = new EndpointConfiguration(stsEndpoint, null);
	    	stsSvc = builder
	    						.withCredentials(p)
	    						.withEndpointConfiguration(endpointConfiguration)
	    						.build();
	    }
	    else {
	    	builder.setRegion(region);
	    	stsSvc = builder.withCredentials(p).build();
	    }
	    
	    return stsSvc;
    }
    
    public static ClientConfiguration getProxyClientConfig(RedshiftLogger log) {
	    boolean useProxy = false;
	    ClientConfiguration clientConfig = null;
	    
	    try {
	    	String useProxyStr = System.getProperty("http.useProxy");
	    	
	    	if(useProxyStr != null) {
	    		useProxy = Boolean.parseBoolean(useProxyStr);
	    	}
	    }
	    catch(Exception ex) {
	    	// Ignore
        if (RedshiftLogger.isEnable())
      		log.logError(ex);
	    }
	    
	    if (useProxy) {
	      clientConfig = new ClientConfiguration();
	    	String proxyHost = System.getProperty("https.proxyHost");
	    	String proxyPort = System.getProperty("https.proxyPort");
	    	String nonProxyHosts = System.getProperty("http.nonProxyHosts");
	    	
	    	if (proxyHost != null)
	    		clientConfig.setProxyHost(proxyHost);

	    	if (nonProxyHosts != null)
	    		clientConfig.setNonProxyHosts(nonProxyHosts);
	    	
	    	if (proxyPort != null)
	    		clientConfig.setProxyPort(Integer.parseInt(proxyPort));
	      
        if (RedshiftLogger.isEnable())
    			log.logDebug(
            String.format("useProxy: %s proxyHost: %s proxyPort:%s nonProxyHosts:%s" , 
            								useProxy, proxyHost, proxyPort, nonProxyHosts));
	    }
	    else {
        if (RedshiftLogger.isEnable())
      			log.logDebug(
              String.format("useProxy: %s", useProxy));
	    }
	    
	    return clientConfig;
    }
    
    private static boolean isCustomStsEndpointUrl(String stsEndpoint) throws Exception {
    	boolean isCustomStsEndPoint = false;
      if(stsEndpoint != null
      		&& !stsEndpoint.isEmpty()) {
      	
      	URL aUrl = new URL(stsEndpoint);
      	String protocol = aUrl.getProtocol();
      	if(protocol != null
      			&& protocol.equals("https")) {
      		isCustomStsEndPoint = true;
      	}
      	else {
      		throw new Exception("Only https STS URL is supported:" + stsEndpoint);
      	}
      }
      	
      return isCustomStsEndPoint;
    }

	/*
	 * Checks expiry for credential.
	 * Note that this method returns true (i.e. credential is "expired") 1 minute before actual expiry time - This
	 * arbitrary buffer has been added to accommodate corner cases and allow enough time for retries if implemented.
	 *
	 * Returns true (i.e. credential is "expired") if expiry time is null.
	 */
	public static boolean isCredentialExpired(Date expiryTime) {
		// We preemptively conclude the credential as expired 1 minute before actual expiry.
		return expiryTime==null || expiryTime.before(new Date(System.currentTimeMillis() + 1000 * 60));
	}
}
