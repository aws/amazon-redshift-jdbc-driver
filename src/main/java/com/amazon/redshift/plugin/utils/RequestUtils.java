package com.amazon.redshift.plugin.utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import java.net.URL;

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
    																						AWSCredentialsProvider p) 
    																					throws Exception {
	    
	    AWSSecurityTokenService stsSvc;
	    
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
}
