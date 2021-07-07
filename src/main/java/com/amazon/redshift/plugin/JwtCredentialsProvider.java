package com.amazon.redshift.plugin;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithWebIdentityRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithWebIdentityResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.util.StringUtils;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import com.amazon.redshift.CredentialsHolder;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.CredentialsHolder.IamMetadata;
import com.amazon.redshift.core.IamHelper;
import com.amazon.redshift.httpclient.log.IamCustomLogFactory;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.LogFactory;


public abstract class JwtCredentialsProvider implements IPlugin
{
  	private static final String KEY_ROLE_ARN = "roleArn";
  	private static final String KEY_WEB_IDENTITY_TOKEN = "webIdentityToken";
    private static final String KEY_DURATION = "duration";
  	private static final String KEY_ROLE_SESSION_NAME = "roleSessionName";

    private static final String DEFAULT_ROLE_SESSION_NAME = "jwt_redshift_session";
    
    // Mandatory parameters
    protected String m_roleArn;
    protected String m_jwt;
    
    // Optional parameters
    protected String m_roleSessionName = DEFAULT_ROLE_SESSION_NAME;
    protected int m_duration;
    
    protected String m_dbUser;
/*    protected String m_dbGroups;
    protected String m_dbGroupsFilter;
    protected Boolean m_forceLowercase;
    protected Boolean m_autoCreate;
*/    
    protected String m_stsEndpoint;
    protected String m_region;
    protected RedshiftLogger m_log;
    protected Boolean m_disableCache = false;
    protected Boolean m_groupFederation = false;

    private static Map<String, CredentialsHolder> m_cache = new HashMap<String, CredentialsHolder>();
    private CredentialsHolder m_lastRefreshCredentials; // Used when cache is disable.

    /**
     * The custom log factory class.
     */
    private static final Class<?> CUSTOM_LOG_FACTORY_CLASS = IamCustomLogFactory.class;

    /**
     * Log properties file name.
     */
    private static final String LOG_PROPERTIES_FILE_NAME = "log-factory.properties";

    /**
     * Log properties file path.
     */
    private static final String LOG_PROPERTIES_FILE_PATH = "META-INF/services/org.apache.commons.logging.LogFactory";

    /**
     * A custom context class loader which allows us to control which LogFactory is loaded.
     * Our CUSTOM_LOG_FACTORY_CLASS will divert any wire logging to NoOpLogger to suppress wire
     * messages being logged.
     */
    private static final ClassLoader CONTEXT_CLASS_LOADER = new ClassLoader(
    		JwtCredentialsProvider.class.getClassLoader())
    {
        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException
        {
            Class<?> clazz = getParent().loadClass(name);
            if (org.apache.commons.logging.LogFactory.class.isAssignableFrom(clazz))
            {
                return CUSTOM_LOG_FACTORY_CLASS;
            }
            return clazz;
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException
        {
            if (LogFactory.FACTORY_PROPERTIES.equals(name))
            {
                // make sure not load any other commons-logging.properties files
                return Collections.enumeration(Collections.<URL>emptyList());
            }
            return super.getResources(name);
        }

        @Override
        public URL getResource(String name)
        {
            if (LOG_PROPERTIES_FILE_PATH.equals(name))
            {
                return JwtCredentialsProvider.class.getResource(LOG_PROPERTIES_FILE_NAME);
            }
            return super.getResource(name);
        }
    };

    // If IDP required to look into JWT then decode it and
    // get any custom claim/tag in it.
    protected abstract String processJwt(String jwt) throws IOException;

    @Override
    public void addParameter(String key, String value)
    {
	      if (RedshiftLogger.isEnable())
	    		m_log.logDebug("key: {0}", key);
    	
        if (KEY_ROLE_ARN.equalsIgnoreCase(key))
        {
            m_roleArn = value;
        }
        else if (KEY_WEB_IDENTITY_TOKEN.equalsIgnoreCase(key))
        {
            m_jwt = value;
        }
        else if (KEY_ROLE_SESSION_NAME.equalsIgnoreCase(key))
        {
            m_roleSessionName = value;
        }
        else if (KEY_DURATION.equalsIgnoreCase(key))
        {
            m_duration = Integer.parseInt(value);
        }
        else if (RedshiftProperty.DB_USER.getName().equalsIgnoreCase(key))
        {
        	// Do not read dbUser from connection, as it derives from token.
          // m_dbUser = value;
        }
/*        else if (RedshiftProperty.DB_GROUPS.getName().equalsIgnoreCase(key))
        {
            m_dbGroups = value;
        }
        else if (RedshiftProperty.DB_GROUPS_FILTER.getName().equalsIgnoreCase(key))
        {
            m_dbGroupsFilter = value;
        }
        else if (RedshiftProperty.FORCE_LOWERCASE.getName().equalsIgnoreCase(key))
        {
            m_forceLowercase = Boolean.valueOf(value);
        }
        else if (RedshiftProperty.USER_AUTOCREATE.getName().equalsIgnoreCase(key))
        {
            m_autoCreate = Boolean.valueOf(value);
        }
*/        
        else if (RedshiftProperty.AWS_REGION.getName().equalsIgnoreCase(key))
        {
            m_region = value;
        }
        else if (RedshiftProperty.STS_ENDPOINT_URL.getName().equalsIgnoreCase(key))
        {
            m_stsEndpoint = value;
        }
        else if (RedshiftProperty.IAM_DISABLE_CACHE.getName().equalsIgnoreCase(key))
        {
            m_disableCache = Boolean.valueOf(value);
        }
    }

    @Override
    public void setLogger(RedshiftLogger log)
    {
        m_log = log;
    }
    
    @Override
    public int getSubType()
    {
        return IamHelper.JWT_PLUGIN;
    }

    @Override
    public CredentialsHolder getCredentials()
    {
    		CredentialsHolder credentials = null;
    		
  			if(!m_disableCache) {
	        String key = getCacheKey();
	        credentials = m_cache.get(key);
  			}
  			
        if (credentials == null || credentials.isExpired())
        {
          if(RedshiftLogger.isEnable()) 
            m_log.logInfo("JWT getCredentials NOT from cache");
        	
          synchronized(this) {
          	
          	refresh();
          	
          	if(m_disableCache) {
          		credentials = m_lastRefreshCredentials;
          		m_lastRefreshCredentials = null;
          	}
          }
        }
        else {
          credentials.setRefresh(false);
          if(RedshiftLogger.isEnable()) 
            m_log.logInfo("SAML getCredentials from cache");
        }

  			if(!m_disableCache) {
  				credentials = m_cache.get(getCacheKey());
  			}
        
        // if dbUser argument has been passed in the connection string, add it to metadata.
/*        if (!StringUtils.isNullOrEmpty(m_dbUser))
        {
            credentials.getThisMetadata().setDbUser(this.m_dbUser);
        } */

        if (credentials == null)
        {
            throw new SdkClientException("Unable to load AWS credentials from ADFS");
        }
        
        return credentials;
    }

    @Override
    public void refresh()
    {
        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();

        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try
        {
            String jwt = processJwt(m_jwt);

            if (RedshiftLogger.isEnable())
          		m_log.logDebug(
                  String.format("JWT : %s", jwt));
            
            String[] decodedjwt = decodeJwt(m_jwt);
            
            m_dbUser = deriveDatabaseUser(decodedjwt);

            AssumeRoleWithWebIdentityRequest jwtRequest = new AssumeRoleWithWebIdentityRequest();
            jwtRequest.setWebIdentityToken(jwt);
            jwtRequest.setRoleArn(m_roleArn);
            jwtRequest.setRoleSessionName(m_roleSessionName);
            if (m_duration > 0)
            {
            	jwtRequest.setDurationSeconds(m_duration);
            }

            AWSCredentialsProvider p = new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
            AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard();
            
            AWSSecurityTokenService stsSvc =
            		RequestUtils.buildSts(m_stsEndpoint, m_region, builder, p, m_log);
            AssumeRoleWithWebIdentityResult result = stsSvc.assumeRoleWithWebIdentity(jwtRequest);
            Credentials cred = result.getCredentials();
            Date expiration = cred.getExpiration();
            AWSCredentials c = new BasicSessionCredentials(cred.getAccessKeyId(),
                    cred.getSecretAccessKey(), cred.getSessionToken());
            CredentialsHolder credentials = CredentialsHolder.newInstance(c, expiration);
            credentials.setMetadata(readMetadata());
            credentials.setRefresh(true);
            
            if(!m_disableCache)
            	m_cache.put(getCacheKey(), credentials);
            else
              m_lastRefreshCredentials = credentials;
        } 
        catch (Exception e)
        {
          if (RedshiftLogger.isEnable())
        		m_log.logError(e);
        	
          throw new SdkClientException("JWT error: " + e.getMessage(), e);
        }
        finally
        {
          currentThread.setContextClassLoader(cl);
        }
    }

    @Override
    public String getPluginSpecificCacheKey() {
    	// Override this in each derived plugin.
    	return "";
    }
    
    @Override
    public String getIdpToken() {
    	
    	String jwt = null;
    	
      // Get the current thread and set the context loader with our custom load class method.
      Thread currentThread = Thread.currentThread();
      ClassLoader cl = currentThread.getContextClassLoader();

      Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

      try
      {
        jwt = processJwt(m_jwt);

        if (RedshiftLogger.isEnable())
      		m_log.logDebug(
              String.format("JWT : %s", jwt));
      }
      catch (Exception e)
      {
        if (RedshiftLogger.isEnable())
      		m_log.logError(e);
      	
        throw new SdkClientException("JWT error: " + e.getMessage(), e);
      }
      finally
      {
        currentThread.setContextClassLoader(cl);
      }
      
      return jwt;
    	
    }
    
    @Override
    public void setGroupFederation(boolean groupFederation) {
    	m_groupFederation = groupFederation;
    }
    
    @Override
    public String getCacheKey()
    {
  		String pluginSpecificKey = getPluginSpecificCacheKey();
    	
      return  m_roleArn + m_jwt +  m_roleSessionName + m_duration + pluginSpecificKey;
    }

    protected void checkRequiredParameters() throws IOException
    {
        if (StringUtils.isNullOrEmpty(m_roleArn))
        {
            throw new IOException("Missing required property: " + KEY_ROLE_ARN);
        }
        if (StringUtils.isNullOrEmpty(m_jwt))
        {
            throw new IOException("Missing required property: " + KEY_WEB_IDENTITY_TOKEN);
        }
    }
    
    protected String[] decodeJwt(String jwt) {
    	if (jwt == null)
    		return null;
    	
    	// Base64(JOSE header).Base64(Payload).Base64(Signature)
    	String[] headerPayloadSig = jwt.split("\\.");
    	
    	if (headerPayloadSig.length == 3) {
	    	String header = new String(Base64.decodeBase64(headerPayloadSig[0]));
	    	String payload = new String(Base64.decodeBase64(headerPayloadSig[1]));
	    	String signature = headerPayloadSig[2];
	    	
        if (RedshiftLogger.isEnable())
      		m_log.logDebug(
              String.format("Decoded JWT : Header: %s payload: %s signature:%s", header, payload, signature));
	    	
	    	return new String[] {header, payload, signature};
    	}
    	else
    		return null;
    	
    }
    
    protected String deriveDatabaseUser(String[] decodedJwt) {
      String databaseUser = null;
      
    	if (decodedJwt != null && decodedJwt.length == 3) {
    		// Use payload
    		String payload = decodedJwt[1];
    		String[] claims = { "DbUser", "upn", "preferred_username", "email" };
    		
        JsonNode entityJson = Jackson.jsonNodeOf(payload);
        JsonNode userTokenField;
    		
    		for(String claim : claims) {
    			userTokenField = entityJson.findValue(claim);
    			if (userTokenField != null) {
    				databaseUser = userTokenField.textValue();
    				if (!StringUtils.isNullOrEmpty(databaseUser)) {
    					
    	        if (RedshiftLogger.isEnable())
    	      		m_log.logDebug(
    	              String.format("JWT claim: %s as database user: %s", claim, databaseUser));
    					
    					break;
    				}
    			}
    		} // Loop
    		
				if (StringUtils.isNullOrEmpty(databaseUser)) {
	        throw new SdkClientException("No database user claim found in JWT");
				}
				
	    	return databaseUser;
    	}
    	else {
        throw new SdkClientException("JWT decoding error");
    	}
    }
    
    private IamMetadata readMetadata() 
    {
        IamMetadata metadata = new IamMetadata();
        
        metadata.setDbUser(m_dbUser);
        metadata.setAutoCreate(true);
        
        return metadata;
    }
}
