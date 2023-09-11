package com.amazon.redshift.plugin;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.INativePlugin;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.core.IamHelper;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;


public abstract class JwtCredentialsProvider extends IdpCredentialsProvider implements INativePlugin
{
    private static final String KEY_PROVIDER_NAME = "providerName";
    protected Boolean m_disableCache = false;
    
    // Optional parameters
    private String m_providerName;

    private static Map<String, NativeTokenHolder> m_cache = new HashMap<String, NativeTokenHolder>();
    private NativeTokenHolder m_lastRefreshCredentials; // Used when cache is disable.

    /**
     * The custom log factory class.
     */
//    private static final Class<?> CUSTOM_LOG_FACTORY_CLASS = JwtCredentialsProvider.class;

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
/*            if (org.apache.commons.logging.LogFactory.class.isAssignableFrom(clazz))
            {
                return CUSTOM_LOG_FACTORY_CLASS;
            } */
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

    @Override
    public void addParameter(String key, String value)
    {
	   if (RedshiftLogger.isEnable())
	    	m_log.logDebug("key: {0}", key);
    	
        if (RedshiftProperty.IAM_DISABLE_CACHE.getName().equalsIgnoreCase(key))
        {
            m_disableCache = Boolean.valueOf(value);
        }
        else
        if (KEY_PROVIDER_NAME.equalsIgnoreCase(key))
        {
            m_providerName = value;
        }
        else if (KEY_SSL_INSECURE.equalsIgnoreCase(key))
        {
            m_sslInsecure = Boolean.parseBoolean(value);
        }
    }
    
    protected boolean isNullOrEmpty(String val) {
      return (val == null || val.length() == 0);
    }

    @Override
    public void setLogger(RedshiftLogger log)
    {
        m_log = log;
    }

    @Override
    public NativeTokenHolder getCredentials() throws RedshiftException
    {
      NativeTokenHolder credentials = null;
    		
      if(!m_disableCache) 
      {
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
          m_log.logInfo("JWT getCredentials from cache");
      }

      if(!m_disableCache) {
		credentials = m_cache.get(getCacheKey());
      }
        

      if (credentials == null)
      {
          throw new RedshiftException("Unable to get IDP credentials");
      }
        
      return credentials;
    }

    protected abstract String getJwtAssertion() throws IOException;
    
    @Override
    public void refresh() throws RedshiftException
    {
        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();

        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try
        {
            String jwt = getJwtAssertion();

            if (RedshiftLogger.isEnable())
          		m_log.logDebug("JwtCredentialsProvider: refreshed JWT assertion of length={0}", jwt != null ? jwt.length() : -1);

            // Default expiration until server sends actual expirations
            Date expiration = new Date(System.currentTimeMillis() + 15 * 60 * 1000);            
            NativeTokenHolder credentials = NativeTokenHolder.newInstance(jwt, expiration);
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
        	
          throw new RedshiftException("JWT error: " + e.getMessage(), e);
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
    public String getIdpToken() throws RedshiftException {
      String jwt = null;
    	
      // Get the current thread and set the context loader with our custom load class method.
      Thread currentThread = Thread.currentThread();
      ClassLoader cl = currentThread.getContextClassLoader();

      Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

      try
      {
        jwt = getJwtAssertion();

        if (RedshiftLogger.isEnable())
      		m_log.logDebug("JwtCredentialsProvider: got JWT asssertion of length={0}", jwt != null ? jwt.length() : -1);
      }
      catch (Exception e)
      {
        if (RedshiftLogger.isEnable())
      		m_log.logError(e);
      	
        throw new RedshiftException("JWT error: " + e.getMessage(), e);
      }
      finally
      {
        currentThread.setContextClassLoader(cl);
      }
      
      return jwt;
    	
    }
    
    @Override
    public String getCacheKey()
    {
  		String pluginSpecificKey = getPluginSpecificCacheKey();
    	
        return  pluginSpecificKey;
    }
}
