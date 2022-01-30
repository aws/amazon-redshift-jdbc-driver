package com.amazon.redshift.plugin;

import java.io.IOException;

/**
 * A basic JWT credential provider class. This class can be changed and implemented to work with
 * any desired JWT service provider.
 */
public class BasicJwtCredentialsProvider extends JwtCredentialsProvider
{
    private static final String KEY_WEB_IDENTITY_TOKEN = "webIdentityToken";
    
    // Mandatory parameters
    private String m_jwt;
  
    /**
     * Optional default constructor.
     */
    public BasicJwtCredentialsProvider()
    {
      m_disableCache = true;
    }

    private void checkRequiredParameters() throws IOException
    {
        if (isNullOrEmpty(m_jwt))
        {
            throw new IOException("Missing required property: " + KEY_WEB_IDENTITY_TOKEN);
        }
    }
    
    @Override
    public String getPluginSpecificCacheKey() {
    	return m_jwt;
    }
    
    @Override
    public void addParameter(String key, String value)
    {
      // The parent class will take care of setting up all other connection properties which are
      // mentioned in the Redshift JDBC driver documentation.
      super.addParameter(key, value);
      
      if (KEY_WEB_IDENTITY_TOKEN.equalsIgnoreCase(key))
      {
          m_jwt = value;
      }
      
    }
    /**
     * This method needs to return the JWT string returned by the specific JWT provider
     * being used for this implementation. How you get this string will depend on the specific JWT
     * provider you are using. This method can decode jwt and process any custom claim/tag in it.
     * <p>
     * This will be used by the JwtCredentialsProvider parent class to get the temporary credentials.
     *
     * @return  The JWT string.
     * @throws IOException throws exception when required parameters are missing.
     */
    @Override
    protected String getJwtAssertion() throws IOException 
    {
       checkRequiredParameters(); 
    		
       return m_jwt;
    }
}
