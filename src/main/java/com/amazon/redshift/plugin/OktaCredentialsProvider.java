package com.amazon.redshift.plugin;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.SdkClientException;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

public class OktaCredentialsProvider extends SamlCredentialsProvider
{
    private static final String KEY_APP_URL = "app_id";
    private static final String KEY_APP_NAME = "app_name";

    protected String m_app_id;
    protected String m_app_name;

    @Override
    public void addParameter(String key, String value)
    {
        super.addParameter(key, value);
        if (KEY_APP_URL.equalsIgnoreCase(key))
        {
            m_app_id = value;
        }
        if (KEY_APP_NAME.equalsIgnoreCase(key))
        {
            m_app_name = value;
        }
    }
    
    @Override
    public String getPluginSpecificCacheKey() {
    	return ((m_app_id != null) ? m_app_id : "")
    					+ ((m_app_name != null) ? m_app_name : "")
    					;
    }
    

    @Override
    protected String getSamlAssertion() throws IOException
    {
        checkRequiredParameters();
        if (StringUtils.isNullOrEmpty(m_app_id))
        {
            throw new IOException("Missing required property: " + KEY_APP_URL);
        }

        CloseableHttpClient httpClient = null;

        try
        {
            httpClient = getHttpClient();
            String strOktaSessionToken = oktaAuthentication(httpClient);
            return handleSamlAssertion(httpClient, strOktaSessionToken);
        }
        catch (GeneralSecurityException e)
        {
            throw new SdkClientException("Failed create SSLContext.", e);
        }
        finally
        {
            IOUtils.closeQuietly(httpClient, null);
        }
    }

    /**
     * Authenticates users credentials via Okta, return Okta session token.
     */
    private String oktaAuthentication(CloseableHttpClient httpClient) throws IOException
    {
        CloseableHttpResponse responseAuthenticate = null;
        try
        {
            ObjectMapper mapper = new ObjectMapper();

            //HTTP Post request to Okta API for session token
            String uri = "https://" + m_idpHost + "/api/v1/authn";
            
            if (RedshiftLogger.isEnable())
          		m_log.logDebug("uri: {0}", uri);
            
            validateURL(uri);
            
            HttpPost httpost = new HttpPost(uri);
            httpost.addHeader("Accept", "application/json");
            httpost.addHeader("Content-Type", "application/json");
            httpost.addHeader("Cache-Control", "no-cache");
            //construction of JSON request
            Map<String,String> creds = new HashMap<String,String>();
            creds.put("username", m_userName);
            creds.put("password", m_password);
            StringWriter writer = new StringWriter();
            mapper.writeValue(writer, creds);
            StringEntity entity = new StringEntity(writer.toString(), "UTF-8");
            entity.setContentType("application/json");
            httpost.setEntity(entity);
            responseAuthenticate = httpClient.execute(httpost);
            String content = EntityUtils.toString(responseAuthenticate.getEntity());

          	if(RedshiftLogger.isEnable())
          		m_log.log(LogLevel.DEBUG, "oktaAuthentication https response:" + content);
            
            StatusLine statusLine = responseAuthenticate.getStatusLine();
            int requestStatus = statusLine.getStatusCode();
            
            if (requestStatus != 200)
            {
                throw new IOException(statusLine.getReasonPhrase());
            }
            //Retrieve and parse the Okta response for session token
            JsonNode json = mapper.readTree(content);

            if ("SUCCESS".equals(json.get("status").asText()))
            {
                return json.get("sessionToken").asText();
            }
            throw new IOException("No session token in the response.");
        }
        finally
        {
            IOUtils.closeQuietly(responseAuthenticate, null);
        }
    }

    /**
     * Retrieves SAML assertion from Okta containing AWS roles.
     */
    private String handleSamlAssertion(CloseableHttpClient httpClient, String oktaSessionToken) throws IOException
    {
        // If no value was specified for m_app_name, use the current default.
        if (StringUtils.isNullOrEmpty(m_app_name))
        {
            m_app_name = "amazon_aws";
        }
        else
        {
            // Ensure that the string is properly encoded.
            m_app_name = URLEncoder.encode(m_app_name, "UTF-8");
        }
        
        String oktaAWSAppUrl = "https://" + m_idpHost + "/home/" + m_app_name + "/" + m_app_id;
        String oktaAWSAppUrlWithToken = oktaAWSAppUrl + "?onetimetoken=" + oktaSessionToken;
        
        if (RedshiftLogger.isEnable())
      		m_log.logDebug("oktaAWSAppUrl: {0}", oktaAWSAppUrl);
        
        validateURL(oktaAWSAppUrlWithToken);
        HttpGet httpget = new HttpGet(oktaAWSAppUrlWithToken);
        CloseableHttpResponse responseSAML = httpClient.execute(httpget);

        int requestStatus = responseSAML.getStatusLine().getStatusCode();
        if (requestStatus != 200)
        {
            throw new RuntimeException("Failed : HTTP error code : " + responseSAML.getStatusLine().getStatusCode()
                + " : Reason : " + responseSAML.getStatusLine().getReasonPhrase());
        }

        String body = EntityUtils.toString(responseSAML.getEntity());
        
        if (RedshiftLogger.isEnable())
      		m_log.logDebug("body: {0}", body);
        
        for (String inputTags : getInputTagsfromHTML(body)) {
            String name = getValueByKey(inputTags, "name");
            String value = getValueByKey(inputTags, "value");
            
            if (RedshiftLogger.isEnable())
          		m_log.logDebug("name: {0}", name);
            
            if ("SAMLResponse".equalsIgnoreCase(name))
            {
                return value.replace("&#x2b;", "+").replace("&#x3d;", "=");
            }
        }
        throw new IOException("Failed to retrieve SAMLAssertion.");
    }
}
