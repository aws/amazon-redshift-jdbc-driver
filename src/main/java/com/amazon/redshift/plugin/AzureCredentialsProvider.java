package com.amazon.redshift.plugin;

import com.amazonaws.SdkClientException;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to get SAML Response from Microsoft Azure using OAuth 2.0 API
 */
public class AzureCredentialsProvider extends SamlCredentialsProvider
{
    /**
     * String containing "idp_tenant" as a parameter key.
     */
    private static final String KEY_IDP_TENANT = "idp_tenant";

    /**
     * String containing "client_secret" as a parameter key.
     */
    private static final String KEY_CLIENT_SECRET = "client_secret";

    /**
     * String containing "client_id" as a parameter key.
     */
    private static final String KEY_CLIENT_ID = "client_id";

    /**
     * The value of parameter idp_tenant.
     */
    private String m_idpTenant;

    /**
     * The value of parameter client_secret.
     */
    private String m_clientSecret;

    /**
     * The value of parameter client_id.
     */
    private String m_clientId;

    /**
     * Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
     *
     * @return Base64 encoded SAML Response string
     * @throws IOException throws error when missing required parameters or unable to access IDP host.
     */
    protected String getSamlAssertion() throws IOException
    {
        /**
         * idp_tenant, client_secret, and client_id are all required parameters to be able to authenticate with
         * Microsoft Azure.
         *
         * user and password are also required and need to be set to the username and password of the
         * Microsoft Azure account that is logging in.
         */
        if (StringUtils.isNullOrEmpty(m_idpTenant))
        {
            throw new IOException("Missing required property: " + KEY_IDP_TENANT);
        }
        else if (StringUtils.isNullOrEmpty(m_userName))
        {
            throw new IOException(
                "Missing required property: " + RedshiftProperty.UID.getName() + " or " +
                		RedshiftProperty.USER.getName());
        }
        else if (StringUtils.isNullOrEmpty(m_password))
        {
            throw new IOException(
                "Missing required property: " + RedshiftProperty.PWD.getName() + " or " +
                		RedshiftProperty.PASSWORD.getName());
        }
        else if (StringUtils.isNullOrEmpty(m_clientSecret))
        {
            throw new IOException("Missing required property: " + KEY_CLIENT_SECRET);
        }
        else if (StringUtils.isNullOrEmpty(m_clientId))
        {
            throw new IOException("Missing required property: " + KEY_CLIENT_ID);
        }

        return azureOauthBasedAuthentication();
    }

    /**
     * Overwritten method to grab the field parameters from JDBC connection string. This method calls the base class
     * addParamter method and adds to it Azure specific parameters.
     *
     * @param key parameter key passed to JDBC
     * @param value paramter value associated with the given key
     */
    @Override
    public void addParameter(String key, String value)
    {
	      if (RedshiftLogger.isEnable())
	    		m_log.logDebug("key: {0}", key);
    	
        if (KEY_IDP_TENANT.equalsIgnoreCase(key))
        {
            m_idpTenant = value;
        }
        else if (KEY_CLIENT_SECRET.equalsIgnoreCase(key))
        {
            m_clientSecret = value;
        }
        else if (KEY_CLIENT_ID.equalsIgnoreCase(key))
        {
            m_clientId = value;
        }
        else
        {
            super.addParameter(key, value);
        }
    }

    @Override
    public String getPluginSpecificCacheKey() {
    	return ((m_idpTenant != null) ? m_idpTenant : "")
    					+ ((m_clientId != null) ? m_clientId : "")
    					+ ((m_clientSecret != null) ? m_clientSecret : "")
    					;
    }
    
    /**
     * Method to initiate a POST request to grab the SAML Assertion from Microsoft Azure and convert it to a
     * SAML Response.
     *
     * @return Base64 encoded SAML Response string
     * @throws IOException
     * @throws SdkClientException
     */
    private String azureOauthBasedAuthentication() throws IOException, SdkClientException
    {
        // endpoint to connect with Microsoft Azure to get SAML Assertion token
        String uri = "https://login.microsoftonline.com/" + m_idpTenant + "/oauth2/token";

        if (RedshiftLogger.isEnable())
      		m_log.logDebug("uri: {0}", uri);
        
        CloseableHttpClient client = null;
        CloseableHttpResponse resp = null;

        try
        {
            client = getHttpClient();

            HttpPost post = new HttpPost(uri);

            // required parameters to pass in POST body
            List<NameValuePair> parameters = new ArrayList<NameValuePair>(7);
            parameters.add(new BasicNameValuePair("grant_type", "password"));
            parameters.add(
                new BasicNameValuePair(
                    "requested_token_type",
                    "urn:ietf:params:oauth:token-type:saml2"));
            parameters.add(new BasicNameValuePair("username", m_userName));
            parameters.add(new BasicNameValuePair("password", m_password));
            parameters.add(new BasicNameValuePair(KEY_CLIENT_SECRET, m_clientSecret));
            parameters.add(new BasicNameValuePair(KEY_CLIENT_ID, m_clientId));
            parameters.add(new BasicNameValuePair("resource", m_clientId));

            // headers to pass with POST request
            post.addHeader("Content-Type", "application/x-www-form-urlencoded");
            post.addHeader("Accept", "application/json");
            post.setEntity(new UrlEncodedFormEntity(parameters, Charset.forName("UTF-8")));

            resp = client.execute(post);

            String content = EntityUtils.toString(resp.getEntity());
            JsonNode entityJson = Jackson.jsonNodeOf(content);

            // if we don't receive a 200 response, throw an error saying we failed to authenticate
            // with Azure
            if (resp.getStatusLine().getStatusCode() != 200)
            {
            	if(RedshiftLogger.isEnable())
            		m_log.log(LogLevel.DEBUG, "azureOauthBasedAuthentication https response: " + content);
            	
                String errorMessage =
                    "Authentication failed on the Azure server. Please check the tenant, user, password, client secret, and client id.";
                JsonNode errorDescriptionNode = entityJson.findValue("error_description");
                if (errorDescriptionNode != null &&
                    !StringUtils.isNullOrEmpty(errorDescriptionNode.textValue()))
                {
                    String errorDescription =
                        errorDescriptionNode.textValue().replaceAll("\r\n", " ");
                    JsonNode errorCodeNode = entityJson.findValue("error");
                    if (errorCodeNode != null &&
                        !StringUtils.isNullOrEmpty(errorCodeNode.textValue()))
                    {
                        errorMessage = errorCodeNode.textValue() + ": " + errorDescription;
                    }
                    else
                    {
                        errorMessage = "Unexpected response: " + errorDescription;
                    }
                }
                throw new IOException(errorMessage);
            }
            
            if (RedshiftLogger.isEnable())
          		m_log.logDebug("content: {0}", content);
            
            // parse the JSON response to grab access_token field which contains Base64 encoded SAML
            // Assertion and decode it
            JsonNode accessTokenField = entityJson.findValue("access_token");
            String encodedSamlAssertion;
            if (accessTokenField != null)
            {
                encodedSamlAssertion = accessTokenField.textValue();
                if (StringUtils.isNullOrEmpty(encodedSamlAssertion))
                {
                    throw new IOException("Invalid Azure access_token response");
                }
            }
            else
            {
                throw new IOException("Failed to find Azure access_token");
            }

            // decode the SAML Assertion to a String to add XML tags to form a SAML Response
            String samlAssertion =
                new String(Base64.decodeBase64(encodedSamlAssertion),
                    Charset.forName("UTF-8"));

            /**
             * SAML Response is required to be sent to base class. We need to provide a minimum of:
             * 1) samlp:Response XML tag with xmlns:samlp protocol value
             * 2) samlp:Status XML tag and samlpStatusCode XML tag with Value indicating Success
             * 3) followed by Signed SAML Assertion
             */
            StringBuilder sb = new StringBuilder();
            sb.append("<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\">");
            sb.append("<samlp:Status>");
            sb.append("<samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>");
            sb.append("</samlp:Status>");
            sb.append(samlAssertion);
            sb.append("</samlp:Response>");

            // re-encode the SAML Resposne in Base64 and return this to the base class
            return new String(Base64.encodeBase64(sb.toString().getBytes()));
        }
        catch (GeneralSecurityException e)
        {
            // failed to get HttpClient and thus cannot continue so throw an error.
            throw new SdkClientException("Failed to create SSLContext", e);
        }
        finally
        {
            // close out closable resp and client. This does not throw any errors.
            IOUtils.closeQuietly(resp, null);
            IOUtils.closeQuietly(client, null);
        }
    }
}
