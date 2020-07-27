package com.amazon.redshift.plugin;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.SdkClientException;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import static java.lang.String.format;

public class PingCredentialsProvider extends SamlCredentialsProvider
{
    private static final Pattern SAML_PATTERN =
            Pattern.compile("SAMLResponse\\W+value=\"([^\"]+)\"");

    /**
     * Property for specifying partner SpId.
     */
    private static final String KEY_PARTNER_SPID = "partner_spid";

    /**
     * String to hold value of partner SpId.
     */
    protected String m_partnerSpId;

    @Override
    public void addParameter(String key, String value)
    {
        super.addParameter(key, value);

        if (KEY_PARTNER_SPID.equalsIgnoreCase(key))
        {
            m_partnerSpId = value;
        }
    }

    @Override
    protected String getSamlAssertion() throws IOException
    {
        checkRequiredParameters();

        // If no value was specified for m_partnerSpid use the AWS default.
        if (StringUtils.isNullOrEmpty(m_partnerSpId))
        {
            m_partnerSpId = "urn%3Aamazon%3Awebservices";
        }
        else
        {
            // Ensure that the string is properly encoded.
            m_partnerSpId = URLEncoder.encode(m_partnerSpId, "UTF-8");
        }

        String uri = "https://" +
                m_idpHost + ':' + m_idpPort +
                "/idp/startSSO.ping?PartnerSpId=" + m_partnerSpId;
        CloseableHttpClient client = null;
        List<NameValuePair> parameters = new ArrayList<NameValuePair>(5);

        try
        {
            CloseableHttpResponse resp;
            
            if (RedshiftLogger.isEnable())
          		m_log.logDebug("uri: {0}", uri);
            
            client = getHttpClient();
            HttpGet get = new HttpGet(uri);
            resp = client.execute(get);
            if (resp.getStatusLine().getStatusCode() != 200)
            {
                throw new IOException(
                        "Failed send request: " + resp.getStatusLine().getReasonPhrase());
            }
            HttpEntity entity = resp.getEntity();
            String body = EntityUtils.toString(entity);
            BasicNameValuePair username = null;
            BasicNameValuePair pass = null;
            String password_tag = null;

            if (RedshiftLogger.isEnable())
          		m_log.logDebug("body: {0}", body);
            
            for (String inputTag : getInputTagsfromHTML(body))
            {
                String name = getValueByKey(inputTag, "name");
                String id = getValueByKey(inputTag, "id");
                String value = getValueByKey(inputTag, "value");

                if (RedshiftLogger.isEnable())
              		m_log.logDebug("name: {0} , id: {1}", name, id);
                
                if (username == null
                    && "username".equals(id) && isText(inputTag)) 
                {
                	username = new BasicNameValuePair(name, m_userName);
		            } 
                else if (("pf.pass".equals(name) || name.contains("pass"))
		                    && isPassword(inputTag))
		            {
		                if (pass != null)
		                {
		                	if(RedshiftLogger.isEnable()) {
		                		m_log.log(LogLevel.DEBUG, format("pass field: %s " +
		                                    "has conflict with field: %s",
		                            password_tag, inputTag));
		                		m_log.log(LogLevel.DEBUG, body);
		                	}
		                	
		                  throw new IOException("Duplicate password fields on " +
		                            "login page.");
		                }
		                password_tag = inputTag;
		                pass = new BasicNameValuePair(name, m_password);
		            } 
                else if (!StringUtils.isNullOrEmpty(name))
		            {
		                parameters.add(new BasicNameValuePair(name, value));
		            }
            }

		        if( username == null )
		        {
		            for (String inputTag : getInputTagsfromHTML(body)) 
		            {
		                String name = getValueByKey(inputTag, "name");
		                if (("email".equals(name) || name.contains("user")
		                        || name.contains("email")) && isText(inputTag))
		                {
		                    username = new BasicNameValuePair(name, m_userName);
		                }
		            }
		        }
		        
		        if (username == null || pass == null)
		        {
		        	if(RedshiftLogger.isEnable())
		        		m_log.log(LogLevel.DEBUG, body);
		          throw new IOException("Failed to parse login form.");
		        }
		        
		        parameters.add(username);
		        parameters.add(pass);
            
            String action = getFormAction(body);
            if (!StringUtils.isNullOrEmpty(action) && action.startsWith("/"))
            {
                uri = "https://" + m_idpHost + ':' + m_idpPort + action;
            }

            if (RedshiftLogger.isEnable())
          		m_log.logDebug("action uri: {0}", uri);
            
            HttpPost post = new HttpPost(uri);
            post.setEntity(new UrlEncodedFormEntity(parameters));
            resp = client.execute(post);
            if (resp.getStatusLine().getStatusCode() != 200)
            {
                throw new IOException(
                        "Failed send request: " + resp.getStatusLine().getReasonPhrase());
            }

            String content = EntityUtils.toString(resp.getEntity());
            Matcher matcher = SAML_PATTERN.matcher(content);
            if (!matcher.find())
            {
                throw new IOException("Failed to retrieve SAMLAssertion.");
            }

            return matcher.group(1);
        }
        catch (GeneralSecurityException e)
        {
            throw new SdkClientException("Failed create SSLContext.", e);
        }
        finally
        {
            IOUtils.closeQuietly(client, null);
        }
    }
}
