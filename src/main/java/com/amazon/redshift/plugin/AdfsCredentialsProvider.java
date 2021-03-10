package com.amazon.redshift.plugin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.SdkClientException;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;

public class AdfsCredentialsProvider extends SamlCredentialsProvider
{
    private static final Pattern SAML_PATTERN =
            Pattern.compile("SAMLResponse\\W+value=\"([^\"]+)\"");

    /**
     * Property for specifying loginToRp.
     */
    private static final String KEY_LOGINTORP = "loginToRp";
 
    /**
     * String to hold value of loginToRp.
     */
    protected String m_loginToRp = "urn:amazon:webservices";
 
    @Override
    public void addParameter(String key, String value)
    {
        super.addParameter(key, value);
 
        if (KEY_LOGINTORP.equalsIgnoreCase(key))
        {
            m_loginToRp = value;
            
            if (RedshiftLogger.isEnable())
          		m_log.logDebug("m_loginToRp: ", m_loginToRp);
        }
    }
    
    @Override
    public String getPluginSpecificCacheKey() {
    	return ((m_loginToRp != null) ? m_loginToRp : "");
    }
    
    protected String getSamlAssertion() throws IOException
    {
        if (StringUtils.isNullOrEmpty(m_idpHost))
        {
            throw new IOException("Missing required property: " + KEY_IDP_HOST);
        }

        if (StringUtils.isNullOrEmpty(m_userName) || StringUtils.isNullOrEmpty(m_password))
        {
            return windowsIntegratedAuthentication();
        }

        return formBasedAuthentication();
    }

    private String windowsIntegratedAuthentication()
    {
        String osName = System.getProperty("os.name").toLowerCase(Locale.getDefault());
        if (!osName.contains("windows"))
        {
            throw new SdkClientException("WIA only support Windows platform.");
        }

        InputStream is = null;
        OutputStream os = null;
        File file = null;
        try
        {
            file = extractExecutable();
            String[] cmd = new String[3];
            cmd[0] = file.getAbsolutePath();

            cmd[1] =
                    "https://" +
                    m_idpHost + ':' + m_idpPort +
                    "/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=" + m_loginToRp;

            cmd[2] = String.valueOf(Boolean.getBoolean("adfs.insecure"));

            if (RedshiftLogger.isEnable())
          		m_log.logDebug("Command: {0}:{1}:{2}", cmd[0],cmd[1],cmd[2]);
            
            Process process = Runtime.getRuntime().exec(cmd);
            is = process.getInputStream();
            os = process.getOutputStream();

            String samlAssertion = IOUtils.toString(is);
            int code = process.waitFor();
            if (code != 0)
            {
                throw new SdkClientException("Failed execute adfs command, return: " + code);
            }

            return samlAssertion;
        }
        catch (InterruptedException e)
        {
            throw new SdkClientException("Failed execute adfs command.", e);
        }
        catch (IOException e)
        {
            throw new SdkClientException("Failed execute adfs command.", e);
        }
        finally
        {
            IOUtils.closeQuietly(is, null);
            IOUtils.closeQuietly(os, null);
            if (file != null && !file.delete())
            {
                file.deleteOnExit();
            }
        }
    }

    private String formBasedAuthentication() throws IOException
    {
        String uri = "https://" + m_idpHost + ':' + m_idpPort +
            "/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=" + m_loginToRp;

        CloseableHttpClient client = null;

        try
        {
          if (RedshiftLogger.isEnable())
        		m_log.logDebug("uri: {0}", uri);
        	
            client = getHttpClient();
            HttpGet get = new HttpGet(uri);
            CloseableHttpResponse resp = client.execute(get);
            if (resp.getStatusLine().getStatusCode() != 200)
            {
            	if(RedshiftLogger.isEnable())
            		m_log.log(LogLevel.DEBUG, "formBasedAuthentication https response:" + EntityUtils.toString(resp.getEntity()));
            	
                throw new IOException(
                        "Failed send request: " + resp.getStatusLine().getReasonPhrase());
            }

            String body = EntityUtils.toString(resp.getEntity());
            
            if (RedshiftLogger.isEnable())
          		m_log.logDebug("body: {0}", body);
            
            List<NameValuePair> parameters = new ArrayList<NameValuePair>();
            for (String inputTag : getInputTagsfromHTML(body))
            {
                String name = getValueByKey(inputTag, "name");
                String value = getValueByKey(inputTag, "value");
                String nameLower = name.toLowerCase();
                
                if (RedshiftLogger.isEnable())
              		m_log.logDebug("name: {0}", name);
                
                if (nameLower.contains("username"))
                {
                    parameters.add(new BasicNameValuePair(name, m_userName));
                }
                else if (nameLower.contains("authmethod"))
                {
                    if (!value.isEmpty())
                    {
                        parameters.add(new BasicNameValuePair(name, value));
                    }
                }
                else if (nameLower.contains("password"))
                {
                    parameters.add(new BasicNameValuePair(name, m_password));
                }
                else if (!name.isEmpty())
                {
                    parameters.add(new BasicNameValuePair(name, value));
                }
            }

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
                throw new IOException("Failed to login ADFS.");
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

    private File extractExecutable() throws IOException
    {
        File file = File.createTempFile("adfs", ".exe");
        InputStream is = null;
        OutputStream os = null;
        try
        {
            is = AdfsCredentialsProvider.class.getResourceAsStream("adfs.exe");
            os = new FileOutputStream(file);
            IOUtils.copy(is, os);
        }
        finally
        {
            IOUtils.closeQuietly(is, null);
            IOUtils.closeQuietly(os, null);
        }

        return file;
    }
}
