package com.amazon.redshift.plugin;

import com.amazon.redshift.CredentialsHolder;
import com.amazon.redshift.CredentialsHolder.IamMetadata;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.core.IamHelper;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.httpclient.log.IamCustomLogFactory;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

public abstract class SamlCredentialsProvider extends IdpCredentialsProvider implements IPlugin
{
    protected static final String KEY_IDP_HOST = "idp_host";
    private static final String KEY_IDP_PORT = "idp_port";
    private static final String KEY_DURATION = "duration";
    private static final String KEY_PREFERRED_ROLE = "preferred_role";

    protected String m_userName;
    protected String m_password;
    protected String m_idpHost;
    protected int m_idpPort = 443;
    protected int m_duration;
    protected String m_preferredRole;
    protected String m_dbUser;
    protected String m_dbGroups;
    protected String m_dbGroupsFilter;
    protected Boolean m_forceLowercase;
    protected Boolean m_autoCreate;
    protected String m_stsEndpoint;
    protected String m_region;
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
        SamlCredentialsProvider.class.getClassLoader())
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
                return SamlCredentialsProvider.class.getResource(LOG_PROPERTIES_FILE_NAME);
            }
            return super.getResource(name);
        }
    };

    protected abstract String getSamlAssertion() throws IOException;

    @Override
    public void addParameter(String key, String value)
    {
        if (RedshiftLogger.isEnable()) {
            m_log.logDebug("key: {0}", key);
        }

        if (RedshiftProperty.UID.getName().equalsIgnoreCase(key)
                || RedshiftProperty.USER.getName().equalsIgnoreCase(key))
        {
            m_userName = value;
        }
        else if (RedshiftProperty.PWD.getName().equalsIgnoreCase(key)
                || RedshiftProperty.PASSWORD.getName().equalsIgnoreCase(key))
        {
            m_password = value;
        }
        else if (KEY_IDP_HOST.equalsIgnoreCase(key))
        {
            m_idpHost = value;
        }
        else if (KEY_IDP_PORT.equalsIgnoreCase(key))
        {
            m_idpPort = Integer.parseInt(value);
        }
        else if (KEY_DURATION.equalsIgnoreCase(key))
        {
            m_duration = Integer.parseInt(value);
        }
        else if (KEY_PREFERRED_ROLE.equalsIgnoreCase(key))
        {
            m_preferredRole = value;
        }
        else if (KEY_SSL_INSECURE.equalsIgnoreCase(key))
        {
            m_sslInsecure = Boolean.parseBoolean(value);
        }
        else if (RedshiftProperty.DB_USER.getName().equalsIgnoreCase(key))
        {
            m_dbUser = value;
        }
        else if (RedshiftProperty.DB_GROUPS.getName().equalsIgnoreCase(key))
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
        return IamHelper.SAML_PLUGIN;
    }

    @Override
    public CredentialsHolder resolveCredentials()
    {
        CredentialsHolder credentials = null;

        if (!m_disableCache) {
            String key = getCacheKey();
            credentials = m_cache.get(key);
        }

        if (credentials == null || credentials.isExpired())
        {
            if (RedshiftLogger.isEnable()) {
                m_log.logInfo("SAML getCredentials NOT from cache");
            }

            synchronized(this) {

                refresh();

                if (m_disableCache) {
                    credentials = m_lastRefreshCredentials;
                    m_lastRefreshCredentials = null;
                }
            }
        }
        else {
            credentials.setRefresh(false);
            if (RedshiftLogger.isEnable()) {
                m_log.logInfo("SAML getCredentials from cache");
            }
        }

        if(!m_disableCache) {
            // if the SAML response has dbUser argument, it will be picked up at this point.
            credentials = m_cache.get(getCacheKey());
        }

        // if dbUser argument has been passed in the connection string, add it to metadata.
        if (!Utils.isNullOrEmpty(m_dbUser))
        {
            credentials.getThisMetadata().setDbUser(this.m_dbUser);
        }

        if (credentials == null)
        {
            throw SdkClientException.create("Unable to load AWS credentials from ADFS");
        }

        if(RedshiftLogger.isEnable()) {
            Date now = new Date();
            m_log.logInfo(now + ": Using entry for SamlCredentialsProvider.getCredentials cache with expiration " + credentials.getExpiration());
        }

        return credentials;
    }

    public void refresh()
    {
        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();

        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try
        {
            String samlAssertion = getSamlAssertion();

            if (RedshiftLogger.isEnable()) {
                m_log.logDebug("SamlCredentialsProvider: Received SAML assertion of length={0}", samlAssertion != null ? samlAssertion.length() : -1);
            }

            final Pattern SAML_PROVIDER_PATTERN = Pattern.compile("arn:aws[-a-z]*:iam::\\d*:saml-provider/\\S+");
            final Pattern ROLE_PATTERN = Pattern.compile("arn:aws[-a-z]*:iam::\\d*:role/\\S+");
            Document doc = parse(Base64.decodeBase64(samlAssertion));
            XPath xPath =  XPathFactory.newInstance().newXPath();
            String expression = "//*[local-name()='Attribute'][@Name='https://aws.amazon.com/SAML/Attributes/Role']/*[local-name()='AttributeValue']/text()";
            NodeList nodeList = (NodeList) xPath.compile(expression)
                    .evaluate(doc, XPathConstants.NODESET);

            Map<String, String> roles = new HashMap<String, String>();
            if (nodeList != null)
            {
                for (int i = 0; i < nodeList.getLength(); ++i)
                {
                    Node node = nodeList.item(i);
                    String value = node.getNodeValue();
                    String[] arns = value.split(",");
                    if (arns.length >= 2)
                    {
                        String provider = null;
                        String role = null;
                        for (String arn : arns)
                        {
                            Matcher providerMatcher = SAML_PROVIDER_PATTERN.matcher(arn);
                            if (providerMatcher.find())
                            {
                                provider = providerMatcher.group(0);
                                continue;
                            }
                            Matcher roleMatcher = ROLE_PATTERN.matcher(arn);
                            if (roleMatcher.find())
                            {
                                role = roleMatcher.group(0);
                            }
                        }
                        if (!Utils.isNullOrEmpty(role) && !Utils.isNullOrEmpty(provider))
                        {
                            roles.put(role, provider);
                        }
                    }
                }
            }

            if (roles.isEmpty())
            {
                throw SdkClientException.create("No role found in SamlAssertion: " + samlAssertion);
            }

            String roleArn;
            String principal;
            if (m_preferredRole != null)
            {
                roleArn = m_preferredRole;
                principal = roles.get(m_preferredRole);
                if (principal == null)
                {
                    throw SdkClientException.create("Preferred role not found in SamlAssertion: " + samlAssertion);
                }
            }
            else
            {
                Map.Entry<String, String> entry = roles.entrySet().iterator().next();
                roleArn = entry.getKey();
                principal = entry.getValue();
            }

            AssumeRoleWithSamlRequest.Builder samlRequestBuilder = AssumeRoleWithSamlRequest.builder();
            samlRequestBuilder.samlAssertion(samlAssertion);
            samlRequestBuilder.roleArn(roleArn);
            samlRequestBuilder.principalArn(principal);
            if (m_duration > 0)
            {
                samlRequestBuilder.durationSeconds(m_duration);
            }

            AwsCredentialsProvider p = AnonymousCredentialsProvider.create();
            StsClientBuilder builder = StsClient.builder();
            ClientOverrideConfiguration config = null;
            builder.overrideConfiguration(config);

            StsClient stsSvc =
                    RequestUtils.buildSts(m_stsEndpoint, m_region, builder, p, m_log);

            AssumeRoleWithSamlResponse result = stsSvc.assumeRoleWithSAML(samlRequestBuilder.build());
            Credentials cred = result.credentials();
            Instant expiration = cred.expiration();
            AwsCredentials c = AwsSessionCredentials.create(cred.accessKeyId(), cred.secretAccessKey(), cred.sessionToken());
            CredentialsHolder credentials = CredentialsHolder.newInstance(c, expiration);
            credentials.setMetadata(readMetadata(doc));
            credentials.setRefresh(true);

            if (!m_disableCache) {
                m_cache.put(getCacheKey(), credentials);
            } else {
                m_lastRefreshCredentials = credentials;
            }
        }
        catch (IOException e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        catch (SAXException e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        catch (ParserConfigurationException e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        catch (XPathExpressionException e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        catch (Exception e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        finally {
            currentThread.setContextClassLoader(cl);
        }
    }

    @Override
    public String getPluginSpecificCacheKey() {
        // Override this in each derived plugin such as Azure, Browser, Okta, Ping etc.
        return "";
    }

    @Override
    public String getIdpToken() {
        String samlAssertion = null;
        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();

        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try
        {
            samlAssertion = getSamlAssertion();

            if (RedshiftLogger.isEnable()) {
                m_log.logDebug("SamlCredentialsProvider: Got SAML assertion of " +
                        "length={0}", samlAssertion != null ? samlAssertion.length() : -1);
            }
        }
        catch (IOException e)
        {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        catch (Exception e)
        {
            if (RedshiftLogger.isEnable()) {
                m_log.logError(e);
            }

            throw SdkClientException.create("SAML error: " + e.getMessage(), e);
        }
        finally
        {
            currentThread.setContextClassLoader(cl);
        }

        return  samlAssertion;
    }

    @Override
    public void setGroupFederation(boolean groupFederation) {
        m_groupFederation = groupFederation;
    }

    @Override
    public String getCacheKey()
    {
        String pluginSpecificKey = getPluginSpecificCacheKey();
        return m_userName + m_password + m_idpHost + m_idpPort + m_duration + m_preferredRole + pluginSpecificKey;
    }

    private IamMetadata readMetadata(Document doc) throws XPathExpressionException
    {
        IamMetadata metadata = new IamMetadata();
        XPath xPath = XPathFactory.newInstance().newXPath();


        List<String> attributeValues = GetSAMLAttributeValues(xPath, doc,
                "https://redshift.amazon.com/SAML/Attributes/AllowDbUserOverride");
        if (!attributeValues.isEmpty())
        {
            metadata.setAllowDbUserOverride(Boolean.valueOf(attributeValues.get(0)));
        }

        attributeValues = GetSAMLAttributeValues(xPath, doc,
                "https://redshift.amazon.com/SAML/Attributes/DbUser");
        if (!attributeValues.isEmpty())
        {
            metadata.setSamlDbUser(attributeValues.get(0));
        }
        else
        {
            attributeValues = GetSAMLAttributeValues(xPath, doc,
                    "https://aws.amazon.com/SAML/Attributes/RoleSessionName");
            if (!attributeValues.isEmpty())
            {
                metadata.setSamlDbUser(attributeValues.get(0));
            }
        }

        attributeValues = GetSAMLAttributeValues(xPath, doc,
                "https://redshift.amazon.com/SAML/Attributes/AutoCreate");
        if (!attributeValues.isEmpty())
        {
            metadata.setAutoCreate(Boolean.valueOf(attributeValues.get(0)));
        }

        attributeValues = GetSAMLAttributeValues(xPath, doc,
                "https://redshift.amazon.com/SAML/Attributes/DbGroups");
        if (!attributeValues.isEmpty())
        {
            attributeValues = filterOutGroups(attributeValues);
            if (!attributeValues.isEmpty())
            {
                StringBuilder sb = new StringBuilder();
                for (String value : attributeValues)
                {
                    if (sb.length() > 0)
                    {
                        sb.append(',');
                    }
                    sb.append(value);
                }
                metadata.setDbGroups(sb.toString());
            }
        }

        attributeValues = GetSAMLAttributeValues(xPath, doc,
                "https://redshift.amazon.com/SAML/Attributes/ForceLowercase");
        if (!attributeValues.isEmpty())
        {
            metadata.setForceLowercase(Boolean.valueOf(attributeValues.get(0)));
        }

        return metadata;
    }

    /**
     * Method removes all groups from given lists matching {@link m_dbGroupsFilter}
     *  regex.
     * @param attributeValues in
     * @return attributeValues filtered
     */
    private List<String> filterOutGroups(List<String> attributeValues) {
        if ( m_dbGroupsFilter != null )
        {
            final Pattern groupsFilter = Pattern.compile(m_dbGroupsFilter);
            List<String> ret = new ArrayList<>();
            for (String attributeValue : attributeValues)
            {
                m_log.logDebug("Check group {0} with regexp {1}",
                        attributeValue, m_dbGroupsFilter);
                if (!groupsFilter.matcher(attributeValue).matches())
                {
                    m_log.logDebug("Add {0} to dbgroups", attributeValue);
                    ret.add(attributeValue);
                }
            }
            return ret;
        }
        else {
            return attributeValues;
        }
    }

    private static Document parse(byte[] samlAssertion) throws IOException, SAXException,
            ParserConfigurationException
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        DocumentBuilder db = factory.newDocumentBuilder();
        return db.parse(new ByteArrayInputStream(samlAssertion));
    }

    private static List<String> GetSAMLAttributeValues(XPath xPath, Document doc, String attributeName)
            throws XPathExpressionException
    {
        String expression = String.format("//Attribute[@Name='%s']/AttributeValue/text()", attributeName);
        NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);
        if (null == nodeList || nodeList.getLength() == 0)
        {
            return Collections.emptyList();
        }
        List<String> attributeValues = new ArrayList<String>(nodeList.getLength());
        for (int i = 0; i < nodeList.getLength(); ++i)
        {
            Node node = nodeList.item(i);
            attributeValues.add(node.getNodeValue());
        }
        return attributeValues;
    }

    protected List<String> getInputTagsfromHTML(String body)
    {
        Set<String> distinctInputTags = new HashSet<>();
        List<String> inputTags = new ArrayList<String>();
        Pattern inputTagPattern = Pattern.compile("<input(.+?)/>", Pattern.DOTALL);
        Matcher inputTagMatcher = inputTagPattern.matcher(body);
        while (inputTagMatcher.find())
        {
            String tag = inputTagMatcher.group(0);
            String tagNameLower = getValueByKey(tag, "name").toLowerCase();
            if (!tagNameLower.isEmpty() && distinctInputTags.add(tagNameLower))
            {
                inputTags.add(tag);
            }
        }
        return inputTags;
    }

    protected String getFormAction(String body)
    {
        Pattern pattern = Pattern.compile("<form.*?action=\"([^\"]+)\"");
        Matcher m = pattern.matcher(body);
        if (m.find())
        {
            return escapeHtmlEntity(m.group(1));
        }
        return null;
    }

    protected String getValueByKey(String input, String key)
    {
        Pattern keyValuePattern = Pattern.compile("(" + Pattern.quote(key) + ")\\s*=\\s*\"(.*?)\"");
        Matcher keyValueMatcher = keyValuePattern.matcher(input);
        if (keyValueMatcher.find())
        {
            return escapeHtmlEntity(keyValueMatcher.group(2));
        }
        return "";
    }

    protected String getValueByKeyWithoutQuotesAndValueInSingleQuote(String input, String key)
    {
        Pattern keyValuePattern = Pattern.compile("(" + key + ")\\s*=\\s*'(.*?)'");
        Matcher keyValueMatcher = keyValuePattern.matcher(input);
        if (keyValueMatcher.find())
        {
            return escapeHtmlEntity(keyValueMatcher.group(2));
        }
        return "";
    }

    /**
     * Escape certain HTML entities for the given input string.
     *
     * @param html          The string to escape.
     * @return              The string with the special HTML entities escaped.
     */
    protected String escapeHtmlEntity(String html)
    {
        StringBuilder sb = new StringBuilder(html.length());
        int i = 0;
        int length = html.length();
        while (i < length)
        {
            char c = html.charAt(i);
            if (c != '&')
            {
                sb.append(c);
                i++;
                continue;
            }

            if (html.startsWith("&amp;", i))
            {
                sb.append('&');
                i += 5;
            }
            else if (html.startsWith("&apos;", i))
            {
                sb.append('\'');
                i += 6;
            }
            else if (html.startsWith("&quot;", i))
            {
                sb.append('"');
                i += 6;
            }
            else if (html.startsWith("&lt;", i))
            {
                sb.append('<');
                i += 4;
            }
            else if (html.startsWith("&gt;", i))
            {
                sb.append('>');
                i += 4;
            }
            else
            {
                sb.append(c);
                ++i;
            }
        }
        return sb.toString();
    }

    protected void checkRequiredParameters() throws IOException
    {
        if (Utils.isNullOrEmpty(m_userName))
        {
            throw new IOException("Missing required property: " + RedshiftProperty.USER.getName());
        }
        if (Utils.isNullOrEmpty(m_password))
        {
            throw new IOException("Missing required property: " + RedshiftProperty.PASSWORD.getName());
        }
        if (Utils.isNullOrEmpty(m_idpHost))
        {
            throw new IOException("Missing required property: " + KEY_IDP_HOST);
        }
    }

    protected boolean isText(String inputTag)
    {
        String typeVal = getValueByKey(inputTag, "type");
        if (typeVal == null
                || typeVal.length() == 0)
        {
            typeVal = getValueByKeyWithoutQuotesAndValueInSingleQuote(inputTag, "type");
        }

        return "text".equals(typeVal);
    }

    protected boolean isPassword(String inputTag)
    {
        String typeVal = getValueByKey(inputTag, "type");
        if (typeVal == null
                || typeVal.length() == 0)
        {
            typeVal = getValueByKeyWithoutQuotesAndValueInSingleQuote(inputTag, "type");
        }

        return "password".equals(typeVal);
    }
}
