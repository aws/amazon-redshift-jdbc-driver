package com.amazon.redshift.core;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.Map.Entry;

import com.amazon.redshift.AuthMech;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.DescribeAuthenticationProfilesRequest;
import com.amazonaws.services.redshift.model.DescribeAuthenticationProfilesResult;
import com.amazonaws.util.StringUtils;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;

public class IdpAuthHelper {
  // Subtype of plugin
  public static final int SAML_PLUGIN = 1;
  public static final int JWT_PLUGIN = 2;

  protected IdpAuthHelper() {
  }
  
  protected static Properties setAuthProperties(Properties info, RedshiftJDBCSettings settings, RedshiftLogger log)
      throws RedshiftException {
    try {
      // Plugin requires an SSL connection to work. Make sure that m_authMech is
      // set to
      // SSL level VERIFY_CA or higher.
      if (settings.m_authMech == null || settings.m_authMech.ordinal() < AuthMech.VERIFY_CA.ordinal()) {
        settings.m_authMech = AuthMech.VERIFY_CA;
      }

      // Check for IAM keys and AuthProfile first
      String iamAccessKey = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IAM_ACCESS_KEY_ID.getName(),
          info);

      String iamSecretKey = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.IAM_SECRET_ACCESS_KEY.getName(), info);

      String iamSessionToken = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.IAM_SESSION_TOKEN.getName(), info);
      String authProfile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AUTH_PROFILE.getName(), info);

      if (!StringUtils.isNullOrEmpty(authProfile)) {
        if (!StringUtils.isNullOrEmpty(iamAccessKey)) {
          Properties authProfileProps = readAuthProfile(authProfile, iamAccessKey, iamSecretKey, iamSessionToken, log,
              info);
          if (authProfileProps != null) {
            // Merge auth profile props with user props.
            // User props overrides auth profile props
            authProfileProps.putAll(info);
            info = authProfileProps;
          }
        } else {
          // Auth profile specified but IAM keys are not
          RedshiftException err = new RedshiftException(
              GT.tr("Dependent connection property setting for {0} is missing {1}",
                  RedshiftProperty.AUTH_PROFILE.getName(), RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
              RedshiftState.UNEXPECTED_ERROR);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.ERROR, err.toString());

          throw err;

        }

      } // AuthProfile

      String userName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.UID.getName(), info);
      if (userName == null)
        userName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.USER.getName(), info);
      String password = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.PWD.getName(), info);
      if (password == null)
        password = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.PASSWORD.getName(), info);

      String iamCredentialProvider = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.CREDENTIALS_PROVIDER.getName(), info);

      String iamDisableCache = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.IAM_DISABLE_CACHE.getName(), info);

      if (null != userName) {
        settings.m_username = userName;
      }

      if (null != password) {
        settings.m_password = password;
      }

      if (null != iamCredentialProvider) {
        settings.m_credentialsProvider = iamCredentialProvider;
      }

      settings.m_iamDisableCache = iamDisableCache == null ? false : Boolean.valueOf(iamDisableCache);
      
      Enumeration<String> enums = (Enumeration<String>) info.propertyNames();
      while (enums.hasMoreElements()) {
        // The given properties are String pairs, so this should be OK.
        String key = enums.nextElement();
        String value = info.getProperty(key);
        key = key.toLowerCase(Locale.getDefault());
        if (!"*".equals(value)) {
          settings.m_pluginArgs.put(key, value);
        }
      }
    }
     catch (RedshiftException re) {
      if (RedshiftLogger.isEnable())
        log.logError(re);
  
      throw re;
    }
    return info;
  }
  
  /*
   * Response format like: "{ " + " \"AuthenticationProfiles\": [ " + " {" +
   * " \"AuthenticationProfileName\":\"ExampleProfileName\", " +
   * " \"AuthenticationProfileContent\":\"{" +
   * "  \\\"AllowDBUserOverride\\\": \\\"1\\\", " +
   * " \\\"databaseMetadataCurrentDbOnly\\\": \\\"true\\\" " + " }\" " + " }" +
   * "] " + "   } ";
   */
  private static Properties readAuthProfile(String authProfile, String iamAccessKeyID, String iamSecretKey,
      String iamSessionToken, RedshiftLogger log, Properties info) throws RedshiftException {
    Properties authProfileProps = null;

    AWSCredentials credentials;
    String awsRegion = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_REGION.getName(), info);
    String endpointUrl = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.ENDPOINT_URL.getName(), info);

    if (!StringUtils.isNullOrEmpty(iamSessionToken)) {
      credentials = new BasicSessionCredentials(iamAccessKeyID, iamSecretKey, iamSessionToken);
    } else {
      credentials = new BasicAWSCredentials(iamAccessKeyID, iamSecretKey);
    }

    AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);

    AmazonRedshiftClientBuilder builder = AmazonRedshiftClientBuilder.standard();

    ClientConfiguration clientConfig = RequestUtils.getProxyClientConfig(log);

    if (clientConfig != null) {
      builder.setClientConfiguration(clientConfig);
    }

    if (endpointUrl != null) {
      EndpointConfiguration cfg = new EndpointConfiguration(endpointUrl, awsRegion);
      builder.setEndpointConfiguration(cfg);
    } else if (awsRegion != null && !awsRegion.isEmpty()) {
      builder.setRegion(awsRegion);
    }

    AmazonRedshift client = builder.withCredentials(provider).build();

    DescribeAuthenticationProfilesRequest request = new DescribeAuthenticationProfilesRequest();

    request.setAuthenticationProfileName(authProfile);

    DescribeAuthenticationProfilesResult result = client.describeAuthenticationProfiles(request);

    String profileContent = result.getAuthenticationProfiles().get(0).getAuthenticationProfileContent();

    authProfileProps = new Properties(info);
    JsonNode profileJson = Jackson.jsonNodeOf(profileContent);

    if (profileJson != null) {
      Iterator<Entry<String, JsonNode>> elements = profileJson.fields();

      while (elements.hasNext()) {
        Entry<String, JsonNode> element = elements.next();
        String key = element.getKey();
        String val = element.getValue().asText();
        authProfileProps.put(key, val);
      }
    } else {
      // Error
      RedshiftException err = new RedshiftException(GT.tr("Auth profile JSON error"), RedshiftState.UNEXPECTED_ERROR);

      if (RedshiftLogger.isEnable())
        log.log(LogLevel.ERROR, err.toString());

      throw err;
    }

    return authProfileProps;
  }
}
