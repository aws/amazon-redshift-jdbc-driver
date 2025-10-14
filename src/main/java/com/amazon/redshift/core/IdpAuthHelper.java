package com.amazon.redshift.core;

import java.net.URI;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map.Entry;

import com.amazon.redshift.AuthMech;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftProperties;
import com.amazon.redshift.util.RedshiftState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.redshift.RedshiftClientBuilder;
import software.amazon.awssdk.services.redshift.model.DescribeAuthenticationProfilesRequest;
import software.amazon.awssdk.services.redshift.model.DescribeAuthenticationProfilesResponse;

public class IdpAuthHelper {
  // Subtype of plugin
  public static final int SAML_PLUGIN = 1;
  public static final int JWT_PLUGIN = 2;
  public static final int IDC_PLUGIN = 3;

  protected IdpAuthHelper() {
  }

  protected static RedshiftProperties setAuthProperties(RedshiftProperties info, RedshiftJDBCSettings settings, RedshiftLogger log)
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

      if (!Utils.isNullOrEmpty(authProfile)) {
        if (!Utils.isNullOrEmpty(iamAccessKey)) {
          RedshiftProperties authProfileProps = readAuthProfile(authProfile, iamAccessKey, iamSecretKey, iamSessionToken, log,
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
  private static RedshiftProperties readAuthProfile(String authProfile, String iamAccessKeyID, String iamSecretKey,
      String iamSessionToken, RedshiftLogger log, RedshiftProperties info) throws RedshiftException {
    RedshiftProperties authProfileProps = null;

    AwsCredentials credentials;
    String awsRegion = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_REGION.getName(), info);
    String endpointUrl = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.ENDPOINT_URL.getName(), info);

    if (!Utils.isNullOrEmpty(iamSessionToken)) {
      credentials = AwsSessionCredentials.create(iamAccessKeyID, iamSecretKey, iamSessionToken);
    } else {
      credentials = AwsBasicCredentials.create(iamAccessKeyID, iamSecretKey);
    }

    AwsCredentialsProvider provider = StaticCredentialsProvider.create(credentials);

    RedshiftClientBuilder builder = RedshiftClient.builder();

    ProxyConfiguration proxyConfig = RequestUtils.getProxyConfiguration(log);
    if (proxyConfig != null) {
      builder.httpClient(ApacheHttpClient.builder()
              .proxyConfiguration(proxyConfig)
              .build());
    }

    if (awsRegion != null && !awsRegion.isEmpty()) {
      builder.region(Region.of(awsRegion));
    }

    if (endpointUrl != null) {
      builder.endpointOverride(URI.create(endpointUrl));
    }

    RedshiftClient client = builder
            .credentialsProvider(provider)
            .build();

    DescribeAuthenticationProfilesRequest request = DescribeAuthenticationProfilesRequest.builder()
            .authenticationProfileName(authProfile)
            .build();

    DescribeAuthenticationProfilesResponse result = client.describeAuthenticationProfiles(request);

    String profileContent = result.authenticationProfiles().get(0).authenticationProfileContent();

    authProfileProps = new RedshiftProperties(info);


    try {
      JsonNode profileJson = Utils.parseJson(profileContent);
      Iterator<Entry<String, JsonNode>> elements = profileJson.fields();

      while (elements.hasNext()) {
        Entry<String, JsonNode> element = elements.next();
        String key = element.getKey();
        String val = element.getValue().asText();
        authProfileProps.put(key, val);
      }
    } catch (JsonProcessingException e) {
      RedshiftException redshiftException = new RedshiftException(GT.tr("Auth profile JSON error"), RedshiftState.UNEXPECTED_ERROR, e);

      if (RedshiftLogger.isEnable())
        log.log(LogLevel.ERROR, redshiftException.toString());

      throw redshiftException;
    }

    return authProfileProps;
  }
}
