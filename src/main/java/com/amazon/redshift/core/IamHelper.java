package com.amazon.redshift.core;

import com.amazon.redshift.util.RedshiftProperties;
import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.amazonaws.services.redshift.model.Endpoint;
import com.amazonaws.services.redshift.model.GetClusterCredentialsRequest;
import com.amazonaws.services.redshift.model.GetClusterCredentialsResult;
import com.amazonaws.services.redshift.model.GetClusterCredentialsWithIAMRequest;
import com.amazonaws.services.redshift.model.GetClusterCredentialsWithIAMResult;
import com.amazonaws.services.redshift.model.DescribeCustomDomainAssociationsRequest;
import com.amazonaws.services.redshift.model.DescribeCustomDomainAssociationsResult;
import com.amazonaws.services.redshift.model.Association;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.util.StringUtils;
import com.amazon.redshift.CredentialsHolder;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IamHelper extends IdpAuthHelper {
  static final int MAX_AMAZONCLIENT_RETRY = 5;
  static final int MAX_AMAZONCLIENT_RETRY_DELAY_MS = 1000;

  private static final String KEY_PREFERRED_ROLE = "preferred_role";
  private static final String KEY_ROLE_SESSION_NAME = "roleSessionName";
  private static final String KEY_ROLE_ARN = "roleArn";

  // Type of GetClusterCredential API
  public static final int GET_CLUSTER_CREDENTIALS_V1_API = 1;
  public static final int GET_CLUSTER_CREDENTIALS_IAM_V2_API = 2;
  public static final int GET_CLUSTER_CREDENTIALS_SAML_V2_API = 3;
  public static final int GET_CLUSTER_CREDENTIALS_JWT_V2_API = 4;
  public static final int GET_SERVERLESS_CREDENTIALS_V1_API = 5;

  private static final Pattern HOST_PATTERN =
          Pattern.compile("(.+)\\.(.+)\\.(.+).redshift(-dev)?\\.amazonaws\\.com(.)*");

  private static final Pattern SERVERLESS_WORKGROUP_HOST_PATTERN =
          Pattern.compile("(.+)\\.(.+)\\.(.+).redshift-serverless(-dev)?\\.amazonaws\\.com(.)*");

  enum CredentialProviderType
  {
    NONE, PROFILE, IAM_KEYS_WITH_SESSION, IAM_KEYS, PLUGIN
  }

  private static Map<String, GetClusterCredentialsResult> credentialsCache = new HashMap<String, GetClusterCredentialsResult>();
  private static Map<String, GetClusterCredentialsWithIAMResult> credentialsV2Cache = new HashMap<String, GetClusterCredentialsWithIAMResult>();

  private IamHelper() {
  }

  /**
   * Helper function to handle IAM connection properties. If any IAM related
   * connection property is specified, all other <b>required</b> IAM properties
   * must be specified too or else it throws an error.
   *
   * @param info
   *          Redshift client settings used to authenticate if connection should
   *          be granted.
   * @param settings
   *          Redshift IAM settings
   * @param log
   *          Redshift logger
   * 
   * @return New property object with properties from auth profile and given
   *         input info properties, if auth profile found. Otherwise same
   *         property object as info return.
   *
   * @throws RedshiftException
   *           If an error occurs.
   */
  public static RedshiftProperties setIAMProperties(RedshiftProperties info, RedshiftJDBCSettings settings, RedshiftLogger log)
      throws RedshiftException {
    try {
      
      // Common code for IAM and Native Auth
      info = setAuthProperties(info, settings, log);

      // IAM keys 
      String iamAccessKey = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IAM_ACCESS_KEY_ID.getName(),
          info);

      String iamSecretKey = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.IAM_SECRET_ACCESS_KEY.getName(), info);

      String iamSessionToken = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.IAM_SESSION_TOKEN.getName(), info);
      
      String authProfile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AUTH_PROFILE.getName(), info);

      String host = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.HOST.getName(), info);
      String userSetServerless = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IS_SERVERLESS.getName(), info);
      Boolean hasUserSetServerless = "true".equalsIgnoreCase(userSetServerless);
      String acctId = null;
      String workGroup = null;

      Matcher mProvisioned = null;
      Matcher mServerless = null;

      if(null != host)
      {
        mProvisioned = HOST_PATTERN.matcher(host);
        mServerless = SERVERLESS_WORKGROUP_HOST_PATTERN.matcher(host);
      }
      String clusterId = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.CLUSTER_IDENTIFIER.getName(), info);;

      if ((null != mProvisioned && mProvisioned.matches()) || (null != clusterId && clusterId.startsWith("redshift-serverless-")))
      {
        // provisioned vanilla OR serverless backdoor which allows calling getClusterCredentials which is a provisioned API
        if (RedshiftLogger.isEnable())
          log.logInfo("Code flow for regular provisioned cluster");

        clusterId = RedshiftConnectionImpl.getRequiredConnSetting(RedshiftProperty.CLUSTER_IDENTIFIER.getName(), info);
      }
      else if (null != mServerless && mServerless.matches())
      {
        // serverless vanilla
        // do nothing, regular serverless logic flow
        if (RedshiftLogger.isEnable())
          log.logInfo("Code flow for regular serverless cluster");

//        String isServerless = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IS_SERVERLESS.getName(), info);
//        settings.m_isServerless = isServerless == null ? false : Boolean.valueOf(isServerless);

        settings.m_isServerless = true;
        acctId = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.SERVERLESS_ACCT_ID.getName(), info);
        workGroup = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.SERVERLESS_WORK_GROUP.getName(), info);
      }
      else if (hasUserSetServerless)
      {
        // hostname doesn't match serverless regex but serverless set to true explicitly by user
        // when ready for implementation, remove setting of the isServerless property automatically in parseUrl(),
        // set it here instead
        // currently do nothing as server does not support cname for serverless

        settings.m_isServerless = true;
        workGroup = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.SERVERLESS_WORK_GROUP.getName(), info);
        acctId = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.SERVERLESS_ACCT_ID.getName(), info);

        if(workGroup != null)
        {
          // workgroup specified by user - serverless nlb call
          // check for serverlessAcctId to enter serverless NLB logic flow, for when we implement this for serverless after server side is ready
          // currently do nothing as regular code flow is sufficient
          if (RedshiftLogger.isEnable())
            log.logInfo("Code flow for nlb serverless cluster");
        }
        else
        {
          // attempt serverless cname call - currently not supported by server
          // currently sets isCname to true which will be asserted on later, as cname for serverless is not supported yet
          if (RedshiftLogger.isEnable())
            log.logInfo("Code flow for cname serverless cluster");
          settings.m_isCname = true;
        }
      }
      else
      {
        if (RedshiftLogger.isEnable())
          log.logInfo("Code flow for nlb/cname in provisioned clusters");

        // attempt provisioned cname call
        // cluster id will be fetched upon describing custom domain name
        settings.m_isCname = true;
      }

      String awsRegion = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_REGION.getName(), info);
      String endpointUrl = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.ENDPOINT_URL.getName(), info);
      String stsEndpointUrl = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.STS_ENDPOINT_URL.getName(),
          info);

      String profile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_PROFILE.getName(), info);
      if (profile == null)
        profile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_PROFILE.getName().toLowerCase(),
            info);
      String iamDuration = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IAM_DURATION.getName(), info);

      String iamAutoCreate = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.USER_AUTOCREATE.getName(),
          info);
      String iamDbUser = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.DB_USER.getName(), info);
      String iamDbGroups = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.DB_GROUPS.getName(), info);
      String iamForceLowercase = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.FORCE_LOWERCASE.getName(), info);
      String iamGroupFederation = RedshiftConnectionImpl
          .getOptionalConnSetting(RedshiftProperty.GROUP_FEDERATION.getName(), info);
      String dbName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.DBNAME.getName(), info);

      String hosts = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.HOST.getName(), info);
      String ports = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.PORT.getName(), info);

      settings.m_clusterIdentifier = clusterId;

      if (!settings.m_isServerless && !settings.m_isCname
              && (null == settings.m_clusterIdentifier || settings.m_clusterIdentifier.isEmpty()))
      {
        RedshiftException err = new RedshiftException(
            GT.tr("Missing connection property {0}", RedshiftProperty.CLUSTER_IDENTIFIER.getName()),
            RedshiftState.UNEXPECTED_ERROR);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      }

      if (settings.m_isServerless) {
        settings.m_acctId = acctId;
        settings.m_workGroup = workGroup;
      }

      // Regions.fromName(string) requires the string to be lower case and in
      // this format:
      // E.g. "us-west-2"
      if (null != awsRegion) {
        settings.m_awsRegion = awsRegion.trim().toLowerCase();
      }

      if (null != endpointUrl) {
        settings.m_endpoint = endpointUrl;
      } else {
        settings.m_endpoint = System.getProperty("redshift.endpoint-url");
      }

      if (null != stsEndpointUrl) {
        settings.m_stsEndpoint = stsEndpointUrl;
      } else {
        settings.m_stsEndpoint = System.getProperty("sts.endpoint-url");
      }

      if (null != profile) {
        settings.m_profile = profile;
      }

      if (null != iamDuration) {
        try {
          settings.m_iamDuration = Integer.parseInt(iamDuration);
          if (settings.m_iamDuration < 900 || settings.m_iamDuration > 3600) {
            RedshiftException err = new RedshiftException(
                GT.tr("Invalid connection property value or type range(900-3600) {0}",
                    RedshiftProperty.IAM_DURATION.getName()),
                RedshiftState.UNEXPECTED_ERROR);

            if (RedshiftLogger.isEnable())
              log.log(LogLevel.ERROR, err.toString());

            throw err;
          }
        } catch (NumberFormatException e) {
          RedshiftException err = new RedshiftException(GT.tr("Invalid connection property value {0} : {1}",
              RedshiftProperty.IAM_DURATION.getName(), iamDuration), RedshiftState.UNEXPECTED_ERROR, e);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, err.toString());

          throw err;
        }
      }

      if (null != iamAccessKey) {
        settings.m_iamAccessKeyID = iamAccessKey;
      }

      // Because the secret access key should be hidden, and most applications
      // (for example:
      // SQL Workbench) only hide passwords, Amazon has requested that we allow
      // the
      // secret access key to be passed as either the IAMSecretAccessKey
      // property or
      // as a password value.
      if (null != iamSecretKey) {
        if (StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID)) {
          RedshiftException err = new RedshiftException(
              GT.tr("Missing connection property {0}", RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
              RedshiftState.UNEXPECTED_ERROR);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.ERROR, err.toString());

          throw err;
        }

        settings.m_iamSecretKey = iamSecretKey;
        if (settings.m_iamSecretKey.isEmpty()) {
          settings.m_iamSecretKey = settings.m_password;
        }
      } else {
        settings.m_iamSecretKey = settings.m_password;
      }

      if (null != iamSessionToken) {
        if (StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID)) {
          RedshiftException err = new RedshiftException(
              GT.tr("Missing connection property {0}", RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
              RedshiftState.UNEXPECTED_ERROR);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.ERROR, err.toString());
          throw err;
        }
        settings.m_iamSessionToken = iamSessionToken;
      }

      settings.m_autocreate = iamAutoCreate == null ? null : Boolean.valueOf(iamAutoCreate);

      settings.m_forceLowercase = iamForceLowercase == null ? null : Boolean.valueOf(iamForceLowercase);

      settings.m_groupFederation = iamGroupFederation == null ? false : Boolean.valueOf(iamGroupFederation);

      if (null != iamDbUser) {
        settings.m_dbUser = iamDbUser;
      }

      settings.m_dbGroups = (iamDbGroups != null)
          ? Arrays.asList((settings.m_forceLowercase != null && settings.m_forceLowercase
              ? iamDbGroups.toLowerCase(Locale.getDefault()) : iamDbGroups).split(","))
          : Collections.<String>emptyList();

      settings.m_Schema = dbName;
      if (hosts != null) {
        settings.m_host = hosts;
      }
      if (ports != null) {
        settings.m_port = Integer.parseInt(ports);
      }

      setIAMCredentials(settings, log, authProfile);

      return info;
    } catch (RedshiftException re) {
      if (RedshiftLogger.isEnable())
        log.logError(re);

      throw re;
    }
  }

  /**
   * Helper function to create the appropriate credential providers.
   *
   * @throws RedshiftException
   *           If an unspecified error occurs.
   */
  private static void setIAMCredentials(RedshiftJDBCSettings settings, RedshiftLogger log, String authProfile) throws RedshiftException {
    AWSCredentialsProvider provider;
    CredentialProviderType providerType = CredentialProviderType.NONE;
    boolean idpCredentialsRefresh = false;
    String idpToken = null;

    if (!StringUtils.isNullOrEmpty(settings.m_credentialsProvider)) {
      if (!StringUtils.isNullOrEmpty(settings.m_profile)) {
        RedshiftException err = new RedshiftException(
            GT.tr("Conflict in connection property setting {0} and {1}",
                RedshiftProperty.CREDENTIALS_PROVIDER.getName(), RedshiftProperty.AWS_PROFILE.getName()),
            RedshiftState.UNEXPECTED_ERROR);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      }

      if (StringUtils.isNullOrEmpty(authProfile)
            && !StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID)) {
        RedshiftException err = new RedshiftException(
            GT.tr("Conflict in connection property setting {0} and {1}",
                RedshiftProperty.CREDENTIALS_PROVIDER.getName(), RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
            RedshiftState.UNEXPECTED_ERROR);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      }

      try {
        Class<? extends AWSCredentialsProvider> clazz = (Class.forName(settings.m_credentialsProvider)
            .asSubclass(AWSCredentialsProvider.class));

        provider = clazz.newInstance();
        if (provider instanceof IPlugin) {
          IPlugin plugin = ((IPlugin) provider);

          providerType = CredentialProviderType.PLUGIN;
          plugin.setLogger(log);
          plugin.setGroupFederation(settings.m_groupFederation);
          for (Map.Entry<String, String> entry : settings.m_pluginArgs.entrySet()) {
            String pluginArgKey = entry.getKey();
            plugin.addParameter(pluginArgKey, entry.getValue());

            if (KEY_PREFERRED_ROLE.equalsIgnoreCase(pluginArgKey))
              settings.m_preferredRole = entry.getValue();
            else if (KEY_ROLE_ARN.equalsIgnoreCase(pluginArgKey))
              settings.m_roleArn = entry.getValue();
            else if (KEY_ROLE_SESSION_NAME.equalsIgnoreCase(pluginArgKey))
              settings.m_roleSessionName = entry.getValue();
            else if (RedshiftProperty.DB_GROUPS_FILTER.getName().equalsIgnoreCase(pluginArgKey))
              settings.m_dbGroupsFilter = entry.getValue();
          }
        }
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        RedshiftException err = new RedshiftException(
            GT.tr("Invalid credentials provider class {0}", settings.m_credentialsProvider),
            RedshiftState.UNEXPECTED_ERROR, e);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      } catch (NumberFormatException e) {
        RedshiftException err = new RedshiftException(
            GT.tr("{0} : {1}", e.getMessage(), settings.m_credentialsProvider), RedshiftState.UNEXPECTED_ERROR, e);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      }
    } else if (!StringUtils.isNullOrEmpty(settings.m_profile)) {
      if (StringUtils.isNullOrEmpty(authProfile)
            && !StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID)) {
        RedshiftException err = new RedshiftException(GT.tr("Conflict in connection property setting {0} and {1}",
            RedshiftProperty.AWS_PROFILE.getName(), RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
            RedshiftState.UNEXPECTED_ERROR);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      }

      ProfilesConfigFile pcf = new PluginProfilesConfigFile(settings, log);
      provider = new ProfileCredentialsProvider(pcf, settings.m_profile);
      providerType = CredentialProviderType.PROFILE;
    } else if (!StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID)) {
      AWSCredentials credentials;

      if (!StringUtils.isNullOrEmpty(settings.m_iamSessionToken)) {
        credentials = new BasicSessionCredentials(settings.m_iamAccessKeyID, settings.m_iamSecretKey,
            settings.m_iamSessionToken);
        providerType = CredentialProviderType.IAM_KEYS_WITH_SESSION;
      } else {
        credentials = new BasicAWSCredentials(settings.m_iamAccessKeyID, settings.m_iamSecretKey);
        providerType = CredentialProviderType.IAM_KEYS;
      }

      provider = new AWSStaticCredentialsProvider(credentials);
    } else {
      provider = new DefaultAWSCredentialsProviderChain();
    }

    if (RedshiftLogger.isEnable())
      log.log(LogLevel.DEBUG, "IDP Credential Provider {0}:{1}", provider, settings.m_credentialsProvider);

    int getClusterCredentialApiType = findTypeOfGetClusterCredentialsAPI(settings, providerType, provider);

    if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_V1_API
        || getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API
        || getClusterCredentialApiType == GET_SERVERLESS_CREDENTIALS_V1_API)
    {
      if (RedshiftLogger.isEnable())
        log.log(LogLevel.DEBUG, "Calling provider.getCredentials()");

      // Provider will cache the credentials, it's OK to call getCredentials()
      // here.
      AWSCredentials credentials = provider.getCredentials();
      if (credentials instanceof CredentialsHolder) {
        idpCredentialsRefresh = ((CredentialsHolder) credentials).isRefresh();

        // autoCreate, user and password from URL take priority.
        CredentialsHolder.IamMetadata im = ((CredentialsHolder) credentials).getMetadata();
        if (null != im) {
          Boolean autoCreate = im.getAutoCreate();
          String dbUser = im.getDbUser();
          String samlDbUser = im.getSamlDbUser();
          String profileDbUser = im.getProfileDbUser();
          String dbGroups = im.getDbGroups();
          boolean forceLowercase = im.getForceLowercase();
          boolean allowDbUserOverride = im.getAllowDbUserOverride();
          if (null == settings.m_autocreate) {
            settings.m_autocreate = autoCreate;
          }

          if (null == settings.m_forceLowercase) {
            settings.m_forceLowercase = forceLowercase;
          }

          /*
           * Order of precedence when configuring settings.m_dbUser:
           *
           * If allowDbUserOverride = true: 1. Value from SAML assertion. 2.
           * Value from connection string setting. 3. Value from credentials
           * profile setting.
           *
           * If allowDbUserOverride = false (default): 1. Value from connection
           * string setting. 2. Value from credentials profile setting. 3. Value
           * from SAML assertion.
           */
          if (allowDbUserOverride) {
            if (null != samlDbUser) {
              settings.m_dbUser = samlDbUser;
            } else if (null != dbUser) {
              settings.m_dbUser = dbUser;
            } else if (null != profileDbUser) {
              settings.m_dbUser = profileDbUser;
            }
          } else {
            if (null != dbUser) {
              settings.m_dbUser = dbUser;
            } else if (null != profileDbUser) {
              settings.m_dbUser = profileDbUser;
            } else if (null != samlDbUser) {
              settings.m_dbUser = samlDbUser;
            }
          }

          if (settings.m_dbGroups.isEmpty() && null != dbGroups) {
            settings.m_dbGroups = Arrays
                .asList((settings.m_forceLowercase ? dbGroups.toLowerCase(Locale.getDefault()) : dbGroups).split(","));
          }
        }
      }

      if ("*".equals(settings.m_username) && null == settings.m_dbUser) {
        RedshiftException err = new RedshiftException(
            GT.tr("Missing connection property {0}", RedshiftProperty.DB_USER.getName()),
            RedshiftState.UNEXPECTED_ERROR);

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.ERROR, err.toString());

        throw err;
      }
    } // V1 Or IAM_V2 for provisional cluster or serverless
    else {
      // TODO not yet decided
      if (RedshiftLogger.isEnable())
        log.log(LogLevel.DEBUG, "groupFederation=" + settings.m_groupFederation);

      // Check for GetClusterCredentialsV2 cache
      // Combine key of IDP and V2 API
      String key = null;
      GetClusterCredentialsWithIAMResult credentials = null;

      if (!settings.m_iamDisableCache) {
        key = getCredentialsV2CacheKey(settings, providerType, provider, getClusterCredentialApiType, false);
        credentials = credentialsV2Cache.get(key);
      }

      if (credentials == null
          || RequestUtils.isCredentialExpired(credentials.getExpiration())) {
        // If not found or expired
        // Get IDP token
        if (providerType == CredentialProviderType.PLUGIN) {
          IPlugin plugin = (IPlugin) provider;

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, "Calling plugin.getIdpToken()");

          idpToken = plugin.getIdpToken();
        }

        settings.m_idpToken = idpToken;
      }
    } // Group federation API for plugin

    setClusterCredentials(provider, settings, log, providerType, idpCredentialsRefresh, getClusterCredentialApiType);
  }

  /**
   * Calls the AWS SDK methods to return temporary credentials. The expiration
   * date is returned as the local time set by the client machines OS.
   *
   * @throws RedshiftException
   *           If getting the cluster credentials fails.
   */
  private static void setClusterCredentials(AWSCredentialsProvider credProvider, RedshiftJDBCSettings settings,
      RedshiftLogger log, CredentialProviderType providerType, boolean idpCredentialsRefresh,
      int getClusterCredentialApiType) throws RedshiftException {
    try {

      AmazonRedshiftClientBuilder builder = AmazonRedshiftClientBuilder.standard();
      builder = (AmazonRedshiftClientBuilder) setBuilderConfiguration(settings, log, builder);

      switch (getClusterCredentialApiType) {
        case GET_CLUSTER_CREDENTIALS_V1_API:
          // Call Provision cluster V1 API
          AmazonRedshift client = builder.withCredentials(credProvider).build();

          callDescribeCustomDomainNameAssociationsAPIForV1(settings, client, log);
          callDescribeClustersAPIForV1(settings, client);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, "Call V1 API of GetClusterCredentials");

          GetClusterCredentialsResult result = getClusterCredentialsResult(settings, client, log, providerType,
                  idpCredentialsRefresh);

          settings.m_username = result.getDbUser();
          settings.m_password = result.getDbPassword();

          if (RedshiftLogger.isEnable())
          {
            Date now = new Date();
            log.logInfo(now + ": Using GetClusterCredentialsResult with expiration " + result.getExpiration());
          }

          break;

        case GET_SERVERLESS_CREDENTIALS_V1_API:
          // Serverless V1 API
          ServerlessIamHelper serverlessIamHelper = new ServerlessIamHelper(settings, log, credProvider);

          if (null == settings.m_host || settings.m_port == 0) {
            serverlessIamHelper.describeConfiguration(settings);
          }

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, "Call Serverless V1 API of GetCredentials");

          serverlessIamHelper.getCredentialsResult(settings, providerType, idpCredentialsRefresh);

          break;

        case GET_CLUSTER_CREDENTIALS_IAM_V2_API:
          // Call V2 IAM API Provision

          AmazonRedshiftClient iamClient = (AmazonRedshiftClient) builder.withCredentials(credProvider).build();

          callDescribeCustomDomainNameAssociationsAPIForV2(settings, iamClient, log);
          callDescribeClustersAPIForV2(settings, iamClient);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, "Call V2 API of GetClusterCredentials");

          GetClusterCredentialsWithIAMResult iamResult = getClusterCredentialsResultV2(settings, iamClient, log, providerType,
                  idpCredentialsRefresh, credProvider, getClusterCredentialApiType);

          settings.m_username = iamResult.getDbUser();
          settings.m_password = iamResult.getDbPassword();

          // result will contain TimeToRefresh
          if (RedshiftLogger.isEnable()) {
            Date now = new Date();
            log.logInfo(now + ": Using GetClusterCredentialsResultV2 with expiration " + iamResult.getExpiration());
            log.logInfo(now + ": Using GetClusterCredentialsResultV2 with TimeToRefresh " + iamResult.getNextRefreshTime());
          }

          break;
      }
    }
    catch (AmazonClientException e)
    {
      RedshiftException err = new RedshiftException(GT.tr("IAM error retrieving temp credentials: {0}", e.getMessage()),
          RedshiftState.UNEXPECTED_ERROR, e);

      if (RedshiftLogger.isEnable())
        log.log(LogLevel.ERROR, err.toString());

      throw err;
    }
  }

  /**
   * Helper function to call the DescribeClustersAPIForV2 for IAM clients for provisioned clusters
   */
  static void callDescribeClustersAPIForV2(RedshiftJDBCSettings settings, AmazonRedshiftClient iamClient)
  {
    if (null == settings.m_host || settings.m_port == 0)
    {
      DescribeClustersRequest req = new DescribeClustersRequest();
      req.setClusterIdentifier(settings.m_clusterIdentifier);
      DescribeClustersResult resp = iamClient.describeClusters(req);
      List<Cluster> clusters = resp.getClusters();
      if (clusters.isEmpty()) {
        throw new AmazonClientException("Failed to describeClusters.");
      }

      Cluster cluster = clusters.get(0);
      Endpoint endpoint = cluster.getEndpoint();
      if (null == endpoint) {
        throw new AmazonClientException("Cluster is not fully created yet.");
      }

      settings.m_host = endpoint.getAddress();
      settings.m_port = endpoint.getPort();
    }
  }

  /**
   * Helper function to call the DescribeClustersAPIForV1 for provisioned clusters
   */
  static void callDescribeClustersAPIForV1(RedshiftJDBCSettings settings, AmazonRedshift client)
  {
    if (null == settings.m_host || settings.m_port == 0)
    {
      DescribeClustersRequest req = new DescribeClustersRequest();
      req.setClusterIdentifier(settings.m_clusterIdentifier);
      DescribeClustersResult resp = client.describeClusters(req);
      List<Cluster> clusters = resp.getClusters();
      if (clusters.isEmpty()) {
        throw new AmazonClientException("Failed to describeClusters.");
      }

      Cluster cluster = clusters.get(0);
      Endpoint endpoint = cluster.getEndpoint();
      if (null == endpoint) {
        throw new AmazonClientException("Cluster is not fully created yet.");
      }

      settings.m_host = endpoint.getAddress();
      settings.m_port = endpoint.getPort();
    }
  }

  /**
   * Helper function to call the DescribeCustomDomainNameAssociationsAPI for IAM clients for provisioned clusters
   */
  static void callDescribeCustomDomainNameAssociationsAPIForV2(RedshiftJDBCSettings settings, AmazonRedshiftClient iamClient, RedshiftLogger log) throws RedshiftException
  {
    if(settings.m_isCname)
    {
      DescribeCustomDomainAssociationsRequest describeRequest = new DescribeCustomDomainAssociationsRequest();

      if(null != settings.m_host)
      {
        // this is traditional case where we pass in the host, aka custom domain name to the API
        log.logInfo("calling describe cname associations API with hostname : " + settings.m_host);

        describeRequest.setCustomDomainName(settings.m_host);
      }
      else if (null != settings.m_clusterIdentifier)
      {
        // this case is for the url format clusterID:region, and user passes in cname:region instead
        log.logInfo("calling describe cname associations API with clusterID : " + settings.m_clusterIdentifier);

        describeRequest.setCustomDomainName(settings.m_clusterIdentifier);
      }
      else
      {
        log.logInfo("No CNAME provided. No-op.");
        return;
      }

      try
      {
        DescribeCustomDomainAssociationsResult describeResponse = iamClient.describeCustomDomainAssociations(describeRequest);
        List<Association> associations = describeResponse.getAssociations();
        // API itself will throw if result list's count is 0, so we enter catch case
        if(associations.stream().count() > 1)
        {
          log.logInfo("Multiple associations received for provided custom domain name : " + describeRequest.getCustomDomainName() + ". Only one expected.");
          return;
        }

        settings.m_clusterIdentifier = describeResponse.getAssociations().get(0).getCertificateAssociations().get(0).getClusterIdentifier();
      }
      catch (Exception ex)
      {
        log.logInfo("No cluster identifier received from Redshift CNAME lookup. Setting CNAME to false.");
        settings.m_isCname = false;      }
    }
  }

  /**
   * Helper function to call the DescribeCustomDomainNameAssociationsAPI for provisioned clusters
   */
  static void callDescribeCustomDomainNameAssociationsAPIForV1(RedshiftJDBCSettings settings, AmazonRedshift client, RedshiftLogger log) throws RedshiftException
  {
    if(settings.m_isCname)
    {
      DescribeCustomDomainAssociationsRequest describeRequest = new DescribeCustomDomainAssociationsRequest();

      if(null != settings.m_host)
      {
        // this is traditional case where we pass in the host, aka custom domain name to the API
        log.logInfo("calling describe cname associations API with hostname : " + settings.m_host);

        describeRequest.setCustomDomainName(settings.m_host);
      }
      else if (null != settings.m_clusterIdentifier)
      {
        // this case is for the url format clusterID:region, and user passes in cname:region instead
        log.logInfo("calling describe cname associations API with clusterID : " + settings.m_clusterIdentifier);

        describeRequest.setCustomDomainName(settings.m_clusterIdentifier);
      }
      else
      {
        log.logInfo("No CNAME provided. No-op.");
        return;
      }

      try
      {
        DescribeCustomDomainAssociationsResult describeResponse = client.describeCustomDomainAssociations(describeRequest);
        List<Association> associations = describeResponse.getAssociations();
        // API itself will throw if result list's count is 0, so we enter catch case
        if(associations.stream().count() > 1)
        {
          log.logInfo("Multiple associations received for provided custom domain name : " + describeRequest.getCustomDomainName() + ". Only one expected.");
          return;
        }

        settings.m_clusterIdentifier = describeResponse.getAssociations().get(0).getCertificateAssociations().get(0).getClusterIdentifier();
      }
      catch (Exception ex)
      {
        log.logInfo("No cluster identifier received from Redshift CNAME lookup. Setting CNAME to false.");
        settings.m_isCname = false;
      }
    }
  }

  private static synchronized GetClusterCredentialsResult getClusterCredentialsResult(RedshiftJDBCSettings settings,
      AmazonRedshift client, RedshiftLogger log, CredentialProviderType providerType, boolean idpCredentialsRefresh)
      throws AmazonClientException {

    String key = null;
    GetClusterCredentialsResult credentials = null;

    if (!settings.m_iamDisableCache) {
      key = getCredentialsCacheKey(settings, providerType, false);
      credentials = credentialsCache.get(key);
    }

    if (credentials == null || (providerType == CredentialProviderType.PLUGIN && idpCredentialsRefresh)
        || RequestUtils.isCredentialExpired(credentials.getExpiration())) {
      if (RedshiftLogger.isEnable())
        log.logInfo("GetClusterCredentials NOT from cache");

      if (!settings.m_iamDisableCache)
        credentialsCache.remove(key);

      if(settings.m_isCname)
      {
        // construct request packet with cname
        GetClusterCredentialsRequest request = constructRequestForGetClusterCredentials(settings, true, log);

        try
        {
          // make api call with cname
          credentials = makeGetClusterCredentialsAPICall(request, credentials, client, log);
        }
        catch(AmazonClientException ace)
        {
          // if api call with cname fails, recreate request packet with clusterid and re-make api call

          if(RedshiftLogger.isEnable())
          {
            log.logInfo("GetClusterCredentials API call failed with CNAME request. Retrying with ClusterID.");
          }

            request = constructRequestForGetClusterCredentials(settings, false, log);
            credentials = makeGetClusterCredentialsAPICall(request, credentials, client, log);
        }
      }
      else
      {
        // construct request packet with clusterid and make api call
        GetClusterCredentialsRequest request = constructRequestForGetClusterCredentials(settings, false, log);
        credentials = makeGetClusterCredentialsAPICall(request, credentials, client, log);
      }

      if (!settings.m_iamDisableCache)
        credentialsCache.put(key, credentials);
    }
    else
    {
      if (RedshiftLogger.isEnable())
        log.logInfo("GetClusterCredentials from cache");
    }

    return credentials;
  }

  /**
   * Helper function to construct the request object for GetClusterCredentials API
   */
  static GetClusterCredentialsRequest constructRequestForGetClusterCredentials(RedshiftJDBCSettings settings, boolean constructWithCname, RedshiftLogger log)
  {
    GetClusterCredentialsRequest request = new GetClusterCredentialsRequest();

    if (settings.m_iamDuration > 0)
    {
      request.setDurationSeconds(settings.m_iamDuration);
    }

    request.setDbName(settings.m_Schema);
    request.setDbUser(settings.m_dbUser == null ? settings.m_username : settings.m_dbUser);
    request.setAutoCreate(settings.m_autocreate);
    request.setDbGroups(settings.m_dbGroups);

    if(constructWithCname)
    {
      request.setCustomDomainName(settings.m_host);
    }
    else
    {
      request.setClusterIdentifier(settings.m_clusterIdentifier);
    }

    if (RedshiftLogger.isEnable())
    {
      log.logInfo(request.toString());
    }

    return request;
  }

  /**
   * Helper function to make the API call to GetClusterCredentials
   */
  static GetClusterCredentialsResult makeGetClusterCredentialsAPICall(GetClusterCredentialsRequest request, GetClusterCredentialsResult credentials, AmazonRedshift client, RedshiftLogger log)
  {
    for (int i = 0; i < MAX_AMAZONCLIENT_RETRY; ++i)
    {
      try
      {
        credentials = client.getClusterCredentials(request);
        break;
      }
      catch (AmazonClientException ce)
      {
        checkForApiCallRateExceedError(ce, i, "getClusterCredentialsResult", log);
      }
    }

    return credentials;
  }

  static void checkForApiCallRateExceedError(AmazonClientException ace, int i, String callerMethod, RedshiftLogger log)
      throws AmazonClientException {
    if (ace.getMessage().contains("Rate exceeded") && i < MAX_AMAZONCLIENT_RETRY - 1) {
      if (RedshiftLogger.isEnable())
        log.logInfo(callerMethod + " caught 'Rate exceeded' error...");
      try {
        Thread.sleep(MAX_AMAZONCLIENT_RETRY_DELAY_MS);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    } else {
      throw ace;
    }
  }

  private static synchronized GetClusterCredentialsWithIAMResult getClusterCredentialsResultV2(
      RedshiftJDBCSettings settings, AmazonRedshiftClient client, RedshiftLogger log,
      CredentialProviderType providerType, boolean idpCredentialsRefresh, AWSCredentialsProvider provider,
      int getClusterCredentialApiType) throws AmazonClientException
  {
    String key = null;
    GetClusterCredentialsWithIAMResult credentials = null;

    if (!settings.m_iamDisableCache)
    {
      key = getCredentialsV2CacheKey(settings, providerType, provider, getClusterCredentialApiType, false);
      credentials = credentialsV2Cache.get(key);
    }

    if (credentials == null || (providerType == CredentialProviderType.PLUGIN && settings.m_idpToken != null)
        || RequestUtils.isCredentialExpired(credentials.getExpiration()))
    {
      if (RedshiftLogger.isEnable())
        log.logInfo("GetClusterCredentialsV2 NOT from cache");

      if (!settings.m_iamDisableCache)
        credentialsV2Cache.remove(key);

      if(settings.m_isCname)
      {
        // construct request packet with cname
        GetClusterCredentialsWithIAMRequest request = constructRequestForGetClusterCredentialsWithIAM(settings, true, log);

        try
        {
          // make api call with cname
          credentials = makeGetClusterCredentialsWithIAMAPICall(request, credentials, client, log);
        }
        catch (AmazonClientException ce)
        {
          // if api call with cname fails, recreate request packet with clusterid and re-make api call

          if(RedshiftLogger.isEnable())
          {
            log.logInfo("GetClusterCredentials API call failed with CNAME request. Retrying with ClusterID.");
          }

          request = constructRequestForGetClusterCredentialsWithIAM(settings, false, log);
          credentials = makeGetClusterCredentialsWithIAMAPICall(request, credentials, client, log);
        }
      }
      else
      {
        // construct request packet with clusterid and make api call
        GetClusterCredentialsWithIAMRequest request = constructRequestForGetClusterCredentialsWithIAM(settings, false, log);
        credentials = makeGetClusterCredentialsWithIAMAPICall(request, credentials, client, log);
      }

      if (!settings.m_iamDisableCache)
        credentialsV2Cache.put(key, credentials);
    }
    else
    {
      if (RedshiftLogger.isEnable())
        log.logInfo("GetClusterCredentialsV2 from cache");
    }

    return credentials;
  }

  /**
   * Helper function to construct the request object for GetClusterCredentialsWithIAM API
   */
  static GetClusterCredentialsWithIAMRequest constructRequestForGetClusterCredentialsWithIAM(RedshiftJDBCSettings settings, boolean constructWithCname, RedshiftLogger log)
  {
    GetClusterCredentialsWithIAMRequest request = new GetClusterCredentialsWithIAMRequest();

    if (settings.m_iamDuration > 0) {
      request.setDurationSeconds(settings.m_iamDuration);
    }
    request.setDbName(settings.m_Schema);

    if (constructWithCname)
    {
      request.setCustomDomainName(settings.m_host);
    }
    else
    {
      request.setClusterIdentifier(settings.m_clusterIdentifier);
    }

    if (RedshiftLogger.isEnable())
      log.logInfo(request.toString());

    return request;
  }

  /**
   * Helper function to make the API call to GetClusterCredentialsWithIAM
   */
  static GetClusterCredentialsWithIAMResult makeGetClusterCredentialsWithIAMAPICall(GetClusterCredentialsWithIAMRequest request, GetClusterCredentialsWithIAMResult credentials, AmazonRedshiftClient client, RedshiftLogger log)
  {
    for (int i = 0; i < MAX_AMAZONCLIENT_RETRY; ++i)
    {
      try
      {
        credentials = client.getClusterCredentialsWithIAM(request);
        break;
      }
      catch (AmazonClientException ace)
      {
        checkForApiCallRateExceedError(ace, i, "getClusterCredentialsResultV2", log);
      }
    }

    return credentials;
  }

  static String getCredentialsCacheKey(RedshiftJDBCSettings settings, CredentialProviderType providerType,
      boolean serverless) {
    String key;
    String dbGroups = "";

    if (settings.m_dbGroups != null && !settings.m_dbGroups.isEmpty()) {
      Collections.sort(settings.m_dbGroups);
      dbGroups = String.join(",", settings.m_dbGroups);
    }

    key = ((!serverless) ? settings.m_clusterIdentifier : settings.m_acctId) + ";"
        + ((serverless && settings.m_workGroup != null) ? settings.m_workGroup : "") + ";"        
        + (settings.m_dbUser == null ? settings.m_username : settings.m_dbUser) + ";"
        + (settings.m_Schema == null ? "" : settings.m_Schema) + ";" + dbGroups + ";" + settings.m_autocreate + ";"
        + settings.m_iamDuration;

    switch (providerType) {
    case PROFILE: {
      key += ";" + settings.m_profile;
      break;
    }

    case IAM_KEYS_WITH_SESSION: {
      key += ";" + settings.m_iamAccessKeyID + ";" + settings.m_iamSecretKey + ";" + settings.m_iamSessionToken;
      break;
    }

    case IAM_KEYS: {
      key += ";" + settings.m_iamAccessKeyID + ";" + settings.m_iamSecretKey;
      break;
    }

    default: {
      break;
    }

    } // Switch

    return key;
  }

  static String getCredentialsV2CacheKey(RedshiftJDBCSettings settings, CredentialProviderType providerType,
      AWSCredentialsProvider provider, int getClusterCredentialApiType, boolean serverless) {
    String key = "";

    if (providerType == CredentialProviderType.PLUGIN) {
      // Get IDP key
      IPlugin plugin = (IPlugin) provider;
      key = plugin.getCacheKey();
    }

    // Combine IDP key with V2 API parameters

    key += (((!serverless) ? settings.m_clusterIdentifier : settings.m_acctId) + ";"
        + ((serverless && settings.m_workGroup != null) ? settings.m_workGroup : "") + ";"        
        + (settings.m_Schema == null ? "" : settings.m_Schema) + ";" + settings.m_iamDuration);

    if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_SAML_V2_API) {
      if (settings.m_preferredRole != null) {
        key += (settings.m_preferredRole + ";");
      }

      if (settings.m_dbGroupsFilter != null) {
        key += (settings.m_dbGroupsFilter + ";");
      }
    } else if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_JWT_V2_API) {
      if (settings.m_idpToken != null) {
        key += (settings.m_idpToken + ";");
      }

      if (settings.m_roleArn != null) {
        key += (settings.m_roleArn + ";");
      }

      if (settings.m_roleSessionName != null) {
        key += (settings.m_roleSessionName + ";");
      }
    }

    switch (providerType) {
    case PROFILE: {
      key += ";" + settings.m_profile;
      break;
    }

    case IAM_KEYS_WITH_SESSION: {
      key += ";" + settings.m_iamAccessKeyID + ";" + settings.m_iamSecretKey + ";" + settings.m_iamSessionToken;
      break;
    }

    case IAM_KEYS: {
      key += ";" + settings.m_iamAccessKeyID + ";" + settings.m_iamSecretKey;
      break;
    }

    default: {
      break;
    }

    } // Switch

    return key;
  }

  private static int findTypeOfGetClusterCredentialsAPI(RedshiftJDBCSettings settings,
      CredentialProviderType providerType, AWSCredentialsProvider provider) {

    if (!settings.m_isServerless)
    {
      if (!settings.m_groupFederation)
        return GET_CLUSTER_CREDENTIALS_V1_API;
      else
      {
          return GET_CLUSTER_CREDENTIALS_IAM_V2_API;
      }
    } else {
      // Serverless
        return GET_SERVERLESS_CREDENTIALS_V1_API;
    }
  }

  static AwsClientBuilder setBuilderConfiguration(RedshiftJDBCSettings settings, RedshiftLogger log,
      AwsClientBuilder builder) {
    ClientConfiguration clientConfig = RequestUtils.getProxyClientConfig(log);

    if (clientConfig != null) {
      builder.setClientConfiguration(clientConfig);
    }

    if (RedshiftLogger.isEnable()) {
      log.logInfo("setBuilderConfiguration: settings.m_endpoint= " + settings.m_endpoint + " settings.m_awsRegion = "
          + settings.m_awsRegion);
    }

    if (settings.m_endpoint != null) {
      EndpointConfiguration cfg = new EndpointConfiguration(settings.m_endpoint, settings.m_awsRegion);
      builder.setEndpointConfiguration(cfg);
    } else if (settings.m_awsRegion != null && !settings.m_awsRegion.isEmpty()) {
      builder.setRegion(settings.m_awsRegion);
    }

    return builder;
  }
}
