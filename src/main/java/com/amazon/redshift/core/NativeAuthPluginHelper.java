package com.amazon.redshift.core;

import com.amazon.redshift.plugin.utils.RequestUtils;
import com.amazon.redshift.util.RedshiftProperties;
import com.amazon.redshift.INativePlugin;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.IdpTokenAuthPlugin;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Map;

public final class NativeAuthPluginHelper extends IdpAuthHelper {

  private NativeAuthPluginHelper()  {
  }

  /**
   * Helper function to handle Native Auth Plugin connection properties. 
   * If any Plugin related
   * connection property is specified, all other <b>required</b> IAM properties
   * must be specified too or else it throws an error.
   *
   * @param info
   *          Redshift client settings used to authenticate if connection should
   *          be granted.
   * @param settings
   *          Redshift Native Plugin settings
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
  public static RedshiftProperties setNativeAuthPluginProperties(RedshiftProperties info, RedshiftJDBCSettings settings, RedshiftLogger log)
      throws RedshiftException {
    try {
        String authProfile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AUTH_PROFILE.getName(), info);

      // Common code for IAM and Native Auth
      info = setAuthProperties(info, settings, log);

      // Extract host from connection settings for potential use in identity-enhanced credentials flow
      String host = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.HOST.getName(), info);

      String idpToken = getNativeAuthPluginCredentials(settings, log, authProfile, host);
      if (RedshiftLogger.isEnable())
        log.logInfo("NativeAuthPluginHelper: Obtained idp token of length={0}", idpToken != null ? idpToken.length() : -1);
      info.put(RedshiftProperty.WEB_IDENTITY_TOKEN.getName(), idpToken);
      
      // Propagate token_type from plugin args to connection properties for startup packet
      // This is especially needed for IdC enhanced credentials credentials with
      // IdpTokenAuthPlugin since token_type is not passed in as an original connection
      // property.
      String tokenType = settings.m_pluginArgs.get("token_type");
      if (!Utils.isNullOrEmpty(tokenType)) {
        info.put(RedshiftProperty.TOKEN_TYPE.getName(), tokenType);
        if (RedshiftLogger.isEnable()){
          log.logDebug("Set token_type in connection properties: {0}", tokenType);
        }
      }
      return info;
    } catch (RedshiftException re) {
      if (RedshiftLogger.isEnable()){
        log.logError(re);
      }

      throw re;
    }
  }

  /**
   * Helper function to create the appropriate IDP token.
   *
   * @throws RedshiftException
   *           If an unspecified error occurs.
   */
  private static String getNativeAuthPluginCredentials(RedshiftJDBCSettings settings, RedshiftLogger log, String authProfile, String host) throws RedshiftException {
    String idpToken = null;
    INativePlugin provider = null;

    if (!Utils.isNullOrEmpty(settings.m_credentialsProvider)) {
      try {
        Class<? extends INativePlugin> clazz = (Class.forName(settings.m_credentialsProvider)
            .asSubclass(INativePlugin.class));

        provider = clazz.newInstance();
        if (provider instanceof INativePlugin) {
          INativePlugin plugin = ((INativePlugin) provider);

          plugin.setLogger(log);
          for (Map.Entry<String, String> entry : settings.m_pluginArgs.entrySet()) {
            String pluginArgKey = entry.getKey();
            plugin.addParameter(pluginArgKey, entry.getValue());
          } // For loop
        }
        else {
          RedshiftException err = new RedshiftException(
              GT.tr("Invalid credentials provider class {0}", settings.m_credentialsProvider),
              RedshiftState.UNEXPECTED_ERROR);

          if (RedshiftLogger.isEnable())
            log.log(LogLevel.ERROR, err.toString());

          throw err;
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
      } catch (IllegalArgumentException e) {
        RedshiftException err = new RedshiftException(
            GT.tr("Error initializing credentials provider: {0}", e.getMessage()),
            RedshiftState.UNEXPECTED_ERROR, e);

        if (RedshiftLogger.isEnable()) {
          log.log(LogLevel.ERROR, err.toString());
        }

        throw err;
      }
    }
    else {
      RedshiftException err = new RedshiftException(
          GT.tr("Required credentials provider class parameter is null or empty {0}", settings.m_credentialsProvider),
          RedshiftState.UNEXPECTED_ERROR);

      if (RedshiftLogger.isEnable())
        log.log(LogLevel.ERROR, err.toString());

      throw err;
    }

    if (RedshiftLogger.isEnable())
      log.log(LogLevel.DEBUG, "IDP Credential Provider {0}:{1}", provider, settings.m_credentialsProvider);

    if (RedshiftLogger.isEnable())
      log.log(LogLevel.DEBUG, "Calling provider.getCredentials()");

    settings.m_idpToken = idpToken;
    
    // Provider will cache the credentials, it's OK to call getCredentials()
    // here.
    NativeTokenHolder credentials = provider.getCredentials();

    if (credentials == null || RequestUtils.isCredentialExpired(credentials.getExpiration())) {
        // If not found or expired
        // Get IDP token
        IPlugin plugin = (IPlugin) provider;

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.DEBUG, "Calling plugin.getIdpToken()");

        idpToken = plugin.getIdpToken();
        settings.m_idpToken = idpToken;
    }
    else {
      idpToken = credentials.getAccessToken();
    }

    // For IdpTokenAuthPlugin, set token_type based on authentication method
    // For direct token flow, use whatever token_type customer provided (or leave empty)
    if (settings.m_credentialsProvider != null && 
        settings.m_credentialsProvider.equals(IdpTokenAuthPlugin.class.getName())) {
      
        // Use the plugin's method to check if using identity-enhanced credentials
        if (provider instanceof INativePlugin) {
          INativePlugin nativePlugin = (INativePlugin) provider;
          if (nativePlugin.isUsingIdentityEnhancedCredentials()) {
            // Identity-enhanced credentials flow
            settings.m_pluginArgs.put("token_type", "SUBJECT_TOKEN");
            if (RedshiftLogger.isEnable()){
              log.log(LogLevel.DEBUG, "Set token_type to SUBJECT_TOKEN for identity-enhanced credentials");
            }
            // Set Host only for identity-enhanced credentials flow (needed to extract cluster name)
            if (!Utils.isNullOrEmpty(host)) {
              settings.m_pluginArgs.put("Host", host);
              if (RedshiftLogger.isEnable())
                log.logDebug("Set Host for IdpTokenAuthPlugin identity-enhanced flow: {0}", host);
            }
          }
        }
    }
    return idpToken;
  }
}
