package com.amazon.redshift.core;

import com.amazon.redshift.util.RedshiftProperties;
import com.amazonaws.util.StringUtils;
import com.amazon.redshift.INativePlugin;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

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

      String idpToken = getNativeAuthPluginCredentials(settings, log, authProfile);
      if (RedshiftLogger.isEnable())
        log.logInfo("NativeAuthPluginHelper: Obtained idp token of length={0}", idpToken != null ? idpToken.length() : -1);
      info.put(RedshiftProperty.WEB_IDENTITY_TOKEN.getName(), idpToken);
      
      return info;
    } catch (RedshiftException re) {
      if (RedshiftLogger.isEnable())
        log.logError(re);

      throw re;
    }
  }

  /**
   * Helper function to create the appropriate IDP token.
   *
   * @throws RedshiftException
   *           If an unspecified error occurs.
   */
  private static String getNativeAuthPluginCredentials(RedshiftJDBCSettings settings, RedshiftLogger log, String authProfile) throws RedshiftException {
    String idpToken = null;
    INativePlugin provider = null;

    if (!StringUtils.isNullOrEmpty(settings.m_credentialsProvider)) {
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

    if (credentials == null
        || 
        (credentials.getExpiration() != null 
            && credentials.getExpiration().before(new Date(System.currentTimeMillis() - 60 * 1000 * 5))
         )
        ) {
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
    
    return idpToken;
  }
}
