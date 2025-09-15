/**
 * Copyright 2010-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.amazon.redshift.plugin;

import com.amazon.redshift.INativePlugin;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.RedshiftException;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

public abstract class CommonCredentialsProvider extends IdpCredentialsProvider implements INativePlugin {
    private static final Map<String, NativeTokenHolder> m_cache = new HashMap<String, NativeTokenHolder>();
    protected Boolean m_disableCache = true;
    private NativeTokenHolder m_lastRefreshCredentials; // Used when cache is disabled

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
    private static final ClassLoader CONTEXT_CLASS_LOADER = new ClassLoader(CommonCredentialsProvider.class.getClassLoader()) {
        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            Class<?> clazz = getParent().loadClass(name);
            return clazz;
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (LogFactory.FACTORY_PROPERTIES.equals(name)) {
                // make sure to not load any other commons-logging.properties files
                return Collections.enumeration(Collections.emptyList());
            }
            return super.getResources(name);
        }

        @Override
        public URL getResource(String name) {
            if (LOG_PROPERTIES_FILE_PATH.equals(name)) {
                return CommonCredentialsProvider.class.getResource(LOG_PROPERTIES_FILE_NAME);
            }
            return super.getResource(name);
        }
    };

    @Override
    public void addParameter(String key, String value) {
        if (RedshiftLogger.isEnable())
            m_log.logDebug("add parameter key: {0}", key);
    }

    @Override
    public void setLogger(RedshiftLogger log) {
        m_log = log;
    }

    @Override
    public NativeTokenHolder getCredentials() throws RedshiftException {
        NativeTokenHolder credentials = null;

        if (!m_disableCache) {
            String key = getCacheKey();
            credentials = m_cache.get(key);
        }

        if (credentials == null || credentials.isExpired()) {
            if (RedshiftLogger.isEnable()) {
                if (m_disableCache) {
                    m_log.logInfo("Auth token Cache disabled : fetching new token");
                } else {
                    m_log.logInfo("Auth token Cache enabled - No auth token found from cache : fetching new token");
                }
            }

            synchronized (this) {
                refresh();

                if (m_disableCache) {
                    credentials = m_lastRefreshCredentials;
                    m_lastRefreshCredentials = null;
                }
            }
        } else {
            credentials.setRefresh(false);
            if (RedshiftLogger.isEnable())
                m_log.logInfo("Auth token found from cache");
        }

        if (!m_disableCache) {
            credentials = m_cache.get(getCacheKey());
        }

        if (credentials == null) {
            m_log.logError("No credentials found");
            throw new RedshiftException("There was an error during authentication.");
        }

        return credentials;
    }

    protected abstract NativeTokenHolder getAuthToken() throws IOException, URISyntaxException;

    @Override
    public void refresh() throws RedshiftException {
        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try {
            NativeTokenHolder authTokenHolder = getAuthToken();
            authTokenHolder.setRefresh(true);

            if (!m_disableCache)
                m_cache.put(getCacheKey(), authTokenHolder);
            else
                m_lastRefreshCredentials = authTokenHolder;
        } catch (IOException ex) {
            if(RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "IOException while refreshing token");
            throw new RedshiftException(!isNullOrEmpty(ex.getMessage()) ? ex.getMessage() : "There was an error during authentication.", ex);
        } catch (Exception ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Exception while refreshing token");
            throw new RedshiftException("There was an error during authentication.", ex);
        } finally {
            currentThread.setContextClassLoader(cl);
        }
    }

    @Override
    public String getIdpToken() throws RedshiftException {

        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try {
            NativeTokenHolder authTokenHolder = getAuthToken();
            return authTokenHolder.getAccessToken();
        } catch (IOException ex) {
            if(RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "IOException during getIdpToken");
            throw new RedshiftException(!isNullOrEmpty(ex.getMessage()) ? ex.getMessage() : "There was an error during authentication.", ex);
        } catch (Exception ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Exception during getIdpToken");
            throw new RedshiftException("There was an error during authentication.", ex);
        } finally {
            currentThread.setContextClassLoader(cl);
        }
    }

    @Override
    public String getPluginSpecificCacheKey() {
        // This needs to be overridden this in each derived plugin.
        return "";
    }

    @Override
    public String getCacheKey() {
        return getPluginSpecificCacheKey();
    }

}
