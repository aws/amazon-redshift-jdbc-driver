package com.amazon.redshift.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.redshift.AuthMech;

public class RedshiftJDBCSettings
{
    /*
     * Static variable(s) ==========================================================================
     */

    /*
     * Instance variable(s) ========================================================================
     */

    /**
     *  The host to connect to.
     */
    public String m_host;

    /**
     *  The port to connect to.
     */
    public int m_port;

    /**
     *  The timeout. 0 indicates no timeout.
     */
//    public int m_loginTimeoutMS;

    /**
     *  The number of rows to fetch for each request.
     */
//    public int m_rowsFetchedPerBlock;

    /**
     *  The size of columns with types that have undefinded lengths.
     */
//    public Integer m_unknownLength;

    /**
     *  The Authentication Mechanism to use.
     */
    public AuthMech m_authMech;

    /**
     *  The user name.
     */
    public String m_username;

    /**
     *  The password.
     */
    public String m_password;

    /**
     *  The Kerberos realm.
     */
//    public String m_krbRealm;

    /**
     *  The Kerberos service name.
     */
//    public String m_krbServiceName;

    /**
     *  The host fully-qualified domain name.
     */
//    public String m_krbHostFQDN;

    /**
     *  The path to the SSL Keytstore file.
     */
//    public String m_sslKeyStore;

    /**
     *  The password for the SSL Keystore file.
     */
//    public String m_sslKeyStorePwd;

    /**
     *  The password for the key file.
     */
//    public String m_sslPassword;

    /**
     *  The path to the key file.
     */
//    public String m_sslKey;

    /**
     *  The path to the server certificate file.
     */
//    public String m_sslCert;

    /**
     *  The path to the CA certificate file (root.crt).
     */
//    public String m_sslRootCert;

    /**
     *  The setting for the default used schema.
     *  This is a DBNAME.
     */
    public String m_Schema;

    /**
     *  The delegation UID.
     */
//    public String m_delegationUID;

    /**
     * How many rows to limit the fetch to. 0 will not limit the fetch.
     */
//    public int m_nRowMode;

    /**
     * How many minutes of inactivity must happen prior to a keepalive being issued
     * This is the "new tcp connection" style, where another connection is attempted.
     * If the new connection fails, then the socket is considered dead.
     */
//    public int m_newTCPConnectionKeepAliveMinutes;

    /**
     * The Filter Level that will be used by the client for incoming error and notice logs
     */
//    public String m_filterLevel;

    /**
     * The current socket timeout value in Milliseconds.
     */
//    public int m_socketTimeoutMS;

    /**
     * Indicates whether the isValid() query should be disabled.
     * The default is false, meaning the query is NOT disabled.
     */
//    public boolean m_disableIsValid;

    /**
     * Indicates whether use IAM authentication.
     */
    public boolean m_iamAuth;

    /**
     * The IAM access key id for the IAM user or role.
     */
    public String m_iamAccessKeyID;

    /**
     * The IAM secret key for the IAM user or role.
     */
    public String m_iamSecretKey;

    /**
     * The IAM security token for an IAM user or role.
     */
    public String m_iamSessionToken;

    /**
     * The AWS profile name for credentials.
     */
    public String m_profile;

    /**
     * A external id string for AssumeRole request.
     */
    public String m_externalId;

    /**
     * The name of the Redshift Cluster to use.
     */
    public String m_clusterIdentifier;

    /**
     * The time in seconds until the temporary IAM credentials expire.
     * Range: 900 - 3600
     */
    public int m_iamDuration;

    /**
     * Indicates whether the user should be created if not exists.
     * Default is false.
     */
    public Boolean m_autocreate;

    /**
     *  The database user name for IAM authentication.
     */
    public String m_dbUser;

    /**
     * A list of database group names to join.
     */
    public List<String> m_dbGroups;

    /**
     * Forces the database group names to be lower case.
     */
    public Boolean m_forceLowercase;
    
    /**
     * The AWS endpoint url for Redshift.
     */
    public String m_endpoint;

    /**
     * The AWS endpoint url for STS.
     */
    public String m_stsEndpoint;
    
    /**
     * The AWS region where the cluster specified by m_clusterIdentifier is located.
     */
    public String m_awsRegion;

    /**
     * The fully qualified class path to a class that implements AWSCredentialsProvider.
     */
    public String m_credentialsProvider;

    /**
     * Connection specific trust store path
     */
//    public String m_sslTrustStorePath;

    /**
     * Connection specific trust store pwd
     */
//    public String m_sslTrustStorePwd;

    /**
     * The plugin arguments.
     */
    public Map<String, String> m_pluginArgs = new HashMap<String, String>();

    /**
     * Indicates whether the schema pattern has a match in external schemas.
     */
//    public boolean m_hasExtSchemaPatternMatch;

    /**
     * Name of a class to use as a SelectorProvider.
     */
//    public String m_selectorProvider;

    /**
     * A String to pass as an argument to the selectorProvider constructor.
     */
//    public String m_selectorProviderArg;
}
