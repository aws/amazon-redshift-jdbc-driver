package com.amazon.redshift.util;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The utility class implements credentials removal from query text. It is based on the PADB
 * implementation in padb/src/xen_utils/log_statement.cpp with the following change: it will apply
 * to any query text regardless of the SQL statement type.
 */
public class QuerySanitizer {
    // replace all occurrences of
    //    password<space>*['\"].*?['\"]"
    // with
    //    password<space>'***'
    private static final String PWD_REGEX_STR = "(password)\\s*['\"].*?['\"]";
    private static final Pattern PWD_REGEX = Pattern.compile(PWD_REGEX_STR, Pattern.CASE_INSENSITIVE);
    private static final String PWD_REPLACEMENT_REGEX = "$1 '***'";
    // replace all occurrences of:
    //   credentials\s*(as)?\s*['\"].*?['\"]
    // with
    //   credentials (as) ''
    //
    // if no quotes are found after the credentials keyword, then we eat all
    // remaining characters in the query text to avoid accidental exposure in
    // logs, etc. We look for a leading whitespace character before "credentials"
    // to minimize the risk of mismatching any substring
    private static final String CREDS_REGEX_STR =
            "(\\scredentials\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern CREDS_REGEX =
            Pattern.compile(CREDS_REGEX_STR, Pattern.CASE_INSENSITIVE);
    private static final String CREDS_REPLACEMENT_REGEX = "$1'***'";
    // replace all occurrences of:
    //   access_key_id\s*(as)?\s*['\"].*?['\"]
    // with
    //   access_key_id (as) ''
    //
    private static final String ACCESS_KEY_REGEX_STR =
            "(\\saccess_key_id\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern ACCESS_KEY_REGEX =
            Pattern.compile(ACCESS_KEY_REGEX_STR, Pattern.CASE_INSENSITIVE);
    // replace all occurrences of:
    //   secret_access_key\s*(as)?\s*['\"].*?['\"]
    // with
    //   secret_access_key (as) ''
    //
    private static final String SECRET_KEY_REGEX_STR =
            "(\\ssecret_access_key\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern SECRET_KEY_REGEX =
            Pattern.compile(SECRET_KEY_REGEX_STR, Pattern.CASE_INSENSITIVE);
    // replace all occurrences of:
    //   iam_role\s*(as)?\s*['\"].*?['\"]
    // with
    //   iam_role (as) ''
    //
    private static final String IAM_ROLE_REGEX_STR =
            "(\\siam_role\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern IAM_ROLE_REGEX =
            Pattern.compile(IAM_ROLE_REGEX_STR, Pattern.CASE_INSENSITIVE);
    // replace all occurrences of:
    //   master_symmetric_key\s*(as)?\s*['\"].*?['\"]
    // with
    //   master_symmetric_key (as) ''
    //
    private static final String SYM_KEY_REGEX_STR =
            "(\\smaster_symmetric_key\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern SYM_KEY_REGEX =
            Pattern.compile(SYM_KEY_REGEX_STR, Pattern.CASE_INSENSITIVE);
    // replace all occurrences of:
    //   kms_key_id\s*(as)?\s*['\"].*?['\"]
    // with
    //   kms_key_id (as) ''
    //
    private static final String KMS_KEY_REGEX_STR =
            "(\\skms_key_id\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern KMS_KEY_REGEX =
            Pattern.compile(KMS_KEY_REGEX_STR, Pattern.CASE_INSENSITIVE);
    // replace all occurrences of:
    //   session_token\s*(as)?\s*['\"].*?['\"]
    // with
    //   session_token (as) ''
    //
    private static final String SESSION_TOKEN_REGEX_STR =
            "(\\ssession_token\\s*)(as\\s*)?(?:(?:['\"].*?['\"])|.*)";
    private static final Pattern SESSION_TOKEN_REGEX =
            Pattern.compile(SESSION_TOKEN_REGEX_STR, Pattern.CASE_INSENSITIVE);
    // As a last-resort, we replace any lingering secrets in the text
    // For example:
    //
    //   aws_access_key_id=foo     -> aws_access_key_id=***
    //   awsaccesskeyid=foo        -> awsaccesskeyid=***
    //   access_key_id = foo       -> access_key_id=***
    //   access_key = foo          -> access_key=***
    //   accesskeyid = foo         -> accesskeyid=***
    //   aws_secret_access_key=foo -> aws_secret_access_key=***
    //   secret_access_key=foo     -> secret_access_key=***
    //   master_symmetric_key=foo  -> master_symmetric_key=***
    //   mastersymmetrickey=foo    -> mastersymmetrickey=***
    //   symmetric_key=foo         -> symmetric_key=***
    private static final String LINGERING_SECRET_REGEX_STR =
            "(((aws[-_]?)?"
                    + "((iam[-_]?role)"
                    + "|(access[-_]?key([-_]id)?)"
                    + "|(secret[-_]?access[-_]?key))"
                    + ")|(token)|((master[-_]?)?symmetric[-_]?key)"
                    + ")\\s*=\\s*[^;'\"]*";
    private static final Pattern LINGERING_SECRET_REGEX =
            Pattern.compile(LINGERING_SECRET_REGEX_STR, Pattern.CASE_INSENSITIVE);
    private static final String LINGERING_SECRET_REPLACEMENT_REGEX = "$1=***";
    // encryptionKey is the one of the arguments of xpx restore_table in case of
    // encrypted clusters. We want to filter that out before dumping query text
    // to log files.
    // Since the value we want to filter out is base64 encoded, we are using
    // [A-Za-z0-9+/=] to identify that as these are the only characters possible
    // for base64 encoded string.
    private static final String ENCRYPT_KEY_REGEX_STR =
            "((--encryptionKey|--k|-k)\\s+)([A-Za-z0-9+/=]*)";
    private static final Pattern ENCRYPT_KEY_REGEX =
            Pattern.compile(ENCRYPT_KEY_REGEX_STR, Pattern.CASE_INSENSITIVE);
    private static final String ENCRYPT_KEY_REPLACEMENT_REGEX = "$1***";

    public static String filterCredentials(final String queryText) {
        String sanitizedText = processPassword(queryText);
        sanitizedText = processCreds(sanitizedText);
        sanitizedText = processAccessKey(sanitizedText);
        sanitizedText = processSecretKey(sanitizedText);
        sanitizedText = processIamRole(sanitizedText);
        sanitizedText = processSymKey(sanitizedText);
        sanitizedText = processKmsKey(sanitizedText);
        sanitizedText = processSessionToken(sanitizedText);
        sanitizedText = processLingeringSecrets(sanitizedText);
        sanitizedText = processEncryptKey(sanitizedText);
        return sanitizedText;
    }

    protected static String processPassword(final String queryText) {
        return processQueryText(queryText, PWD_REGEX, PWD_REPLACEMENT_REGEX);
    }

    protected static String processCreds(final String queryText) {
        return processQueryText(queryText, CREDS_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processAccessKey(final String queryText) {
        return processQueryText(queryText, ACCESS_KEY_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processSecretKey(final String queryText) {
        return processQueryText(queryText, SECRET_KEY_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processIamRole(final String queryText) {
        return processQueryText(queryText, IAM_ROLE_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processSymKey(final String queryText) {
        return processQueryText(queryText, SYM_KEY_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processKmsKey(final String queryText) {
        return processQueryText(queryText, KMS_KEY_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processSessionToken(final String queryText) {
        return processQueryText(queryText, SESSION_TOKEN_REGEX, CREDS_REPLACEMENT_REGEX);
    }

    protected static String processLingeringSecrets(final String queryText) {
        return processQueryText(queryText, LINGERING_SECRET_REGEX, LINGERING_SECRET_REPLACEMENT_REGEX);
    }

    protected static String processEncryptKey(final String queryText) {
        return processQueryText(queryText, ENCRYPT_KEY_REGEX, ENCRYPT_KEY_REPLACEMENT_REGEX);
    }

    private static String processQueryText(
            String queryText, Pattern matchPattern, String replacementPattern) {
        if (Objects.isNull(queryText)) {
            return null;
        }
        Matcher matcher = matchPattern.matcher(queryText);
        return matcher.find() ? matcher.replaceAll(replacementPattern) : queryText;
    }
}
