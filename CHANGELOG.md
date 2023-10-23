Changelog
=========

v2.1.0.21 (2023-10-23)
----------------------
- Fixed a bug where the check for token/credentials expiration time was incorrect [Bhavik Shah]
  
v2.1.0.20 (2023-10-16)
----------------------
- Added support for lz4 compression over wire protocol communication between Redshift server and the client/driver. Compression is turned on by default and can be set using new connection parameter compression=off or compression=lz4 [Bhavik Shah]
- Fixed a bug where closing a statement with partially read results could lead to errors in subsequent statements on the same connection [Bhavik Shah]
- Improved driver performance when closing statements with partially read results [Bhavik Shah]
- Fixed a bug where the cancellation of a query could lead to an unexpected connection close by the server [Bhavik Shah]

v2.1.0.19 (2023-09-11)
----------------------
- Added Identity Center authentication support with new plugins [Bhavik Shah]
- Increased HTTP server backlog size to 2 to account for Google Chrome's preloading causing HTTP requests to fail occasionally when logging in using BrowserSamlCredentialsProvider [PR#95](https://github.com/aws/amazon-redshift-jdbc-driver/pull/95) [
Jimmy Do]
- Fix error message shown when value provided for maxResultBuffer property cannot be parsed [Bhavik Shah]
- Improvements for logging [Bhavik Shah]
- Improvements for XML parsing [Bhavik Shah]
  
v2.1.0.18 (2023-08-01)
----------------------
- Added feature to detect region automatically for IAM connections if not provided by user [Bhavik Shah]
- Fixed a bug where ring buffer was growing beyond expected limits [issue#88](https://github.com/aws/amazon-redshift-jdbc-driver/issues/88) [Bhavik Shah]
- Fixed NULLABLE and IS_NULLABLE definition in metadata for external datasharing objects [Bhavik Shah]
- Removed a restriction that was preventing calls to the getClusterCredentialsWithIAM API in the AWS SDK. This allows the user to authenticate with IAM group federation on clusters [Bhavik Shah]

v2.1.0.17 (2023-07-05)
----------------------
- Support for Custom Cluster Names [Bhavik Shah]
- Fix to close suspended portal when ringbuffer queue is closed [Bhavik Shah]
- Bump Jackson dependencies from 2.14.2 to 2.15.0 [issue#91](https://github.com/aws/amazon-redshift-jdbc-driver/issues/91) [Bhavik Shah]
- Bump AWS Java SDK dependencies from 1.12.408 to 1.12.493 [Bhavik Shah]
  
v2.1.0.16 (2023-06-09)
----------------------
- Improve connection validation logic [Bhavik Shah]
- Improvements to logging [Bhavik Shah]
- Fixed a bug where portal was being closed incorrectly [Bhavik Shah]

v2.1.0.14 (2023-04-13)
----------------------
- Fix null pointer exception when role based profile has no source_profile defined [issue#83](https://github.com/aws/amazon-redshift-jdbc-driver/issues/83) [PR#84](https://github.com/aws/amazon-redshift-jdbc-driver/pull/84) [Jérôme Mainaud]
- Improvements to logging [Bhavik Shah]
- Fix for ringbuffer to respect statement.setMaxRows() [Bhavik Shah]

v2.1.0.13 (2023-03-31)
----------------------
- Remove setting of session-level timezone to JVM timezone at start of session. This change defaults session-level timezone to server side timezone, typically UTC unless set to otherwise [Bhavik Shah]
- Upgrade commons-codec from 1.11 to 1.15 [Bhavik Shah]
- Improve XML parsing [Bhavik Shah]

v2.1.0.12 (2023-02-24)
----------------------
- Allow connection parameters to be case-insensitive [Bhavik Shah]
- Upgrade AWS Java SDK from 1.12.251 to 1.12.408 [Bhavik Shah]
- Upgrade Jackson version from 2.12.6.1 to 2.14.2 [Bhavik Shah]
- Bump httpclient from 4.5.13 to 4.5.14 [Bhavik Shah]
- Update pom.xml [Bhavik Shah]

v2.1.0.11 (2023-02-03)
----------------------
- Fix views when CAST NULL [PR#75](https://github.com/aws/amazon-redshift-jdbc-driver/pull/75) [Bhavik Shah]
- Fix to allow sslMode to be set as DISABLE when ssl is explicitly set to false [issue#76](https://github.com/aws/amazon-redshift-jdbc-driver/issues/76) [Bhavik Shah]
- Fix to allow ring buffer threading to finish before statement close [issue#63](https://github.com/aws/amazon-redshift-jdbc-driver/issues/63) [Bhavik Shah]
- Allow overriding schema pattern type using property 'OverrideSchemaPatternType' [issue#64](https://github.com/aws/amazon-redshift-jdbc-driver/issues/64) [PR#65](https://github.com/aws/amazon-redshift-jdbc-driver/pull/65) [Bhavik Shah]
- Fix to properly cancel socket timeout [issue#77](https://github.com/aws/amazon-redshift-jdbc-driver/issues/77) [Bhavik Shah]
- Fix getColumns for Late binding views [PR#74](https://github.com/aws/amazon-redshift-jdbc-driver/pull/74) [Bhavik Shah]
- Addressed the issue for caching of PG queries [Bhavik Shah]

v2.1.0.10 (2022-11-30)
----------------------
- Removed loggerLevel and loggerFile properties. [Brooke White]
- Fix for extended properties not working with iam endpoints. [Brooke
  White]
- Chore: align aws-sdk versions. [Brooke White]
- Fix github issue-53. [Brooke White]
- Update pom.xml. [Brooke White]
- Update pom.xml. [ilesh garish]

v2.1.0.9 (2022-07-01)
---------------------
- Support serverless using public Java SDK. [ilesh Garish]
- Upgrade jackson version from 2.12.3 to 2.12.6.
- Fix [issue#58](https://github.com/aws/amazon-redshift-jdbc-driver/issues/58) [ilesh Garish]
- Fix [issue#59](https://github.com/aws/amazon-redshift-jdbc-driver/issues/59) [ilesh Garish]

v2.1.0.8 (2022-06-08)
------------
- Fix [issue#54](https://github.com/aws/amazon-redshift-jdbc-driver/issues/54) [ilesh Garish]
- Fix [issue#53](https://github.com/aws/amazon-redshift-jdbc-driver/issues/53) [ilesh Garish]
- Fix Object Factory to check class type when instantiating an object
  from class name. [ilesh Garish]
- Set default 15 min timeout to protect pops-up for OAuth2 Browser Plugin [ilesh Garish]

v2.1.0.7 (2022-05-04)
---------------------
- Updated SAML Plugin browser launch process. [ilesh Garish]
- Fix race condition for Browser Plugin. [ilesh Garish]

v2.1.0.6 (2022-04-14)
---------------------
- Preserve server error in case of SSL Request's response
- Remove IAM Role check for V2 API as CDO team requested
- ApplicationName connection parameter is auto-populated with the caller application class name, if it is not set explicitly by the caller in the connection string.

v2.1.0.5 (2022-03-11)
---------------------
- Fix INTERVAL type value issue in BINARY from date calculation. [ilesh
  Garish]
- Fix [issue#45](https://github.com/aws/amazon-redshift-jdbc-driver/issues/45) [ilesh Garish]
- Fix external table's columns query. [ilesh Garish]

v2.1.0.4 (2022-01-30)
---------------------
- Support GEOGRAPHY data type. [ilesh Garish]
- Fix [issue#39](https://github.com/aws/amazon-redshift-jdbc-driver/issues/39) [ilesh Garish]
- Support Native Auth with Browser Azure IDP

v2.1.0.3 (2021-12-03)
---------------------
- Support ABSTIME datatype. [ilesh Garish]
- Support Serverless. [ilesh Garish]
- Support Redshift Native Auth Federation. [ilesh garish]

v2.1.0.2 (2021-11-10)
----------------------
- Fix getProcedures procedure_type having  OUT param. [ilesh Garish]
- Fix column type for IN type stored procedure. [ilesh Garish]
- Fix [issue#37](https://github.com/aws/amazon-redshift-jdbc-driver/issues/37). [ilesh Garish]
- Fix TIMETZ OUT param type issue. [ilesh Garish]
- Fix ADFS form parsing issue. [ilesh Garish]
- Support SHA256 password digest. [ilesh Garish]

v2.1.0.1 (2021-09-04)
---------------------
- Fix stack overflow for "unknown" type. [ilesh Garish]
- Fix SQLWorkbench issue for underscore in database name. [ilesh Garish]
- Support VARBYTE data type. [ilesh Garish]
- Use derived user from JWT as role session name for JWT SSO. [ilesh Garish]
- Fix Varying length precision and scale for numeric types. [ilesh
  Garish]
- Fix getColumns exception in federated query due to missing varchar
  length. [ilesh Garish]
- Fix for AuthProfile with IDP parameters. [ilesh Garish]


v2.0.0.7 (2021-07-23)
---------------------
- Support nonProxyHosts system property for STS and Redshift coral service. [ilesh Garish]
- Support of reading connection properties using an authentication profile.  [ilesh Garish]
- Fix nano second issue with Timestamp to string conversion. [ilesh Garish]
- Fix [issue#26](https://github.com/aws/amazon-redshift-jdbc-driver/issues/26). [ilesh Garish]
- Return current database instead of NULL for getSchemas() API. [ilesh Garish]

v2.0.0.6 (2021-06-29)
---------------------
- Fix [issue#27](https://github.com/aws/amazon-redshift-jdbc-driver/issues/27). [ilesh Garish]
- Add support for Profile process credentials. [Laurent Goujon]
- Bump httpclient from 4.5.2 to 4.5.13. [dependabot[bot]]
- Upgrade AWS Java SDK from 1.1.118 to 1.12.2. [ilesh Garish]

v2.0.0.5 (2021-06-08)
---------------------
- Fix security vulnerability. [ilesh Garish]
- Support of JDBC connection config using INI file. [ilesh Garish]
- Fix [issue#19](https://github.com/aws/amazon-redshift-jdbc-driver/issues/19). [ilesh Garish]
- Support column case sensitivity based on collate. [ilesh Garish]
- Binary protocol support. [ilesh Garish]
- Fix [issue#16](https://github.com/aws/amazon-redshift-jdbc-driver/issues/16). [ilesh Garish]
- Error while trying to retrieve stored procedures in DBeaver

v2.0.0.4 (2021-03-28)
---------------------
- Added more logging for Browser plugins. [ilesh Garish]
- Map XID to Integer and TID to VARCHAR. [ilesh Garish]
- Fix [issue#14](https://github.com/aws/amazon-redshift-jdbc-driver/issues/14). [ilesh Garish]
- SSL should not be disable in IAM authentication. [ilesh Garish]
- Changes for OUT and INOUT proc params to get size of the param in
  getProcedureColumns. [ilesh Garish]
- Return metadata privileges for views and foreign tables. [Jeremy
  Mailen]
- Change getProcedureColumns() to get param length. [ilesh Garish]
- Read database user from JWT. [ilesh Garish]
- Fix [issue#12](https://github.com/aws/amazon-redshift-jdbc-driver/issues/12). [ilesh Garish]
- Log error response of SAML request. [ilesh Garish]


v2.0.0.3 (2021-02-25)
---------------------
 - Fix [issue#9](https://github.com/aws/amazon-redshift-jdbc-driver/issues/9). [ilesh Garish]
 - Support for proxy connection to STS. [ilesh Garish]
 - Server parameter marker changes, stringtype connetcion parameter default value change to unspecified. [ilesh Garish]
 - Added region as part of endpoint config for vpc endpoint. [ilesh Garish]
 - EndpointURL couldn't set as a region. [ilesh Garish]
 - Support GeneratedKeys from RETURNING clause. [ilesh Garish]
 - Added custom sts endpoint support in all plugins. [ilesh Garish]
 - Fix for IDP HTTPS Proxy properties support. [ilesh Garish]
 - Fix [issue#7](https://github.com/aws/amazon-redshift-jdbc-driver/issues/7). [Steven Nguyen]


v2.0.0.2 (2021-01-19)
---------------------
- Fix GitHub [Issue#8](https://github.com/aws/amazon-redshift-jdbc-driver/issues/8). [ilesh Garish]
- Fix GitHub [Issue#6](https://github.com/aws/amazon-redshift-jdbc-driver/issues/6). [ilesh Garish]
- Support caching of credentials to protect against AWS API limit issue.
  [ilesh Garish]
- Support JWT provider plugin. [ilesh Garish]
- Update README.md. [iggarish]
- Enable client_protocol_version startup parameter. [ilesh Garish]
- Added .gitignore file. [ilesh Garish]
- Support Redshift parameter marker in a query. [ilesh Garish]
- SQLProcedureColumns not returning all SP parameters due to inproper
  handling of multiple out parameters. [ilesh Garish]


v2.0.0.1 (2020-12-02)
---------------------
- Generate CHANGELOG.md file. [ilesh Garish]
- Fix synchronization issue. [ilesh Garish]


v2.0.0.0 (2020-11-03)
---------------------
- Fixes from security review. [ilesh Garish]
- Update README.md. [iggarish]
- Browser plugin fix, multi databases support. [ilesh Garish]
- Update PULL_REQUEST_TEMPLATE.md. [iggarish]
- Update PULL_REQUEST_TEMPLATE.md. [iggarish]
- Merge branch 'master' of github.com:aws/amazon-redshift-jdbc-driver.
  [ilesh Garish]
- Create CODEOWNERS. [iggarish]
- Update THIRD_PARTY_LICENSES. [iggarish]
- Sync with latest source code. [ilesh Garish]
- Update CONTRIBUTING.md. [iggarish]
- Create checkstyle.xml. [iggarish]
- Create ISSUE_TEMPLATE.md. [iggarish]
- Create THIRD_PARTY_LICENSES. [iggarish]
- Delete pull_request_template.md. [iggarish]
- Create PULL_REQUEST_TEMPLATE.md. [iggarish]
- Create CHANGELOG.md. [iggarish]
- Create pull_request_template.md. [iggarish]
- Sample changes to test CR. [ilesh Garish]
- Changes for Build requirements. [ilesh Garish]
- Added notes for how to build. [ilesh Garish]
- Added more info in README file. [ilesh Garish]
- Added more info in README file. [ilesh Garish]
- Added initial content in README file. [ilesh Garish]
- Added link of open issues and close issues. [ilesh Garish]
- Initial version. [Ilesh Garish]
- Initial commit. [Amazon GitHub Automation]


