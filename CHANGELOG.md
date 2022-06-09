Changelog
=========

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


