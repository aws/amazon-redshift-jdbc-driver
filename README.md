## Redshift JDBC Driver

Redshift JDBC Driver requires JDK8 to compile the code. At runtime it needs
JRE8. This driver has many Redshift specific features such as,

* IAM authentication
* IDP authentication
* Redshift specific datatypes support
* External schema support as part of getTables() and getColumns() JDBC API

This driver supports JDBC 4.2 specification.

## Build Driver
On Unix system run:
```
build.sh
```
It builds **redshift-jdbc42-{version}.jar** and **redshift-jdbc42-{version}.zip** files under **target** directory. 
The jar file is the Redshift JDBC driver.  
The zip file contains the driver jar file and all required dependencies files to use AWS SDK for the IDP/IAM features.

## Report Bugs

See [CONTRIBUTING](CONTRIBUTING.md#Reporting-Bugs/Feature-Requests) for more information.

## Contributing Code Development

See [CONTRIBUTING](CONTRIBUTING.md#Contributing-via-Pull-Requests) for more information.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

