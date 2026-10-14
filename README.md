## Redshift JDBC Driver

The Amazon JDBC Driver for Redshift is a Type 4 JDBC driver that provides database connectivity through the standard JDBC application program interfaces (APIs) available in the Java Platform, Enterprise Editions. The Driver provides access to Redshift from any Java application, application server, or Java-enabled applet.

The driver has many Redshift specific features such as,

* IAM authentication
* IDP authentication
* Redshift specific datatypes support
* External schema support as part of getTables() and getColumns() JDBC API

The driver supports JDBC 4.2 specification.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.amazon.redshift/redshift-jdbc42/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.amazon.redshift/redshift-jdbc42)
[![javadoc](https://javadoc.io/badge2/com.amazon.redshift/redshift-jdbc42/javadoc.svg)](https://javadoc.io/doc/com.amazon.redshift/redshift-jdbc42)

## Build Driver
### Prerequisites
* JDK8
* Maven 3.x
* Redshift instance connect to.

### Build Artifacts
On Unix system run:
```
build.sh
```
It builds **redshift-jdbc42-{version}.jar** and **redshift-jdbc42-{version}.zip** files under **target** directory. 
The jar file is the Redshift JDBC driver.The zip file contains the driver jar file and all required dependencies files to use AWS SDK for the IDP/IAM features.

### Installation and Configuration of Driver

See [Amazon Redshift JDBC Driver Installation and Configuration Guide](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-install.html) for more information.

Here are download links for the latest release:

* https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.2.9/redshift-jdbc42-2.2.9.jar
* https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.2.9/redshift-jdbc42-2.2.9.zip

It also available on Maven Central, groupId: com.amazon.redshift and artifactId: redshift-jdbc42.

## DataSource serialization change (driver 2.3.0)

Starting with driver version 2.3.0, the driver changed how it serializes and reads
`DataSource` objects. The connection-configuration fields are now written as simple
values (numbers and text) with a small format marker, and on read the driver reads
those values directly without reconstructing any objects from the stream. A stream
that is not in this format is rejected rather than read.

**Most users do not need to do anything.** This change only affects applications that
persist a Redshift `DataSource` as a **Java-serialized object** and read it back. You
are **not** affected if you connect using a JDBC URL / `DriverManager`, configure the
driver through connection properties/setters, or use a JNDI resource **reference**
(for example a Tomcat `<Resource>`), since those paths do not deserialize a Java
object.

### How a serialized DataSource is created (to check if this applies to you)

You have a serialized DataSource if your application built a
`RedshiftSimpleDataSource` (or `RedshiftConnectionPoolDataSource`), set its
properties, and then wrote the **object itself** to a store — most often by binding it
into a JNDI/LDAP directory, or by writing it with `ObjectOutputStream`. For example:

```java
RedshiftSimpleDataSource ds = new RedshiftSimpleDataSource();
ds.setServerName("example.redshift.amazonaws.com");
ds.setDatabaseName("dev");
ds.setUser("awsuser");

// Bound into a directory as a serialized object (stored under javaSerializedData):
Context ctx = new InitialContext();
ctx.rebind("cn=myRedshiftDataSource", ds);
```

Later, another process reads it back and gets a ready-to-use DataSource:

```java
DataSource ds = (DataSource) new InitialContext().lookup("cn=myRedshiftDataSource");
```

It is that stored object — written by a driver older than 2.3.0 — that 2.3.0 and
later can no longer read directly. If your application instead **re-creates** the
DataSource in code each time it starts (building a fresh `new RedshiftSimpleDataSource()` and setting
its properties, as in the first snippet) rather than reading back a stored object, you
have nothing to migrate: a fresh save already uses the current format.

**Breaking change in 2.3.0 (only for serialized DataSources):** the new format is not
backward-readable. A `RedshiftSimpleDataSource` / `RedshiftConnectionPoolDataSource`
that was serialized by a driver earlier than 2.3.0 (for example, stored in a
JNDI/LDAP directory as `javaSerializedData`, or in clustered session/failover state)
is rejected on read with `java.io.InvalidClassException`. Convert the stored bytes
**once** with the bundled helper.

The migration is always three steps: read the old bytes from wherever the DataSource
is stored, convert them, then write them back. The helper is the middle step:

> **Note:** run the one-time conversion of older-format bytes on **Java 9 or later, or
> Java 8u121 or later**. On those runtimes the conversion uses the JDK's built-in
> serialization size limits while reading the old data. On an older JVM that lacks
> that support, `migrateToCurrentFormat` refuses an older-format input with an
> `InvalidClassException` rather than reading it unbounded — so run the conversion on a
> supported JVM (the converted bytes can then be used anywhere). Reads of already
> converted (current-format) DataSources have no such requirement.

```java
import com.amazon.redshift.ds.common.DataSourceMigration;

// 1. Read the bytes your store already holds. For example, from an LDAP/JNDI
//    directory entry that was bound with an old driver (attribute javaSerializedData):
DirContext ctx = new InitialDirContext(env);
Attributes attrs = ctx.getAttributes("cn=myRedshiftDataSource", new String[] { "javaSerializedData" });
byte[] oldBytes = (byte[]) attrs.get("javaSerializedData").get();

// 2. Convert the old-format bytes to the current format.
byte[] newBytes = DataSourceMigration.migrateToCurrentFormat(oldBytes);

// 3. Write the converted bytes back to the same entry.
ModificationItem[] mods = {
    new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
        new BasicAttribute("javaSerializedData", newBytes))
};
ctx.modifyAttributes("cn=myRedshiftDataSource", mods);
```

The same pattern applies to any store: load the bytes, call
`migrateToCurrentFormat`, save the returned bytes back. If instead you can simply
re-create and re-save the DataSource with a current driver (for example, an app that
rebuilds and re-binds its DataSource on startup), you do not need this helper at
all — a fresh save already uses the current format.

`migrateToCurrentFormat` reads the old bytes through the same type validation, so an
input that is not a recognized DataSource is rejected during conversion. The method is
also safe to run over bytes that are **already** in the current format (it returns an
equivalent current-format encoding), so you can apply it across a store without first
sorting which entries are old.

## Report Bugs

See [CONTRIBUTING](CONTRIBUTING.md#Reporting-Bugs/Feature-Requests) for more information.

## Contributing Code Development

See [CONTRIBUTING](CONTRIBUTING.md#Contributing-via-Pull-Requests) for more information.

 ## Changelog Generation
 An entry in the changelog is generated upon release using `gitchangelog <https://github.com/vaab/gitchangelog>`.
 Please use the configuration file, ``.gitchangelog.rc`` when generating the changelog.
	 
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the BSD-2-Clause License.

