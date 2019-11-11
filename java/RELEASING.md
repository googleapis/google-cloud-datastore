# Releasing to Maven Central

## Set up Sonatype Account

* Sign up for a Sonatype JIRA account [here](https://issues.sonatype.org)
* Click *Sign Up* in the login box, follow instructions

## Get access to repository

* Go to [community support](https://issues.sonatype.org/browse/OSSRH)
* Ask for publish rights by creating an issue similar to [this one](https://issues.sonatype.org/browse/OSSRH-52782)
  * You must be logged in to create a new issue
  * Use the *Create* button at the top tab

## Set up PGP keys

* Install GNU Privacy Guard (GPG)
  * GPG is installed by default on Ubuntu systems
  * For other systems, see [GnuPG download page](https://www.gnupg.org/download/)

* Generate the key ```gpg --gen-key```
  * Keep the defaults, but specify a passphrase
  * The passphrase can be random; you just need to remember it long enough to finish the next step
  * One way to make a random passphrase: ```base64 /dev/urandom | head -c20; echo;```

* Find the ID of your public key ```gpg --list-secret-keys```
  * Look for the line with format ```sec   2048R/ABCDEFGH 2015-11-17```
  * The ```ABCDEFGH``` is the ID for your public key

* Upload your public key to a public server: ```gpg --send-keys --keyserver hkp://pgp.mit.edu ABCDEFGH```

## Create a Maven settings file

* Create a file at ```$HOME/.m2/settings.xml``` with your passphrase and your sonatype username and password
```
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <profiles>
    <profile>
      <id>ossrh</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <properties>
        <gpg.executable>gpg</gpg.executable>
        <gpg.passphrase>[the password for your gpg key]</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
  <servers>
    <server>
      <id>ossrh</id>
      <username>[your sonatype account name]</username>
      <password>[your sonatype account password]</password>
    </server>
    <server>
      <id>sonatype-nexus-snapshots</id>
      <username>[your sonatype account name]</username>
      <password>[your sonatype account password]</password>
    </server>
    <server>
      <id>sonatype-nexus-staging</id>
      <username>[your sonatype account name]</username>
      <password>[your sonatype account password]</password>
    </server>
  </servers>
</settings>
```

## Build and push artifacts

### Stage the artifacts to Maven Central

From the `java` directory, run:

```bash
$ mvn clean deploy
```

### Close the staging repository

1. Log into the Sonatype dashboard at https://oss.sonatype.org
2. Find and select the staged repository that was just created. It should be
   marked as "Open"
3. Click the "Close" button
4. Wait until the repository is in the "Closed" state
5. Select the repository again and click "Release"

It may take some time for the artifacts to be published and be discoverable.
