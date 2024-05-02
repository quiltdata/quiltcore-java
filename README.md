# quiltcore-java

Java implementation of the Quilt Packaging API

## Testing

```sh
./gradlew check || open lib/build/reports/tests/test/index.html
```

## Publishing

This project is published to Maven Central using the Portal Publisher API. For the publishing to work the credentials for Sonatype OSS as well as for the GPG key that is used for signing need to provided.
You need to:

1. Get username/password credentials (not a SSO login) to the new Central Portal (not the old Nexus one)
2. Store them in your `~/.gradle/gradle.properties` file:

```properties
mavenCentralUsername=username
mavenCentralPassword=the_password

signing.keyId=12345678
signing.password=some_password
signing.secretKeyRingFile=/Users/yourusername/.gnupg/secring.gpg
```

Note this requires you to export to the `secring.gpg` file from your GPG keychain. You can do this with the following command:

```sh
gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg
```
