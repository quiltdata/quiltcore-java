# quiltcore-java

Java implementation of the Quilt Packaging API

## Testing

```sh
./gradlew check || open lib/build/reports/tests/test/index.html
```

## Publishing

This project is published to [Maven Central](https://central.sonatype.com/artifact/com.quiltdata/quiltcore/versions) using `SonatypeHost.CENTRAL_PORTAL` from the [vanniktech.github.io/gradle-maven-publish-plugin](https://vanniktech.github.io/gradle-maven-publish-plugin/central/#configuring-what-to-publish).

### Setup Publishing

For the publishing to work, you need to:

1. Get username/password credentials (not a SSO login) to the new Central Portal (not the old Nexus one)
2. Use that login to generate an [API Token](https://central.sonatype.com/account) username and password
3. Store them in your `~/.gradle/gradle.properties` or `./gradle.properties` file:

```properties
mavenCentralUsername=api_token_username
mavenCentralPassword=api_token_password_without_wrapper

signing.keyId=12345678
signing.password=some_password
signing.secretKeyRingFile=/Users/yourusername/.gnupg/secring.gpg
```

Note this requires you to export to the `secring.gpg` file from your GPG keychain. You can do this with the following command:

```sh
gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg
```

### Publish Component

To publish a new version, run:

```sh
./gradlew publish
```

This will build the project, run the tests, and publish the new version to Maven Central.

Next, you need to go to the [Sonatype Central Portal](https://central.sonatype.com/) and log in with your username/password. You will see the new version in under Publishing Settings -> [Deployments](https://central.sonatype.com/publishing/deployments).

Once it has been "Validated", you need to click "Publish" to make the new version available to the public.
It should show up in the [Maven Central Repository](https://search.maven.org/artifact/com.quiltdata/quiltcore) in five minutes or so.
