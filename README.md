# quiltcore-java

Java implementation of the Quilt Packaging API

## Testing

```sh
./gradlew check || open lib/build/reports/tests/test/index.html
```

## Publishing

This project is published to Maven Central using the Portal Publisher API. You need to:

1. Get username/password credentials (not a SSO login) to the new Central Portal (not the old Nexus one)
2. Store them in your `~/.gradle/gradle.properties` file like this:

```sh
sonatypeUsername=yourusername
sonatypePassword=yourpassword
```
