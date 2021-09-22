# DS1 Project

Team members:

- Nicola Arpino
- Andrea Stedile

## Build and run

This project requires JDK 11, which
is [certified for Akka](https://developer.lightbend.com/docs/introduction/getting-help/java-versions.html), so make sure
to install it beforehand.

Build and run with Gradle exclusively:

```shell
./gradlew run
```

Otherwise, build a JAR and run it:

```shell
./gradlew uberjar
java -jar build/libs/ds1-1.0-SNAPSHOT-uber.jar 
```

Otherwise, use Docker:

```shell
docker build -t dsproject1 .
docker run dsproject1  
```
