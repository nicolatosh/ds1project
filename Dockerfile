FROM gradle:7.2.0-jdk11 AS build
WORKDIR /src
COPY . ./
RUN gradle uberjar --no-daemon

FROM gradle:7.2.0-jre11
WORKDIR /app
COPY --from=build /src/build/libs/*.jar ./ds1project.jar
CMD ["java", "-jar", "ds1project.jar"]