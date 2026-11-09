# Builds one of the services: docker build --build-arg MODULE=api .
# Used by the "app" profile in infra/docker-compose.yml.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
# Dependencies first, so they are cached until a pom.xml changes.
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY api/pom.xml api/
COPY worker/pom.xml worker/
COPY mock-sms/pom.xml mock-sms/
RUN ./mvnw -B -q dependency:go-offline -DexcludeReactor=true
COPY api/src api/src
COPY worker/src worker/src
COPY mock-sms/src mock-sms/src
ARG MODULE
RUN ./mvnw -B -q -pl "${MODULE}" package -DskipTests \
    && cp "${MODULE}/target/${MODULE}-0.0.1-SNAPSHOT.jar" /app.jar

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 app
USER app
COPY --from=build /app.jar /app/app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
