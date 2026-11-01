FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY stream-processor/pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY stream-processor/src ./src
COPY config/rules.yml ./src/main/resources/rules.yml
RUN mvn -q package

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 governance
WORKDIR /app
COPY --from=build /workspace/target/stream-processor-1.0.0.jar /app/app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
