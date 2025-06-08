# Stage 1: Build
FROM maven:3.8-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Assuming this module's pom.xml can resolve its parent during the build.
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
ARG SERVICE_PORT=8082
ENV SERVICE_PORT=${SERVICE_PORT}
COPY --from=builder /app/target/*.jar app.jar
EXPOSE ${SERVICE_PORT}
ENTRYPOINT ["java", "-jar", "app.jar"]
