# Stage 1: Build
FROM maven:3.8-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
# Copy parent pom if it's a multi-module project and needed for build context
# For this project structure, the parent pom is one level up.
# To make it available, we might need to adjust how we copy files or build context.
# Assuming this Dockerfile is run with context at `etl-extract-service`
# and `pom.xml` can resolve its parent if the parent is in a known repo or installed locally in builder.
# A safer way for multi-module is to copy the whole project or build JARs outside and copy them in.
# For now, assume this module's pom.xml is self-contained enough for `mvn package` within its own dir,
# or that the Maven build process handles parent POM resolution correctly.
# If it needs the parent POM file: COPY ../pom.xml ./parent-pom.xml (and adjust module pom) - this is complex.
# A common pattern is to have a global Dockerfile at the root or build JARs first.
# Given the current structure, I'll assume module's pom.xml is sufficient for now.
# If `mvn clean package` fails due to parent POM not found, this needs adjustment.
# One way is to copy the entire project structure into /app in builder.
# Let's try copying the entire project to /app/etl-tool in the builder stage
# and then set WORKDIR to the specific module.

# Alternative for multi-module:
# WORKDIR /app/etl-tool
# COPY . . # Copies entire project from parent etl-tool directory
# WORKDIR /app/etl-tool/etl-extract-service
# RUN mvn clean package -DskipTests -pl etl-extract-service -am # Build only this module and its dependencies

# Simpler approach if Docker context is the module directory:
COPY src ./src
# This assumes parent POM is accessible via Maven repositories or was installed by a previous step not shown.
# For local builds, `mvn install` of parent pom first helps.
# Let's assume the `pom.xml` within the module can be built directly if its parent has been published/installed.
# To avoid issues with parent POM, a better approach is to copy the relevant parts.
# Assuming the build context is the root of the 'etl-tool' project:
# WORKDIR /app
# COPY pom.xml . # Copy parent pom
# COPY etl-common/pom.xml ./etl-common/pom.xml
# COPY etl-common/src ./etl-common/src
# COPY etl-extract-service/pom.xml ./etl-extract-service/pom.xml
# COPY etl-extract-service/src ./etl-extract-service/src
# RUN mvn -f etl-extract-service/pom.xml clean package -DskipTests

# Sticking to the provided Dockerfile structure for now, which assumes context is the module itself.
# This implies that `pom.xml` can resolve its parent during the build.
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
ARG SERVICE_PORT=8081
ENV SERVICE_PORT=${SERVICE_PORT}
# The path to the JAR file depends on the artifactId and version in pom.xml
# For example, if artifactId is 'etl-extract-service' and version is '1.0-SNAPSHOT',
# the JAR would be target/etl-extract-service-1.0-SNAPSHOT.jar
# Using *.jar is a common workaround if the name is consistent or only one JAR is there.
COPY --from=builder /app/target/*.jar app.jar
EXPOSE ${SERVICE_PORT}
ENTRYPOINT ["java", "-jar", "app.jar"]
