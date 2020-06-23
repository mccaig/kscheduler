FROM adoptopenjdk:11-jre-hotspot as BUILD

COPY *.gradle gradle.* gradlew /src/
COPY gradle /src/gradle
WORKDIR /src
RUN ./gradlew --version

COPY . .
RUN ./gradlew --no-daemon build

# Stage 2, distribution container
FROM adoptopenjdk:11-jre-hotspot
COPY --from=BUILD /src/build/libs/app-all.jar /bin/run.jar
RUN mkdir /opt/app
COPY --from=BUILD /src/build/libs/kscheduler-1.0.0-SNAPSHOT.jar /opt/app/kscheduler.jar
CMD ["java", "-jar", "/opt/app/kscheduler.jar"]