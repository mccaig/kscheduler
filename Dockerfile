# Stage 1 build app
FROM azul/zulu-openjdk-debian:14 as BUILD
COPY *.gradle gradle.* gradlew /src/
COPY gradle /src/gradle
COPY scripts /src/scripts
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon build

# Stage 2 build minimal jre
FROM azul/zulu-openjdk-debian:14 as packager
ENV JAVA_MINIMAL=/opt/jre
RUN apt-get -qq update && \
    apt-get install binutils -y && \
    jlink \
    --verbose \
    --add-modules java.base,jdk.unsupported,java.xml,java.desktop,java.management,java.naming \
    --compress 2 \
    --strip-debug \
    --no-header-files \
    --no-man-pages \
    --output "$JAVA_MINIMAL"

# Stage 3 assemble final image with custom jre and application
FROM ubuntu:focal
ENV JAVA_MINIMAL=/opt/jre
RUN mkdir /opt/app
RUN mkdir /opt/config
COPY --from=packager "$JAVA_MINIMAL" "$JAVA_MINIMAL"
ENV PATH="$PATH:$JAVA_MINIMAL/bin"
COPY --from=BUILD /src/scripts /opt/scripts
COPY --from=BUILD /src/build/libs/kscheduler-1.0.0-SNAPSHOT-all.jar /opt/app/kscheduler.jar
ENTRYPOINT ["opt/scripts/startup.sh"]
CMD ["java", "-jar", "/opt/app/kscheduler.jar"]
