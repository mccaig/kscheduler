FROM azul/zulu-openjdk-alpine:11 as packager

RUN { \
        java --version ; \
        echo "jlink version:" && \
        jlink --version ; \
    }

ENV JAVA_MINIMAL=/opt/jre

# build modules distribution
RUN jlink \
    --verbose \
    --add-modules \
        java.base,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument \
        # java.naming - javax/naming/NamingException
        # java.desktop - java/beans/PropertyEditorSupport
        # java.management - javax/management/MBeanServer
        # java.security.jgss - org/ietf/jgss/GSSException
        # java.instrument - java/lang/instrument/IllegalClassFormatException
    --compress 2 \
    --strip-debug \
    --no-header-files \
    --no-man-pages \
    --output "$JAVA_MINIMAL"

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