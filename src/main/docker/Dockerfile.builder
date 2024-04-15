FROM registry.access.redhat.com/ubi8/openjdk-17:1.19-1 AS builder
ARG TARGETARCH
USER root
WORKDIR /tmp/build
COPY .mvn/ .mvn
COPY pom.xml mvnw .
COPY src/main/java src/main/java
COPY src/main/resources src/main/resources
COPY src/main/webui src/main/webui
COPY src/main/docker/include src/main/docker/include
RUN ./mvnw -Dmaven.repo.local=/tmp/build/m2/repository -B -U -Dmaven.test.skip=true -Dlicense.skip=true -Dspotless.check.skip=true -Dquarkus.container-image.build=false -Dbuild.arch=$TARGETARCH package

FROM registry.access.redhat.com/ubi8/openjdk-17-runtime:1.19-1
ENV LANGUAGE='en_US:en'
EXPOSE 8181
USER 185
LABEL io.cryostat.component=cryostat3
ENV JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"
ENTRYPOINT [ "/opt/jboss/container/java/run/run-java.sh" ]
COPY --from=builder --chown=185 /tmp/build/src/main/docker/include/cryostat.jfc /usr/lib/jvm/jre/lib/jfr/
COPY --from=builder --chown=185 /tmp/build/target/quarkus-app/lib/ /deployments/lib/
COPY --from=builder --chown=185 /tmp/build/target/quarkus-app/*.jar /deployments/
COPY --from=builder --chown=185 /tmp/build/target/quarkus-app/app/ /deployments/app/
COPY --from=builder --chown=185 /tmp/build/target/quarkus-app/quarkus/ /deployments/quarkus/
