<a target="_blank" href="https://cryostat.io">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./docs/images/cryostat_logo_hori_rgb_reverse.svg">
    <img src="./docs/images/cryostat_logo_hori_rgb_default.svg">
  </picture>
</a>

[![CI build and push](https://github.com/cryostatio/cryostat3/actions/workflows/push-ci.yaml/badge.svg "Continuous Integration")](https://github.com/cryostatio/cryostat3/actions/workflows/push-ci.yaml)
[![Quay Repository](https://img.shields.io/badge/Container_Image-cryostat/cryostat-teal.svg "Quay Repository")](https://quay.io/repository/cryostat/cryostat)
[![Google Group : Cryostat Development](https://img.shields.io/badge/Google_Group-Cryostat_Development-blue.svg "Mailing List")](https://groups.google.com/g/cryostat-development)

A container-native JVM application which acts as a bridge to other containerized JVMs and exposes a secure API for producing, analyzing, and retrieving JDK Flight Recorder data from your cloud workloads.

## SEE ALSO

* [cryostat.io](https://cryostat.io) : upstream documentation website with user
  guides, tutorials, blog posts, and other user-facing content. Start here if
  what you've read so far sounds interesting and you want to know more as a
  **user**, rather than as a _developer_. Here you will find instructions on
  how to install Cryostat using the
  [Cryostat Operator](https://github.com/cryostatio/cryostat-operator), how to
  configure your applications to enable connectivity, and how to use the
  Cryostat application.

* [cryostat-core](https://github.com/cryostatio/cryostat-core) : the core library
  providing a convenience wrapper and headless stubs for use of JFR using
  JDK Mission Control internals.

* [cryostat-operator](https://github.com/cryostatio/cryostat-operator) : an Operator
  for deploying Cryostat in your OpenShift or Kubernetes cluster.

* [cryostat-helm](https://github.com/cryostatio/cryostat-helm): a Helm Chart for
  deploying Cryostat in your OpenShift or Kubernetes cluster

* [cryostat-web](https://github.com/cryostatio/cryostat-web) : the React frontend
  included as a submodule in Cryostat and built into Cryostat's OCI images.

* [JDK Mission Control](https://github.com/openjdk/jmc) : the desktop application
  complement to JFR. Some parts of JMC are borrowed and re-used to form the basis of
  Cryostat. JMC is still a recommended tool for more full-featured analysis of JFR
  files beyond what Cryostat currently implements.

* [binjr](https://github.com/binjr/binjr) : a timeseries visualization desktop
  application which supports reading JFR files

This repository contains the source code for Cryostat versions 3.0 and later. Cryostat (née "container-jfr") versions prior to 3.0
are located at [cryostatio/cryostat-legacy](https://github.com/cryostatio/cryostat-legacy). Container images from both are published
to the same [`quay.io`](https://quay.io/repository/cryostat/cryostat) repository.

## CONTRIBUTING

We welcome and appreciate any contributions from our community. Please visit our guide on how you can take part in improving Cryostat.

[See contribution guide →](./CONTRIBUTING.md)

## REQUIREMENTS

Build requirements:
- git
- JDK 17+
- Maven v3+
- [Quarkus CLI](https://quarkus.io/guides/cli-tooling) v3.4.1+ (Recommended)
- [Podman](https://podman.io/docs/installation) 4.7+

Smoketest run requirements:
- [`yq`](https://github.com/mikefarah/yq) v4.35.1+
- [`kompose`](https://kompose.io/installation/) v1.31.2+
- [docker-compose](https://docs.docker.com/compose/install/) v1.29.2
- [podman-docker](https://packages.fedoraproject.org/pkgs/podman/podman-docker/) (Optional)

General run requirements:
- an OpenShift or Kubernetes cluster
  - recommended installation methods: [`cryostat-operator`](https://github.com/cryostatio/cryostat-operator) or [`cryostat-helm`](https://github.com/cryostatio/cryostat-helm)
- OR any other container platform which can host OCI containers

## BUILD

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

### Setup Dependencies

For ease and convenience, it is suggested to use `podman` with the following configurations:

```bash
$ systemctl --user enable --now podman.socket
```

`$HOME/.bashrc` (or equivalent shell configuration)
```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
```

`$HOME/.testcontainers.properties`
```properties
ryuk.container.privileged=true
docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy
testcontainers.reuse.enable=false
```

Initialize submodules before building:

```bash
$ git submodule init && git submodule update
$ cd src/main/webui
$ yarn install && yarn yarn:frzinstall
$ cd -
```

### Build the application container image

The application image can be created using:

```bash
# With Maven
$ ./mvnw package
# Or with Quarkus CLI
$ quarkus build
```

### Build and run the application in dev mode

You can run your application in dev mode that enables live coding using:

```bash
# With Maven
$ ./mvnw compile quarkus:dev
# Or with Quarkus CLI
$ quarkus dev
```

This will run Cryostat as a local JVM process hooked up to its frontend, and required companion services in containers. Any changes made to the backend or frontend sources, `application.properties`, `pom.xml`, etc. will trigger automatic rebuilds and live-coding.

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8181/q/dev/.

## RUN

### Local Smoketesting

Development on this project is primarily done using `podman`, though things should generally work when using `docker` as well.
Ensure you have performed the `podman` setup above first, then build the container image and run smoketests.
This will spin up the cryostat container and its required services.

```bash
# build Cryostat container, clean up any dangling container images/layers
$ ./mvnw package ; podman image prune -f
# alternatively, use Quarkus CLI instead of the Maven wrapper
$ quarkus build ; podman image prune -f
# generate a certificate and key for the server to enable HTTPS connections
$ sh compose/auth_certs/generate.sh
# check the available smoketest options
$ bash smoketest.bash -h
# run a smoketest scenario
$ bash smoketest.bash -O # without the -O flag, the smoketest will pull the latest development image version, rather than the one you just built
```

To make containers' names DNS-resolvable from the host machine, do:
```bash
$ git clone https://github.com/figiel/hosts libuserhosts
$ cd libuserhosts
$ make PREFIX=$HOME/bin all install
$ echo 'export LD_PRELOAD=$HOME/bin/lib/libuserhosts.so' >> ~/.bashrc
$ export LD_PRELOAD=$HOME/bin/lib/libuserhosts.so
```
(this will require a C compiler toolchain present on your development machine)

You can verify that this setup works by running `smoketest.bash`, and then in another terminal:
```bash
$ LD_PRELOAD=$HOME/bin/libuserhosts.so ping auth
$ LD_PRELOAD=$HOME/bin/libuserhosts.so curl http://auth:8080
$ LD_PRELOAD=$HOME/bin/libuserhosts.so firefox http://auth:8080
```
