#!/usr/bin/sh

set -xe

openssl req -new -newkey rsa:4096 -x509 -sha256 -days 365 -nodes -out certificate.pem -keyout private.key
