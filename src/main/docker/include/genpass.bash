#!/usr/bin/env bash

genpass() {
    < /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32
}
