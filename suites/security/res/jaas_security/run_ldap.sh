#!/usr/bin/env bash

docker run --name ldap -d -p 389:10389 -p 636:10636 openmicroscopy/apacheds

