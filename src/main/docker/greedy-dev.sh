#!/usr/bin/env bash
docker run -e "SPRING_PROFILES_ACTIVE=dev" -p 8080:8080 -t kduda/greedy