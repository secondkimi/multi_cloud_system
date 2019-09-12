#!/bin/bash
gradle clean
gradle build
gradle shadowJar
gradle clientJar
gradle chaosJar
cp serverConfig.txt build/libs/