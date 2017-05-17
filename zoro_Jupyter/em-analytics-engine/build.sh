#!/bin/bash

export JAVA_HOME=${JAVA_HOME_ORACLEJDK8}
export PATH=${JAVA_HOME}/bin:${PATH}
export http_proxy=http://sjc1intproxy01.crd.ge.com:8080
export https_proxy=$http_proxy

echo ${BUILD_NUMBER}

#maven install
mvn clean install -Dci.build.number=${BUILD_NUMBER} -s mvn_settings.xml -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8

if [ "$?" -ne 0 ]; then
    echo "build failed"
    exit 1 
fi

#maven deploy to artifactory
mvn clean deploy -Dci.build.number=${BUILD_NUMBER} -s mvn_settings.xml -DskipTests -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8