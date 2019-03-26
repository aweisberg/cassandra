set -xe

VERSION="4.0-SNAPSHOT"
VERSION_SHORT="4.0"
GROUP_DIR=.dist/com/apple/pie/cassandra
cd /code
ant dtest-jar

mkdir -p ${GROUP_DIR}/in-jvm-dtest/${VERSION}/
cp ./dtest-${VERSION_SHORT}.pom ${GROUP_DIR}/in-jvm-dtest/${VERSION}/in-jvm-dtest-${VERSION}.pom
mv ./build/dtest-${VERSION_SHORT}.jar ${GROUP_DIR}/in-jvm-dtest/${VERSION}/in-jvm-dtest-${VERSION}.jar

set +xe
