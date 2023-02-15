#!/bin/bash -e

MAJOR=false

for ARG in "$@"; do
  case "$ARG" in
  --major) #increment major version
    MAJOR=true
    ;;
  esac
done

function project_version() {
    mvn help:evaluate -Dexpression=project.version -q -DforceStdout
}

git checkout master
git pull

cd parent
CURRENT_VERSION=$(project_version | awk -F'-' '{print $1}')

echo "CURRENT VERSION IS $CURRENT_VERSION"
git branch "releases/R-$CURRENT_VERSION"

if [[ $MAJOR == true ]]; then
  # x+1.0-SNAPSHOT
  mvn build-helper:parse-version versions:set \
    -DnewVersion="\${parsedVersion.nextMajorVersion}.0\${parsedVersion.qualifier?}"
else
  # x.y+1-SNAPSHOT
  mvn build-helper:parse-version versions:set \
    -DnewVersion="\${parsedVersion.majorVersion}.\${parsedVersion.nextMinorVersion}\${parsedVersion.qualifier?}"
fi

NEXT_SNAPSHOT_VERSION=$(project_version)
mvn versions:set -DnewVersion="$NEXT_SNAPSHOT_VERSION" -f ..
mvn versions:set -DnewVersion="$NEXT_SNAPSHOT_VERSION" -f ../util
mvn versions:set -DnewVersion="$NEXT_SNAPSHOT_VERSION" -f ../coverage
mvn versions:set -DnewVersion="$NEXT_SNAPSHOT_VERSION" -f ../lzy
git add -u ..
git commit -m "set version $NEXT_SNAPSHOT_VERSION"

git checkout "releases/R-$CURRENT_VERSION"

# x.y.0
mvn build-helper:parse-version versions:set \
  -DnewVersion="\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.0"

RELEASE_VERSION=$(project_version)
echo "$RELEASE_VERSION" > ../pylzy/lzy/version/version
git add -u ..
git commit -m "set version $RELEASE_VERSION"
git tag "R-$RELEASE_VERSION"
git push origin "releases/R-$CURRENT_VERSION" #branch
git push origin "R-$RELEASE_VERSION" #tag
git push origin master
