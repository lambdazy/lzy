#!/bin/bash -ex

function project_version() {
    mvn help:evaluate -Dexpression=project.version -q -DforceStdout
}

cd parent
# x.y.z+1
mvn build-helper:parse-version versions:set \
  -DnewVersion="\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.\${parsedVersion.nextIncrementalVersion}" \
  -DgenerateBackupPoms=false -DprocessAllModules
NEW_VERSION=$(project_version)
echo "$NEW_VERSION" > ../pylzy/lzy/version/version

echo "release-version=$NEW_VERSION" >> "$GITHUB_OUTPUT"