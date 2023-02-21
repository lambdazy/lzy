#!/bin/bash -ex

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <release-branch>"
  exit 1
fi

BRANCH=$1

echo "$BRANCH" | grep -E "releases/R-[0-9]+\.[0-9]+" -

if [[ $? -ne 0 ]]; then
  echo "Branch format doesn't fit correct release branch pattern - releases/R-[0-9]+\.[0-9]+"
  exit 1
fi

function project_version() {
    mvn help:evaluate -Dexpression=project.version -q -DforceStdout
}

git fetch origin "$BRANCH:$BRANCH"
git checkout "$BRANCH"

cd parent
# x.y.z+1
mvn build-helper:parse-version versions:set \
  -DnewVersion="\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.\${parsedVersion.nextIncrementalVersion}"
NEW_VERSION=$(project_version)
mvn versions:set -DnewVersion="$NEW_VERSION" -f ..
mvn versions:set -DnewVersion="$NEW_VERSION" -f ../util
mvn versions:set -DnewVersion="$NEW_VERSION" -f ../coverage
mvn versions:set -DnewVersion="$NEW_VERSION" -f ../lzy
echo "$NEW_VERSION" > ../pylzy/lzy/version/version

git add -u ..
git commit -m "set version $NEW_VERSION"
git tag "R-$NEW_VERSION"
git push origin "$BRANCH"
git push origin "R-$NEW_VERSION"

echo "release-branch=$BRANCH" >> "$GITHUB_OUTPUT"
echo "release-version=$NEW_VERSION" >> "$GITHUB_OUTPUT"