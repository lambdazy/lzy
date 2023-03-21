#!/bin/bash -e

function project_revision() {
    mvn help:evaluate -Dexpression=revision -q -DforceStdout
}

cd parent
CURRENT_REVISION=$(project_revision | awk -F'-' '{print $1}')
CUR_MAJOR=$(echo $CURRENT_REVISION | awk -F'.' '{print $1}')
CUR_MINOR=$(echo $CURRENT_REVISION | awk -F'.' '{print $2}')
NEW_MINOR=$((CUR_MINOR + 1))
NEW_MAJOR=$((CUR_MAJOR + 1))

echo "CURRENT VERSION IS $CURRENT_VERSION"
# x.y+1-SNAPSHOT
rgx="(<revision>)([0-9]*\.[0-9]*)(<\/revision>)"
sed -E "1,/${rgx}/s%${rgx}%\1$CURRENT_VERSION\4%" test-input.txt


NEXT_SNAPSHOT_VERSION=$(project_version)
NEXT_PYTHON_VERSION=$(project_version | awk -F'-' '{print $1}')
echo "$NEXT_PYTHON_VERSION" > ../pylzy/lzy/version/version

git add -u ..
git commit -m "set version $NEXT_SNAPSHOT_VERSION"

git checkout "releases/R-$CURRENT_VERSION"

# x.y.0
mvn build-helper:parse-version versions:set \
  -DnewVersion="\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.0" \
  -DgenerateBackupPoms=false -DprocessAllModules

RELEASE_VERSION=$(project_version)
echo "$RELEASE_VERSION" > ../pylzy/lzy/version/version
git add -u ..
git commit -m "set version $RELEASE_VERSION"
git tag "R-$RELEASE_VERSION"
git push origin "releases/R-$CURRENT_VERSION" #branch
git push origin "R-$RELEASE_VERSION" #tag
git push origin master

echo "release-branch=releases/R-$CURRENT_VERSION" >> "$GITHUB_OUTPUT"
echo "release-version=$RELEASE_VERSION" >> "$GITHUB_OUTPUT"
echo "release-tag=R-$RELEASE_VERSION" >> "$GITHUB_OUTPUT"
