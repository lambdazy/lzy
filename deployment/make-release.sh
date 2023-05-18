#!/bin/bash -e

MAJOR=false

for ARG in "$@"; do
  case "$ARG" in
  --major) #increment major version
    MAJOR=true
    ;;
  esac
done

function revision() {
    mvn help:evaluate -Dexpression=revision -q -DforceStdout
}

function changelist() {
    mvn help:evaluate -Dexpression=changelist -q -DforceStdout
}

git checkout master
git pull

cd parent
CURRENT_REVISION=$(revision)
REVISION_PARTS=($(echo $CURRENT_REVISION | awk -F'.' '{print $1, $2}'))
CURRENT_MAJOR=${REVISION_PARTS[0]}
CURRENT_MINOR=${REVISION_PARTS[1]}
NEXT_REVISION=

echo "CURRENT VERSION IS $CURRENT_REVISION"
git branch "releases/R-$CURRENT_REVISION"

if [[ $MAJOR == true ]]; then
  # x+1.0-SNAPSHOT
  NEXT_REVISION="$((CURRENT_MAJOR + 1)).0"
else
  # x.y+1-SNAPSHOT
  NEXT_REVISION="${CURRENT_MAJOR}.$((CURRENT_MINOR + 1))"
fi

mvn versions:set-property -Dproperty=revision \
    -DnewVersion="$NEXT_REVISION" \
    -DgenerateBackupPoms=false -DprocessAllModules

CHANGELIST=$(changelist)
echo "$NEXT_REVISION.0" > ../pylzy/lzy/version/version

git add -u ..
git commit -m "set version ${NEXT_REVISION}${CHANGELIST}"

git checkout "releases/R-$CURRENT_REVISION"

# x.y.0
mvn versions:set-property -Dproperty=changelist \
      -DnewVersion="" \
      -DgenerateBackupPoms=false -DprocessAllModules
mvn versions:set-property -Dproperty=revision \
      -DnewVersion="${CURRENT_REVISION}.0" \
      -DgenerateBackupPoms=false -DprocessAllModules

RELEASE_VERSION=$(revision)
echo "$RELEASE_VERSION" > ../pylzy/lzy/version/version
git add -u ..
git commit -m "set version $RELEASE_VERSION"
git tag "R-$RELEASE_VERSION"
git push origin "releases/R-$CURRENT_REVISION" #branch
git push origin "R-$RELEASE_VERSION" #tag
git push origin master

echo "release-branch=releases/R-$CURRENT_REVISION" >> "$GITHUB_OUTPUT"
echo "release-version=$RELEASE_VERSION" >> "$GITHUB_OUTPUT"
echo "release-tag=R-$RELEASE_VERSION" >> "$GITHUB_OUTPUT"
