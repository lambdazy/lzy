#!/bin/bash -ex

function revision() {
    mvn help:evaluate -Dexpression=revision -q -DforceStdout
}

cd parent

CURRENT_REVISION=$(revision)
REVISION_PARTS=($(echo $CURRENT_REVISION | awk -F'.' '{print $1, $2, $3}'))
CURRENT_MAJOR=${REVISION_PARTS[0]}
CURRENT_MINOR=${REVISION_PARTS[1]}
CURRENT_PATCH=${REVISION_PARTS[2]}
NEXT_REVISION="${CURRENT_MAJOR}.${CURRENT_MINOR}.$((CURRENT_PATCH + 1))"

# x.y.z+1
mvn versions:set-property -Dproperty=changelist \
      -DnewVersion="" \
      -DgenerateBackupPoms=false -DprocessAllModules
mvn versions:set-property -Dproperty=revision \
      -DnewVersion="$NEXT_REVISION" \
      -DgenerateBackupPoms=false -DprocessAllModules
echo "$NEXT_REVISION" > ../pylzy/lzy/version/version

echo "release-version=$NEXT_REVISION" >> "$GITHUB_OUTPUT"
