#!/usr/bin/env bash

# This script is meant to setup the heed3 crate.
#

if [[ -n $(git status -s) ]]; then
    echo "Error: Repository is git dirty, please commit or stash changes before running this script."
    exit 1
fi

set -e

# It basically copy the heed3/Cargo.toml file into
# the heed folder...
if [[ "$OSTYPE" == "cygwin" || "$OSTYPE" == "msys" ]]; then
    cp heed3\\Cargo.toml heed\\Cargo.toml
else
    cp heed3/Cargo.toml heed/Cargo.toml
fi

# ...and replaces the `heed::` string by the `heed3::` one.
for file in $(find heed/src -type f -name "*.rs"); do
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' 's/heed::/heed3::/g' "$file"
    else
        sed -i 's/heed::/heed3::/g' "$file"
    fi
done

# Make it easier to rollback by doing a commit
git config --local user.email "ci@github.com"
git config --local user.name "The CI"
git commit -am 'remove-me: heed3 changes generate by the convert-to-heed3.sh script'

echo "Heed3 crate setup completed successfully. Configurations for the heed crate have been copied and modified."
echo "A commit (starting with remove-me) has been generated and must be deleted before merging into the main branch."
