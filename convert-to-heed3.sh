# This script is meant to setup the heed3 crate.
#

if [[ -n $(git status -s) ]]; then
    echo "Error: Repository is git dirty, please commit or stash changes before running this script."
    exit 1
fi

set -e

# It basically copy the heed3/Cargo.toml file into
# the heed folder...
cp heed3/Cargo.toml heed/Cargo.toml

# ...and replaces the `heed::` string by the `heed3::` one.
for file in $(find heed/src -type f -name "*.rs"); do
    sed -i '' 's/heed::/heed3::/g' "$file"
done

echo "Heed3 crate setup completed successfully. Copied and modified configurations for the heed crate."
