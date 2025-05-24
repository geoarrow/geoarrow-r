main() {
  local -r repo_url="https://github.com/apache/arrow-nanoarrow"
  # Check releases page: https://github.com/apache/arrow-nanoarrow/releases/
  local -r commit_sha=fe0e3a2745952c7bd415fdf09bda2a869bf1390a

  echo "Fetching $commit_sha from $repo_url"
  SCRATCH=$(mktemp -d)
  trap 'rm -rf "$SCRATCH"' EXIT

  local -r tarball="$SCRATCH/nanoarrow.tar.gz"
  wget -O "$tarball" "$repo_url/archive/$commit_sha.tar.gz"
  tar --strip-components 1 -C "$SCRATCH" -xf "$tarball"

  # Remove previous bundle
  rm -rf src/geoarrow/nanoarrow

  # Build the bundle
  python3 "${SCRATCH}/ci/scripts/bundle.py" \
      --include-output-dir=src \
      --source-output-dir=src \
      --symbol-namespace=GeoArrowRPkg
}

main
