
rm src/geoarrow*

GEOARROW_C_REF="d72a933da342958322242c27af8e3e7d763aae76"

curl -L \
    "https://github.com/geoarrow/geoarrow-c/archive/${GEOARROW_C_REF}.zip" \
    -o geoarrow.zip

unzip -d . geoarrow.zip

CMAKE_DIR=$(find . -name "geoarrow-c-*")

mkdir geoarrow-cmake
pushd geoarrow-cmake
cmake "../${CMAKE_DIR}" \
  -DGEOARROW_BUNDLE=ON -DGEOARROW_USE_RYU=ON -DGEOARROW_USE_FAST_FLOAT=ON \
  -DGEOARROW_NAMESPACE=RPkgGeoArrow
cmake --build .
cmake --install . --prefix=../src
popd

# Mangle -Wunused pragmas since they are there for a good reason
sed -i.bak -e "s|#pragma|/*ignore*/#pragma|" src/geoarrow.c
rm src/geoarrow.c.bak

rm geoarrow.zip
rm -rf geoarrow-c-*
rm -rf geoarrow-cmake
