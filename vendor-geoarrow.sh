
# rm src/geoarrow*

# GEOARROW_C_REF="4a755ea6a09b8e842a9ccaef6ef05e6e3870f973"

# curl -L \
#     "https://github.com/geoarrow/geoarrow-c/archive/${GEOARROW_C_REF}.zip" \
#     -o geoarrow.zip

# unzip -d . geoarrow.zip

CMAKE_DIR=$(find . -name "geoarrow-c-*")

mkdir geoarrow-cmake
pushd geoarrow-cmake
cmake "../${CMAKE_DIR}" \
  -DGEOARROW_BUNDLE=ON -DGEOARROW_USE_RYU=ON -DGEOARROW_USE_FAST_FLOAT=ON \
  -DGEOARROW_NAMESPACE=RPkgGeoArrow
cmake --build .
cmake --install . --prefix=../src
popd

rm geoarrow.zip
# rm -rf geoarrow-c-*
rm -rf geoarrow-cmake
