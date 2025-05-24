
rm src/geoarrow*

GEOARROW_C_REF="5234b187523497bac5a163ecdd51108401bf8c40"

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

mv src/geoarrow.h src/geoarrow/geoarrow.h
mv src/geoarrow.hpp src/geoarrow/geoarrow.hpp

curl -L https://raw.githubusercontent.com/geoarrow/geoarrow-c/$GEOARROW_C_REF/src/geoarrow/fast_float.h \
  -o src/fast_float.h

curl -L https://raw.githubusercontent.com/geoarrow/geoarrow-c/$GEOARROW_C_REF/src/geoarrow/double_parse_fast_float.cc \
  -o src/double_parse_fast_float.cc

rm geoarrow.zip
rm -rf geoarrow-c-*
rm -rf geoarrow-cmake
