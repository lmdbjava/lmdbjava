#!/bin/bash

set -o errexit

rm -rf lmdb
git clone --depth 1 --branch LMDB_0.9.29 https://github.com/LMDB/lmdb.git
pushd lmdb/libraries/liblmdb
trap popd SIGINT

# zig targets | jq -r '.libc[]'
for target in aarch64-linux-gnu \
              aarch64-macos-none \
              x86_64-linux-gnu \
              x86_64-macos-none \
              x86_64-windows-gnu
do
  echo "##### Building $target ####"
  make -e clean liblmdb.so CC="zig cc -target $target" AR="zig ar"
  if [[ "$target" == *-windows-* ]]; then
    extension="dll"
  else
    extension="so"
  fi
  cp -v liblmdb.so ../../../src/main/resources/org/lmdbjava/$target.$extension
done

ls -l ../../../src/main/resources/org/lmdbjava
