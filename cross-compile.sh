#!/bin/bash
#
# Copyright © 2016-2025 The LmdbJava Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


set -o errexit

rm -rf lmdb
git clone --depth 1 --branch LMDB_0.9.31 https://github.com/LMDB/lmdb.git
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
