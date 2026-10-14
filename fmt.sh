#!/bin/bash

for dir in core_lib app/main/src-tauri; do
  find "$dir" -name '*.rs' -not -path "*/target/*" -exec rustfmt {} +
done