#!/bin/bash

input="flist"

while IFS= read -r var
do
  diff /work/00410/huang/maverick2/lz4/test/${var} /tmp/fs_`id -u`/${var}
done < "$input"

