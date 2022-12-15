#!/bin/bash

if [ $(ls /tmp/scb/*.zip 2>/dev/null | wc -l) -gt 0 ]; then
  echo "creating symbolic links for .zip files in /tmp/scb into /assets"
  for f in /tmp/scb/*.zip; do
    ln -s $f /assets
  done
fi

echo "sleeping forever; create a bash shell to do something useful!"
sleep infinity
