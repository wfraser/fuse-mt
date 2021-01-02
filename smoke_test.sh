#!/bin/bash

set -e

cd example
mkdir mount
cargo build
cargo run src mount &

delay=0
while [ $delay -lt 5 ]; do
    if [ ! -f mount/main.rs ]; then
        echo "waiting for startup"
        delay=$(($delay + 1))
        sleep 1
    else
        break
    fi
done

if [ $delay -eq 5 ]; then
    echo "took too long to start up"
    exit 2
fi

# exclude the initial "total" line because it's not really accurate for virtual filesystems
diff <(ls -al src | grep -v '^total') <(ls -al mount | grep -v '^total')

diff src/main.rs mount/main.rs

if [ $(uname) = "Linux" ]; then
    fusermount -u mount
else
    umount mount
fi
rmdir mount
