#!/bin/bash

before=$(awk '{print $16}' /sys/block/ublkb1/stat)
sudo fio --name=fua --filename=/dev/ublkb1 --direct=1 --ioengine=io_uring \
  --rw=write --bs=4k --size=4M --iodepth=1 --sync=dsync
after=$(awk '{print $16}' /sys/block/ublkb1/stat)
echo "flush 증가: $((after - before))"
