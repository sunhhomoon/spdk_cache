#!/bin/bash

sudo scripts/rpc.py nvmf_create_transport -t RDMA
sudo scripts/rpc.py bdev_nvme_attach_controller -b Nvme0 -t PCIe -a 0000:86:00.0
sudo scripts/rpc.py bdev_malloc_create -b Malloc0 1024 512
sudo scripts/rpc.py bdev_ocf_create Cache0 wb Malloc0 Nvme0n1
sudo scripts/rpc.py nvmf_create_subsystem nqn.2016-06.io.spdk:cnode1 -a -s SPDK001
sudo scripts/rpc.py nvmf_subsystem_add_ns nqn.2016-06.io.spdk:cnode1 Cache0
sudo scripts/rpc.py nvmf_subsystem_add_listener nqn.2016-06.io.spdk:cnode1 -t rdma -a 10.0.0.2 -s 4420
