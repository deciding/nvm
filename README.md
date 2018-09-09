## Pull-Mode Asynchronous B+-Tree
This is a pull-mode asynchronous B+-tree (PA-tree) implementation, tailored to the performance characteristics of the NVM Express interface.

Additional information we need to know:
1. Should use Amazon Linux AMI instead of Ubuntu, since ubuntu doesn't contain uio modules
2. Should use update cmake and make version accordingly when they are reporting errors.
3. Should change the opts.core_mask when we have different number of cores
4. always check the meminfo to confirm we have enough amount of hugepages

### 1. Compile

1. Install spdk drive

```
bash configure_spdk.sh
```

2. Compile the source code

```
mkdir build
cd build
cmake ..
make
```

### 2. Run the benchmark

1. Mount nvme device
```
sudo ../spdk/src/scripts/setup.sh
```

2. Run the benchmark
```
sudo ./tree_benchmark
```

Note: sudo previlage is required to allocate NVMe namespace.
