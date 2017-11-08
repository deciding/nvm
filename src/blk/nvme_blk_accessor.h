//
// Created by robert on 7/11/17.
//

#ifndef NVM_NVME_BLK_ACCESSOR_H
#define NVM_NVME_BLK_ACCESSOR_H

#include <unordered_set>
#include <stdio.h>
#include "blk.h"
#include "../tree/blk_node_reference.h"
#include "../accessor/ns_entry.h"
#include "../accessor/qpair_context.h"
#include "../utils/rdtsc.h"

namespace tree {
    template<typename K, typename V, int CAPACITY>
    class blk_node_reference;
}

using namespace nvm;

template<typename K, typename V, int CAPACITY>
class nvme_blk_accessor: public blk_accessor<K, V> {
public:
    nvme_blk_accessor(const int& block_size): blk_accessor<K, V>(block_size) {
        writes_ = 0;
        write_cycles_ = 0;
        reads_ = 0;
        read_cycles_ = 0;
    };
    virtual node_reference<K, V>* allocate_ref() {
        blk_address addr = allocate();
        return new blk_node_reference<K, V, CAPACITY>(addr);
    };

    virtual node_reference<K, V>* create_null_ref() {
        return new blk_node_reference<K, V, CAPACITY>(-1);
    };

    virtual int open() {
        int status = nvm::nvm_utility::initialize_namespace();
        if (status != 0) {
            cout << "Errors in initialization of namespace." << endl;
            exit(1);
        } else {
            cout << "namespace is initialized." << endl;
        }
        qpair_ = nvm_utility::allocateQPair(1);
    }

    virtual blk_address allocate() {
        if (!freed_blk_addresses_.empty()) {
            auto it = freed_blk_addresses_.begin();
            blk_address blk_addr = *it;
            freed_blk_addresses_.erase(it);
            return blk_addr;
        } else {
            return blk_address(cursor_++);
        }
    }
    virtual void deallocate(const blk_address& address) {
        if (cursor_ == address - 1)
            cursor_ --;
        else {
            freed_blk_addresses_.insert(address);
        }
    }
    virtual int close() {
        qpair_->free_qpair();

        if (reads_ > 0)
            printf("[NVM:] total reads: %ld, average: %.2f us\n", reads_, cycles_to_microseconds(read_cycles_ / reads_));
        if (writes_ > 0)
            printf("[NVM:] total writes: %ld, average: %.2f us\n", writes_, cycles_to_microseconds(write_cycles_ / writes_));
    }
    virtual int read(const blk_address & blk_addr, void* buffer) {
        uint64_t start = ticks();
        qpair_->synchronous_read(buffer, this->block_size, blk_addr);
        read_cycles_ += ticks() - start;
        reads_++;
    }
    virtual int write(const blk_address & blk_addr, void* buffer) {
        uint64_t start = ticks();
        qpair_->synchronous_write(buffer, this->block_size, blk_addr);
        write_cycles_ += ticks() - start;
        writes_++;
    }

    virtual void* malloc_buffer() const {
        return spdk_dma_zmalloc(this->block_size, this->block_size, NULL);
    }
    virtual void free_buffer(void* buffer) const {
        spdk_dma_free(buffer);
    }

    void flush() {
    }
private:
    std::unordered_set<blk_address> freed_blk_addresses_;
    uint32_t cursor_;
    QPair* qpair_;
    uint64_t read_cycles_;
    uint64_t write_cycles_;
    uint64_t reads_;
    uint64_t writes_;
};

#endif //NVM_NVME_BLK_ACCESSOR_H