#ifndef FILE_AIO_BLK_ACCESSOR_H
#define FILE_AIO_BLK_ACCESSOR_H
#include <deque>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_set>
#include <queue>
#include <libaio.h>
#include <errno.h>
#include <string.h>/* memset() */
#include "../tree/blk_node_reference.h"
#include "blk.h"
#include "../utils/rdtsc.h"
#include "asynchronous_accessor.h"
#include "blk_file_cache.h"
#include "../utils/sync.h"
#include "../scheduler/ready_state_estimator.h"
#include "../scheduler/linear_regression_estimator.h"

#define AIO_READ 0
#define AIO_WRITE 1

namespace tree {
    template<typename K, typename V, int CAPACITY>
    class blk_node_reference;
}

struct iocb_data{
    io_callback_t cb;
    void* param;
};

void io_set_callback_with_param(struct iocb *iocb, io_callback_t cb, void* param){
    iocb_data* idata=new iocb_data;
    idata->cb=cb;
    idata->param=param;
    iocb->data = (void *)idata;
}

int io_queue_run_with_param(io_context_t ctx){
	static struct timespec timeout = { 0, 0 };
	struct io_event event;
	int ret;
    int cnt=0;

	/* FIXME: batch requests? */
	while (1 == (ret = io_getevents(ctx, 0, 1, &event, &timeout))) {
		iocb_data* idata = (iocb_data*)event.data;
		io_callback_t cb = (io_callback_t)idata->cb;
		void* param = idata->param;
        
		struct iocb *iocb = event.obj;
        iocb->data=param;

		cb(ctx, iocb, event.res, event.res2);
        cnt++;
	}

	return ret<0?ret:cnt;
}

using namespace std;
template<typename K, typename V, int CAPACITY>
class file_aio_blk_accessor: public blk_accessor<K, V>{
public:
    explicit file_aio_blk_accessor(const char* path, const uint32_t& block_size) : path_(path), blk_accessor<K, V>(block_size), cursor_(0), io_id_generator_(0), recent_reads_(0), recent_writes_(0) {
        //estimator_ = new ready_state_estimator(1000, 2000);
        cache_ = new blk_file_cache(block_size, 100000);
    }

    ~file_aio_blk_accessor() {
        if (cache_) {
            cache_->print();
        }
        delete cache_;
        cache_ = 0;
    }

    node_reference<K, V>* allocate_ref() override {
        blk_address addr = allocate();
        return new blk_node_reference<K, V, CAPACITY>(addr);
    };

    node_reference<K, V>* create_null_ref() override {
        return new blk_node_reference<K, V, CAPACITY>(-1);
    };

    int open() override{
        fd_ = ::open(path_, O_CREAT|O_TRUNC|O_RDWR, S_IRWXU|S_IRWXG|S_IRWXO);
//        fd_ = ::open(path_, O_CREAT|O_TRUNC|O_RDWR|O_DIRECT, S_IRWXU|S_IRWXG|S_IRWXO);
        if(fd_<0)
            return errno;
        
        // TODO: deal with the queue size
        int ret = io_queue_init(128, &ctx);
        return ret==0 ? 0 : errno;
    }
    
    virtual int close() {
        if (!closed_) {
            int ret = io_queue_release(ctx);
            if (ret < 0) {
                perror("io_destroy error");
                return -1;
            }
          
            int fd = fd_;
            fd_ = -1;
            closed_ = true;
            return ::close(fd);
        }
        return 0;
    }
    
    blk_address allocate() override {
        if (!freed_blk_addresses_.empty()) {
            auto it = freed_blk_addresses_.begin();
            blk_address blk_addr = *it;
            freed_blk_addresses_.erase(it);
            return blk_addr;
        } else {
            return blk_address(cursor_++);
        }
    }

    void deallocate(const blk_address& address) override {
        if (cursor_ == address - 1)
            cursor_ --;
        else {
            freed_blk_addresses_.insert(address);
        }
    }


    //not supposed to be called
    int read(const blk_address& address, void *buffer) override {
        uint64_t start = ticks();
        struct iocb cb;
        struct iocb *cbs[1];
        struct io_event events[1];
        int ret;
        if (!is_address_valid(address))
            return 0;
        lock_.acquire();
        if (cache_ && cache_->read(address, buffer)) {
            lock_.release();
            this->metrics_.read_cycles_ += ticks() - start;
            this->metrics_.reads_++;
            return this->block_size;
        }
        lock_.release();

        io_prep_pread(&cb, fd_, buffer, this->block_size, address*this->block_size);
        cbs[0] = &cb;

        ret = io_submit(ctx, 1, cbs);
        if (ret != 1) {
            if (ret < 0)
                perror("io_submit error");
            else
                // less than expected, but no errno
                fprintf(stderr, "could not sumbit IOs");
            return  -1;
        }

        ret = io_getevents(ctx, 1, 1, events, NULL);

        this->metrics_.read_cycles_ += ticks() - start;
        this->metrics_.reads_++;

        if (cache_) {
            blk_file_cache::cache_unit unit;
            lock_.acquire();
            bool evicted = cache_->write(address, buffer, false, unit);
            lock_.release();
            if (evicted) {
                io_prep_pwrite(&cb, fd_, unit.data, this->block_size, unit.id * this->block_size);
                cbs[0]=&cb;
                ret = io_submit(ctx, 1, cbs);
                if (ret != 1) {
                    if (ret < 0)
                        perror("io_submit error");
                    else
                        // less than expected, but no errno
                        fprintf(stderr, "could not sumbit IOs");
                    return  -1;
                }
                ret = io_getevents(ctx, 1, 1, events, NULL);
                cache_->free_block(unit.data);
            }
        }

        return ret;
    }

    //not supposed to be called
    int write(const blk_address& address, void *buffer) override {
        uint64_t start = ticks();
        struct iocb cb;
        struct iocb *cbs[1];
        struct io_event events[1];
        int ret;
        if (!is_address_valid(address))
            return 0;
        if (cache_) {
            blk_file_cache::cache_unit unit;
            lock_.acquire();
            if (cache_->write(address, buffer, true, unit)) {
                lock_.release();
                if (unit.dirty) {
                    io_prep_pwrite(&cb, fd_, unit.data, this->block_size, unit.id * this->block_size);
                    cbs[0]=&cb;
                    ret = io_submit(ctx, 1, cbs);
                    if (ret != 1) {
                        if (ret < 0)
                            perror("io_submit error");
                        else
                            // less than expected, but no errno
                            fprintf(stderr, "could not sumbit IOs");
                        return  -1;
                    }
                    ret = io_getevents(ctx, 1, 1, events, NULL);
                    if(ret<0){
                        perror("io_getevents error");
                    }
                }
                cache_->free_block(unit.data);
                unit.data = 0;
            } else {
                lock_.release();
            }
            return this->block_size;
        }

        io_prep_pwrite(&cb, fd_, buffer, this->block_size, address * this->block_size);
        cbs[0]=&cb;
        ret = io_submit(ctx, 1, cbs);
        if (ret != 1) {
            if (ret < 0)
                perror("io_submit error");
            else
                // less than expected, but no errno
                fprintf(stderr, "could not sumbit IOs");
            return  -1;
        }
        ret = io_getevents(ctx, 1, 1, events, NULL);
        if(ret<0){
            perror("io_getevents error");
        }

        this->metrics_.write_cycles_ += ticks() - start;
        this->metrics_.writes_++;
        return ret;
    }

    void* malloc_buffer() const override {
        return malloc(this->block_size);
    }
    void free_buffer(void* buffer) const override {
        free(buffer);
    }

    void flush() override {
//        fsync(fd_);
    }

    void asynch_read(const blk_address& blk_addr, void* buffer, call_back_context* context) override {
        int64_t start = ticks();
        struct iocb cb;
        struct iocb *cbs[1];
        int ret;
        uint64_t io_id = io_id_generator_++;
        if (cache_ && cache_->read(blk_addr, buffer)) {
            context->transition_to_next_state();
            ready_contexts_.push_back(context);
            this->metrics_.add_read_latency(ticks() - start);
            return;
        }

        aio_callback_para* para = new aio_callback_para;
        para->start_time = start;
        para->type = AIO_READ;
        para->context = context;
        para->id = blk_addr;
        para->accessor = this;
        para->buffer = buffer;
        para->io_id = io_id;
        para->evicted = false;
        para->estimated_completion = estimator_.get_current_read_latency() + para->start_time;
        estimator_.register_read_io(io_id, start);
        

        io_prep_pread(&cb, fd_, buffer, this->block_size, blk_addr*this->block_size);
        io_set_callback_with_param(&cb,context_call_back_function,(void*)para);
        cbs[0] = &cb;
        ret = io_submit(ctx, 1, cbs);
        if (ret != 1) {
            if (ret < 0)
                perror("io_submit error");
            else
                // less than expected, but no errno
                fprintf(stderr, "could not sumbit IOs");
            printf("blk_addr: %ld, block_size: %d\n", blk_addr, this->block_size);
            return;
        }

        //int status = qpair_->submit_read_operation(buffer, this->block_size, blk_addr, context_call_back_function, para);
        this->metrics_.pending_commands_ ++;
    }
    
    std::vector<call_back_context*>& get_ready_contexts() override {
        return ready_contexts_;
    }

    int process_completion(int max = 0) {
        int processed =  process_completion_();
        this->metrics_.pending_commands_ -= processed;
        this->metrics_.pending_command_counts_.push_back(this->metrics_.pending_commands_);
        return processed;
    }

    void asynch_write(const blk_address& blk_addr, void* buffer, call_back_context* context) override {
        uint64_t start = ticks();
        struct iocb cb;
        struct iocb *cbs[1];
        int ret;
        uint64_t io_id = io_id_generator_++;
        aio_callback_para* para = new aio_callback_para;
        para->start_time = start;
        para->type = AIO_WRITE;
        para->context = context;
        para->id = blk_addr;
        para->accessor = this;
        para->buffer = buffer;
        para->io_id = io_id;
        para->evicted = false;

        blk_file_cache::cache_unit unit;
        if (cache_ && cache_->is_cached(blk_addr)) {
            if (strong_consistent) {
                cache_->invalidate(blk_addr);
            } else {
                blk_file_cache::cache_unit unit;
                bool evicted = cache_->write(blk_addr, buffer, true, unit);
                assert(!evicted);
                context->transition_to_next_state();
                ready_contexts_.push_back(context);
                this->metrics_.add_read_latency(ticks() - start);
                return;
            }
        }
        estimator_.register_write_io(io_id, start);

        io_prep_pwrite(&cb, fd_, buffer, this->block_size, blk_addr*this->block_size);
        io_set_callback_with_param(&cb,context_call_back_function,(void*)para);
        cbs[0] = &cb;
        ret = io_submit(ctx, 1, cbs);
        if (ret != 1) {
            if (ret < 0)
                perror("io_submit error");
            else
                // less than expected, but no errno
                fprintf(stderr, "could not sumbit IOs");
            printf("blk_addr: %ld, block_size: %d\n", blk_addr, this->block_size);
            return;
        }

        //int status = qpair_->submit_write_operation(buffer, this->block_size, blk_addr, context_call_back_function, para);
        this->metrics_.pending_commands_ ++;
    }

    static void context_call_back_function(io_context_t ctx, struct iocb *iocb, long res, long res2) {

        aio_callback_para* para = reinterpret_cast<aio_callback_para*>(iocb->data);


        if (para->type == AIO_READ) {
            para->accessor->estimator_.remove_read_io(para->io_id);
            para->accessor->metrics_.add_read_latency(ticks() - para->start_time);

            if (para->io_id % 1024 == 0) {
                int latency = para->accessor->metrics_.get_recent_avg_read_latency_in_cycles();
                para->accessor->estimator_.update_read_latency_in_cycles(latency);
            }
            para->context->set_tag(CONTEXT_READ_IO);
            para->accessor->recent_reads_++;

        } else {
            para->accessor->estimator_.remove_write_io(para->io_id);
            para->accessor->metrics_.add_write_latency(ticks() - para->start_time);
            if (para->io_id % 1024  == 0) {
                int latency = para->accessor->metrics_.get_recent_avg_write_latency_in_cycles();
                para->accessor->estimator_.update_write_latency_in_cycles(latency);
            }
            para->context->set_tag(CONTEXT_WRITE_IO);
            para->accessor->recent_writes_++;
        }

        blk_file_cache::cache_unit unit;
        if (para->accessor->cache_ && para->accessor->cache_->write(para->id, para->buffer, false, unit)) {
            // cache is enabled and a unit is evicted.
            if (!unit.dirty)
                para->accessor->cache_->free_block(unit.data);
            else {
                para->accessor->asynch_write_evicted(unit.id, unit.data, para->context);
                return; // the context transition will be done in the call_back function.
            }
        }

        if (para->accessor->cache_ && para->evicted) {
            para->accessor->cache_->free_block(para->buffer);
        }

        para->context->transition_to_next_state();
        para->accessor->ready_contexts_.push_back(para->context);

        delete para;
    }

    void asynch_write_evicted(const blk_address& blk_addr, void* buffer, call_back_context* context) {
        uint64_t start = ticks();
        struct iocb cb;
        struct iocb *cbs[1];
        struct io_event events[1];
        int ret;
        uint64_t io_id = io_id_generator_++;
        aio_callback_para* para = new aio_callback_para;
        para->start_time = start;
        para->type = AIO_WRITE;
        para->context = context;
        para->id = blk_addr;
        para->accessor = this;
        para->buffer = buffer;
        para->io_id = io_id;
        para->evicted = true;
        if (cache_) {
            cache_->invalidate(blk_addr);
        }
        estimator_.register_write_io(io_id, start);

        io_prep_pwrite(&cb, fd_, buffer, this->block_size, blk_addr*this->block_size);
        io_set_callback_with_param(&cb,context_call_back_function,(void*)para);
        cbs[0] = &cb;
        ret = io_submit(ctx, 1, cbs);
        if (ret != 1) {
            if (ret < 0)
                perror("io_submit error");
            else
                // less than expected, but no errno
                fprintf(stderr, "could not sumbit IOs");
            printf("blk_addr: %ld, block_size: %d\n", blk_addr, this->block_size);
            return;
        }
        //int status = qpair_->submit_write_operation(buffer, this->block_size, blk_addr, context_call_back_function, para);
        this->metrics_.pending_commands_ ++;
    }

    int32_t process_ready_contexts(int32_t max = 1) {
        int32_t processed = 0;
        while (processed < max && ready_contexts_.size() > 0) {
            call_back_context* context = ready_contexts_.front();
            assert(false);
            context->run();
            processed++;
        }
        return processed;
    }

    std::vector<call_back_context*>& get_ready_context_queue() override {
        return ready_contexts_;
    }

    struct aio_callback_para {
        call_back_context* context;
        int64_t id;
        int64_t start_time;
        file_aio_blk_accessor* accessor;
        int32_t type;
        void* buffer;
        uint64_t io_id;
        uint64_t estimated_completion;
        bool evicted;
    };

    std::string get_name() const {
        return std::string("Libaio");
    }

    ready_state_estimator& get_ready_state_estimator() override{
        return estimator_;
    }

    virtual int get_and_reset_recent_reads() {
        int ret = recent_reads_;
        recent_reads_ = 0;
        return ret;
    }

    virtual int get_and_reset_recent_writes() {
        int ret = recent_writes_;
        recent_writes_ = 0;
        return ret;
    }

protected:
    int process_completion_() {
        int ret=io_queue_run_with_param(ctx);
        if (ret < 0) {
            perror("errors in process_completions!");
            return ret;
        }
        return ret;
    }

private:
    bool is_address_valid(const blk_address& address) const {
        return address < cursor_ && freed_blk_addresses_.find(address) == freed_blk_addresses_.cend();
    }

    //struct pending_io {
    //    uint64_t id;
    //    enum {read_io, write_io} type;
    //};
    std::unordered_set<blk_address> freed_blk_addresses_;
    uint32_t cursor_;
    //std::unordered_map<int64_t, string> pending_io_;
    SpinLock lock_;
    //std::vector<call_back_context*> pending_contexts_;
    std::vector<call_back_context*> ready_contexts_;
    blk_file_cache *cache_;
    //uint32_t wait_for_completion_counts_;
    //ready_state_estimator estimator_;
    linear_regression_estimator estimator_;
    uint64_t io_id_generator_;
    //std::vector<pending_io> pending_ids_;
    int recent_reads_, recent_writes_;

    const bool strong_consistent = false;

    const char* path_;
    int fd_;
    bool closed_;
    io_context_t ctx;
};
#endif //FILE_AIO_BLK_ACCESSOR_H
