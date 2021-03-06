//
// Created by Li Wang on 11/12/17.
//

#ifndef NVM_PULL_BASED_B_PLUS_TREE_H
#define NVM_PULL_BASED_B_PLUS_TREE_H
//
// Created by robert on 9/11/17.
//

#include <queue>
#include <atomic>
#include <chrono>
#include <deque>
#include <stack>
#include <thread>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include <boost/lockfree/queue.hpp>
//#include "in_nvme_b_plus_tree.h"
#include "vanilla_b_plus_tree.h"
#include "../utils/sync.h"
#include "../context/call_back.h"
#include "../tree/blk_node_reference.h"
#include "../context/barrier_manager.h"
#include "../utils/concurrent_queue.h"
//#include "../scheduler/scheduler.h"

#define SEARCH_REQUEST 1
#define INSERT_REQUEST 2
using namespace std;

typedef void (*callback_function)(void*);

typedef SpinLock request_lock;

namespace tree {


    template<typename K, typename V>
    class request {
    public:
        request(int t): type(t), ownership(true){};
        int type;
        K key;
        bool ownership;
        SpinLock* semaphore;
        uint64_t start;
        uint64_t admission;
        uint64_t graduation;
    };

    template<typename K, typename V>
    class search_request: public request<K, V> {
    public:
        search_request(): request<K, V>(SEARCH_REQUEST) {};
    public:
        V *value;
        bool *found;
        callback_function cb_f;
        void* args;
    };

    template<typename K, typename V>
    class insert_request: public request<K, V>  {
    public:
        insert_request(): request<K, V>(INSERT_REQUEST) {};
        V value;
        bool succeed;
        callback_function  cb_f;
        void* args;
    };

    template<typename K, typename V, int CAPACITY>
    class pull_based_b_plus_tree : public VanillaBPlusTree<K, V, CAPACITY> {
    public:


//        class search_request_with_semaphore: public search_request {
//        public:
//            Semaphore semaphore;
//        };

    public:
        pull_based_b_plus_tree(uint32_t queue_length) : VanillaBPlusTree<K, V, CAPACITY>(0) {
            queue_free_slots_ = Semaphore(queue_length);
            free_context_slots_ = queue_length;
            pending_request_ = 0;
            queue_length_ = queue_length;
            barrier_handling_cycles_ = 0;
//          request_queue_ = moodycamel::ConcurrentQueue<request<K, V> *>(256);
        };

        ~pull_based_b_plus_tree() {
            working_thread_terminate_flag_ = true;
//            destroy_free_contexts();
        }

        virtual void init() {
            create_and_init_blk_accessor();
//            create_free_contexts();
            VanillaBPlusTree<K, V, CAPACITY>::init();
            working_thread_terminate_flag_ = false;
            pthread_create(&thread_handle_, NULL, pull_based_b_plus_tree::schedule, this);
//            pthread_create(&thread_handle_, NULL, pull_based_b_plus_tree::context_based_process, this);
        }

        virtual void create_and_init_blk_accessor() = 0;

        virtual void close() {

            working_thread_terminate_flag_ = true;
            if (pthread_join(thread_handle_, NULL) != 0) {
                printf("failed to join the thread!\n");
            }
            VanillaBPlusTree<K, V, CAPACITY>::close();
        }

        bool asynchronous_search_with_callback(search_request<K, V>* request) {
//            lock_.acquire();
//            request_queue_.push(request);
//            request_queue_size_++;
//            lock_.release();
//            pending_request_ ++;
//            return true;

//            bool succeed = request_queue_.enqueue(request);
//            assert(succeed);
//            pending_request_++;
//            return true;

            pending_request_++;
            while(!request_queue_.push(request));
            return true;
        }

        bool asynchronous_insert_with_callback(insert_request<K, V>* request) {
//            lock_.acquire();
//            request_queue_.push(request);
//            request_queue_size_++;
//            lock_.release();
//            pending_request_++;
//            return true;

//            bool succeed = request_queue_.enqueue(request);
//            assert(succeed);
//            pending_request_++;
//            return true;

            pending_request_++;
            while(!request_queue_.push(request));
            return true;
        }

        int get_pending_requests() const {
            return pending_request_.load();
        }

        request<K, V>* atomic_dequeue_request() {
//            request<K, V>* ret = nullptr;
//            lock_.acquire();
//            if (request_queue_.size() > 0) {
//                ret = request_queue_.front();
//                request_queue_.pop();
//                request_queue_size_--;
//            }
//            lock_.release();
//            return ret;


//            request<K, V>* ret;
//            if (request_queue_.try_dequeue(ret))
//                return ret;
//            else
//                return nullptr;

            request<K, V>* ret;
            if (request_queue_.pop(ret))
                return ret;
            else
                return nullptr;
        }


        static void *schedule(void* para) {
            pull_based_b_plus_tree* tree = reinterpret_cast<pull_based_b_plus_tree*>(para);
            tree->create_free_contexts();
//            naive_scheduler sched(tree);
            latency_aware_scheduler sched(tree);
            sched.run();
        }

//        static void *context_based_process(void* para) {
//            pull_based_b_plus_tree* tree = reinterpret_cast<pull_based_b_plus_tree*>(para);
//            tree->create_free_contexts();
//#ifndef __APPLE__
//            cpu_set_t mask;
//            CPU_ZERO(&mask);
//            CPU_SET(2, &mask);
//            pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
//#endif
//
//            int64_t t1 = ticks();
//
//            uint64_t admission_cycles = 0;
//            uint64_t blk_completion_cycles = 0;
//            uint64_t blk_ready_cycles = 0;
//            uint64_t manager_ready_cycles = 0;
//
//            uint64_t admission = 0, blk_completion = 0, blk_ready = 0, manager_ready = 0;
//
//            uint64_t loops = 0, last_loop = 0;
//
//            uint64_t empty_queue_time = 0;
//
//            std::vector<int> blk_processed;
//
//            std::vector<call_back_context*>& blk_ready_contexts = tree->blk_accessor_->get_ready_contexts();
//            std::vector<call_back_context*>& barrier_ready_contexts = tree->manager.get_ready_contexts();
//
//            ready_state_estimator &estimator = tree->blk_accessor_->get_ready_state_estimator();
//
//            int64_t last = 0;
//            uint64_t last_call_completion = 0;
//            uint64_t context_id_generator = 0;
//
//            uint64_t delay_start = 0;
//            uint64_t delay = 0;
//
////            scheduler<K, V, CAPACITY> sche(this);
//
//            while (!tree->working_thread_terminate_flag_ || tree->pending_request_.load() > 0) {
//                loops++;
////                usleep(1);
//                request<K, V>* request;
//                int64_t last = ticks();
//                int state;
//                uint64_t start;
//                int new_arrivals = 0;
//
////                int probe_granularity = max(8, min(32, (tree->pending_request_.load() - 1) / 4 + 1));
//                int probe_granularity = 16;
//
////                if (rand() % 10000 < 1)
////                    printf("granularity = %d.\n", probe_granularity);
//                int processed = 0;
//                do {
//                    int32_t free = tree->free_context_slots_.load();
////                    while (tree->free_context_slots_.load() > 0 && (request = tree->atomic_dequeue_request()) != nullptr) {
////                while (free-- > 0 && (request = tree->atomic_dequeue_request()) != nullptr) {
//                    new_arrivals = 0;
//                    if (free-- && (request = tree->atomic_dequeue_request()) != nullptr) {
//
//
////                    printf("admission: %.2f us, blk_com: %.2f us, blk_ready: %.2f us, manager_ready: %.2f us\n",
////                           cycles_to_microseconds(admission_cycles),
////                           cycles_to_microseconds(blk_completion_cycles),
////                           cycles_to_microseconds(blk_ready_cycles),
////                           cycles_to_microseconds(manager_ready_cycles));
////                    printf("Count: adm: %d, blk_com: %d, blk_read: %d, manager: %d\n", admission, blk_completion, blk_ready, manager_ready);
//
////                    admission_cycles = 0;
////                    blk_completion_cycles = 0;
////                    blk_ready_cycles = 0;
////                    manager_ready_cycles = 0;
////                    admission = 0, blk_completion = 0, blk_ready = 0, manager_ready = 0;
//
//                        request->admission = ticks();
//                        call_back_context *context;
//                        if (request->type == SEARCH_REQUEST) {
//                            context = tree->get_free_search_context();
//                            reinterpret_cast<search_context *>(context)->init(
//                                    reinterpret_cast<search_request<K, V> *>(request));
//                        } else {
//                            context = tree->get_free_insert_context();
//                            reinterpret_cast<insert_context *>(context)->init(
//                                    reinterpret_cast<insert_request<K, V> *>(request));
//                        }
//                        context->set_id(context_id_generator++);
//                        tree->free_context_slots_--;
//                        start = ticks();
//                        context->run();
//                        admission++;
//                        admission_cycles += ticks() - start;
//                        new_arrivals ++;
//                    }
////                } while (tree->manager.process_ready_context(tree->queue_length_));
////                process_ready_contexts(blk_ready_contexts, tree->queue_length_);
////                process_ready_contexts(barrier_ready_contexts, tree->queue_length_);
////                } while (process_ready_contexts(blk_ready_contexts, tree->queue_length_) || process_ready_contexts(barrier_ready_contexts, tree->queue_length_));
//                    processed = 0;
//                    start = ticks();
//                    processed += process_ready_contexts(blk_ready_contexts, probe_granularity);
//                    blk_ready++;
//                    blk_ready_cycles += ticks() - start;
//
//                    start = ticks();
//                    processed += process_ready_contexts(barrier_ready_contexts, probe_granularity);
//                    manager_ready++;
//                    manager_ready_cycles += ticks() - start;
////                    printf("%d processed \n", processed);
//                } while (ticks() - delay_start < delay && (new_arrivals || processed));
//
//                int64_t current_tick = ticks();
//                bool timeout = false;
//                int64_t cycles_to_wait = INT64_MAX;
//                int64_t cycles_to_wait_for_write = INT64_MAX;
//                int estimated_write = 0;
//                const int64_t max_waiting_cycles = 100000;
////                const int64_t max_waiting_cycles = 1000;
//                delay_start = ticks();
//                if ((timeout = ((current_tick - last_call_completion) > max_waiting_cycles))
//                    || (cycles_to_wait_for_write = max((int64_t)0, estimator.estimate_the_time_to_get_desirable_ready_write_state(1, current_tick) - current_tick)) == 0
//                    || (cycles_to_wait = max((int64_t) 0,
//                                             estimator.estimate_the_time_to_get_desirable_ready_state(probe_granularity, current_tick) - current_tick)) == 0) {
//                    start = ticks();
//                    const int processed = tree->blk_accessor_->process_completion(probe_granularity);
//                    if (timeout) {
////                        printf("%d (e) vs %d (a) timeout (%f us)\n", probe_granularity, processed,
////                               cycles_to_microseconds(current_tick - last_call_completion));
////                        printf("pending: %d, pending_write: %d\n", estimator.get_number_of_pending_state(), estimator.get_number_of_pending_write_state());
////                        printf("loops: %ld\n", loops - last_loop);
//                        last_loop = loops;
//                    } else if (cycles_to_wait_for_write == 0) {
////                        printf("%d (e) vs %d (a) write\n", 1, processed);
//                    } else {
////                        printf("%d (e) vs %d (a) REAL\n", probe_granularity, processed);
//                    }
//                    last_call_completion = ticks();
//                    blk_processed.push_back(processed);
//                    blk_completion++;
//                    blk_completion_cycles += ticks() - start;
////                    printf("%d processed in blk_completion\n", processed);
//                }
////                else if (blk_ready_contexts.empty() && barrier_ready_contexts.empty() && tree->request_queue_.empty()) {
////                else if (estimator.get_number_of_pending_state() > tree->queue_length_ / 3 * 2) {
////                    usleep(10);
////                    std::this_thread::sleep_for(std::chrono::nanoseconds((int)cycles_to_nanoseconds(cycles_to_wait)));
////                }
//
//                delay = min(max_waiting_cycles, min(cycles_to_wait_for_write, cycles_to_wait));
//
////                printf("delay: %ld\n", delay);
//
////                int processed = 0;
//                do {
//                    processed = 0;
//                    start = ticks();
//                    processed += process_ready_contexts(blk_ready_contexts, probe_granularity);
//                    blk_ready++;
//                    blk_ready_cycles += ticks() - start;
//
//                    start = ticks();
//                    processed += process_ready_contexts(barrier_ready_contexts, probe_granularity);
//                    manager_ready++;
//                    manager_ready_cycles += ticks() - start;
////                    printf("%d processed \n", processed);
//                } while (ticks() - delay_start < delay && processed > 0);
//
////                do {
////                    start = ticks();
////                    process_ready_contexts(barrier_ready_contexts, 8);
////                    manager_ready++;
////                    manager_ready_cycles += ticks() - start;
////                } while (ticks() - delay_start < delay);
//
//            }
//            tree->destroy_free_contexts();
//
//            printf("loops: %ld\nblk_ready: %ld, blk_ready_cycle: %ld\nbarrier_ready: %ld, barrier_ready_cycles: %ld\nblk_com: %ld, blk_com_cycles: %ld\n",
//                   loops/1000000, blk_ready/1000000, blk_ready_cycles/1000000, manager_ready/1000000, manager_ready_cycles/1000000, blk_completion/1000000, blk_completion_cycles/1000000);
//
//            long sum = 0;
//            for (auto it = blk_processed.begin(); it != blk_processed.end(); it++) {
//                sum += *it;
//            }
//
//            double avg = (double)sum / blk_processed.size();
//
//            double var = 0;
//
//            for (auto it = blk_processed.begin(); it != blk_processed.end(); it++) {
//                var += pow((avg - *it), 2);
//            }
//            var /= blk_processed.size();
//
//            printf("mean: %f, var: %.6f\n", avg, var);
//
//            return nullptr;
//        }

        static bool compare(call_back_context* l, call_back_context* r) {
            if (l->tag == r->tag)
                return l->id < r->id;
            else
                return l->tag < r->tag;
        }

        static int process_ready_contexts(std::deque<call_back_context*>& ready_contexts, int max = 1) {
            if (ready_contexts.empty())
                return 0;
            std::sort(ready_contexts.begin(), ready_contexts.end(), compare);
            int processed = 0;
            while (processed < max && ready_contexts.size() > 0) {
                call_back_context* context = ready_contexts.front();
                ready_contexts.pop_front();
                context->run();
                processed++;
            }
            return processed;
        }

        virtual void sync() override {
            while(pending_request_.load()> 0) {
                usleep(1000);
            }
        }

    private:

        class insert_context: public call_back_context {
        public:
            insert_context(pull_based_b_plus_tree* tree, insert_request<K, V>* request):
                    call_back_context(), tree_(tree){
                buffer_ = tree_->blk_accessor_->malloc_buffer();
                buffer_2 = tree_->blk_accessor_->malloc_buffer();
//                init(request);
            }

            ~insert_context() {
                int64_t start = ticks();
                tree_->blk_accessor_->free_buffer(buffer_);
                tree_->blk_accessor_->free_buffer(buffer_2);
                buffer_ = 0;
                buffer_2 = 0;
//                printf("deallocation time: %.2f ns\n", cycles_to_nanoseconds(ticks() - start));
            }

            void init(insert_request<K, V> *request) {
                request_ = request;
                int64_t start = ticks();
                node_ref_ = -1;
                free_slot_available_in_parent_ = false;
                refer_to_root_ = false;
                optimistic_ = true;
                pending_parent_nodes_.clear();
                next_visit_is_leaf_node_ = tree_->depth_ == 1;
                current_node_level_ = tree_->get_height();
                this->reset_state();
//                printf("allocation time: %.2f ns\n", cycles_to_nanoseconds(ticks() - start));
            }

            int run() {
//                while(true)
                int64_t last;
                last = ticks();
                int current = this->current_state;
                switch (this->current_state) {
                    case 0: {
                        last_state_1 = false;
                        if (node_ref_ == -1) {
                            refer_to_root_ = true;
                            node_ref_ = tree_->root_->get_unified_representation();
                            current_node_level_ = tree_->get_height();
//                            printf("begin to insert [%d], root is %lld\n", request_->key,
//                                   tree_->root_->get_unified_representation());
                        }
                        set_next_state(10001);
//                        printf("[%d] --> <%lld>\n", request_->key, node_ref_->get_unified_representation());
                        bool obtained_barrier;
                        uint64_t barrier_start = ticks();
                        if (optimistic_ && !next_visit_is_leaf_node_)
                            obtained_barrier = tree_->manager.request_read_barrier(node_ref_, this);
                        else
                            obtained_barrier = tree_->manager.request_write_barrier(node_ref_, this);
                        tree_->barrier_handling_cycles_ += ticks() - barrier_start;

                        if (obtained_barrier) {
                            transition_to_next_state();
//                            printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                            return run();
//                            continue;
                        } else {
//                            printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                            return CONTEXT_TRANSIT;
                        }
                    }
                    case 10001: {
                        last_state_1 = false;
                        if (refer_to_root_) {
                            if (obtained_barriers_.back().barrier_id_ != tree_->root_->get_unified_representation()) {
                                // root was updated
//                                printf("[%d]: detected root update!\n", request_->key);
                                release_all_barriers();
                                refer_to_root_ = true;
                                node_ref_ = -1;
                                set_next_state(0);
                                transition_to_next_state();
//                                printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                                return run();
//                                continue;
                            } else {
                                refer_to_root_ = false;
                            }
                        }
                        if (optimistic_) {
                            barrier_token latest_token = obtained_barriers_.back();
                            obtained_barriers_.pop_back();
                            release_all_barriers();
                            obtained_barriers_.push_back(latest_token);
                        } else if (free_slot_available_in_parent_ && !parent_boundary_update_) {
                            //TODO release all the
                            barrier_token latest_token = obtained_barriers_.back();
                            obtained_barriers_.pop_back();
                            barrier_token latest_token_but_one = obtained_barriers_.back();
                            obtained_barriers_.pop_back();
                            release_all_barriers();
                            obtained_barriers_.push_back(latest_token_but_one);
                            obtained_barriers_.push_back(latest_token);
                        }
                        set_next_state(1);
                        transition_to_next_state();
//                        printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                        return run();
//                        continue;
                    }

                    case 1: {
                        assert(!last_state_1);
                        last_state_1 = true;
                        set_next_state(2);
                        tree_->blk_accessor_->asynch_read(node_ref_, buffer_, this);

//                        printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                        return CONTEXT_TRANSIT;
                    }
                    case 2: {
                        last_state_1 = false;
                        uint32_t node_type = *reinterpret_cast<uint32_t*>(buffer_);
                        switch (node_type) {
                            case LEAF_NODE: {
                                assert(current_node_level_ == 1);
                                assert(obtained_barriers_.back().type_ == WRITE_BARRIER);
                                current_node_ = new LeafNode<K, V, CAPACITY>(tree_->blk_accessor_, false);
                                current_node_->deserialize(buffer_);
                                if (optimistic_ && current_node_->size() == CAPACITY) {
                                    // leaf node is full, so the optimistic update fails.
                                    delete current_node_;
                                    current_node_ = 0;
                                    set_next_state(13);
                                    transition_to_next_state();
//                                    printf("during is %.2f ns, state: %d [2L1]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return run();
//                                    continue;
                                }
                                child_node_split_ = current_node_->insert_with_split_support(request_->key, request_->value, split_);
                                if (!child_node_split_) {
                                    // the leaf node does not split, so we only need to flush the leaf node
                                    current_node_->serialize(buffer_);
                                    delete current_node_;
                                    current_node_ = 0;
                                    write_back_completion_target_ = 1;
                                    write_back_completion_count_ = 0;
                                    set_next_state(10);
                                    tree_->blk_accessor_->asynch_write(node_ref_, buffer_, this);
//                                    printf("during is %.2f ns, state: %d [2L2]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return CONTEXT_TRANSIT;
                                } else {
                                    // the leaf node wat split and we need to flush both the leaf node and the new node.
                                    current_node_->serialize(buffer_);
                                    split_.right->serialize(buffer_2);
                                    if (current_node_->get_self_rep() ==
                                        tree_->root_->get_unified_representation()) {
                                        set_next_state(12);
                                    } else {
                                        set_next_state(10);
                                    }
                                    write_back_completion_target_ = 2;
                                    write_back_completion_count_ = 0;
                                    tree_->blk_accessor_->asynch_write(node_ref_, buffer_, this);
                                    tree_->blk_accessor_->asynch_write(split_.right->get_self_rep(), buffer_2, this);
//                                    printf("during is %.2f ns, state: %d [2L3]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return CONTEXT_TRANSIT;
                                }
                            }
                            case INNER_NODE: {
                                current_node_ = new InnerNode<K, V, CAPACITY>(tree_->blk_accessor_, false);
                                current_node_->deserialize(buffer_);
                                InnerNode<K, V, CAPACITY>* inner_node = reinterpret_cast<InnerNode<K, V, CAPACITY>*>(current_node_);
                                int target_node_index = inner_node->locate_child_index(request_->key);
                                const bool exceed_left_boundary = target_node_index < 0;
                                if (exceed_left_boundary && optimistic_) {
                                    // the insertion needs to update the inner node, so the optimistic insertion fails.
                                    delete current_node_;
                                    current_node_ = 0;
                                    set_next_state(13);
                                    transition_to_next_state();
//                                    printf("during is %.2f ns, state: %d [2I1]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return run();
//                                    continue;
                                }
                                target_node_index = target_node_index < 0 ? 0 : target_node_index;
                                if (exceed_left_boundary) {
                                    inner_node->key_[0] = request_->key;
                                    inner_node->mark_modified();
                                }
                                store_parent_node(inner_node, target_node_index);
                                current_node_ = nullptr;
                                node_ref_ = inner_node->child_rep_[target_node_index];
                                free_slot_available_in_parent_ = inner_node->has_free_slot();
                                parent_boundary_update_ = exceed_left_boundary;
                                next_visit_is_leaf_node_ = current_node_level_ == 2;
                                current_node_level_ --;
                                set_next_state(0);
                                transition_to_next_state();
//                                printf("during is %.2f ns, state: %d [2I2]\n", cycles_to_nanoseconds(ticks() - last), current);
                                return run();
//                                continue;
                            }
                        }
                    }
                    case 9: {
                        last_state_1 = false;
                        // The insertion process goes into this state, when the tuple has been inserted into the
                        // child node. Depending on whether the child node was split during the insertion, the
                        // process logic varies. If the child node was split, we need to accommodate the new node;
                        // Otherwise, we only need to close the current node as well as the parent node(s).

                        if (child_node_split_) {
                            // child node was split

                            if (pending_parent_nodes_.empty()) {
                                // the root node was split.

                            } else {

                                parent_node_context parent_context = pending_parent_nodes_.back();
                                pending_parent_nodes_.pop_back();
                                InnerNode<K, V, CAPACITY> *parent_node = parent_context.node;
                                current_node_ = parent_node;
                                node_ref_ = current_node_->get_self_rep();

                                if (current_node_->size() < CAPACITY) {
                                    // the current node has free slot for the new node.
                                    parent_node->insert_inner_node(split_.right, split_.boundary_key,
                                                                   parent_context.position + 1);
                                    child_node_split_ = false;

                                    current_node_->serialize(buffer_);

                                    write_back_completion_count_ = 0;
                                    write_back_completion_target_ = 1;

                                    set_next_state(10);
                                    tree_->blk_accessor_->asynch_write(
                                            current_node_->get_self_rep(), buffer_, this);
                                    delete split_.left;
                                    delete split_.right;
                                    delete parent_node;
//                                    printf("during is %.2f ns, state: %d [9*1]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return CONTEXT_TRANSIT;
                                } else {
                                    // the current node need to split to accommodate the new node.
                                    bool insert_to_first_half = parent_context.position < CAPACITY / 2;

                                    //
                                    int start_index_for_right = CAPACITY / 2;
                                    InnerNode<K, V, CAPACITY> *left = reinterpret_cast<InnerNode<K, V, CAPACITY>*>(current_node_);
                                    InnerNode<K, V, CAPACITY> *right = new InnerNode<K, V, CAPACITY>(
                                            tree_->blk_accessor_);
                                    right->mark_modified();

                                    // move the keys and children to the right node
                                    for (int i = start_index_for_right, j = 0; i < left->size_; ++i, ++j) {
                                        right->key_[j] = left->key_[i];
                                        right->child_rep_[j] = left->child_rep_[i];
                                        right->child_[j] = left->child_[i];
                                    }

                                    const int moved = left->size_ - start_index_for_right;
                                    left->size_ -= moved;
                                    right->size_ = moved;
                                    left->mark_modified();

                                    // insert the new child node to the appropriate split node.
                                    InnerNode<K, V, CAPACITY> *host_for_node = insert_to_first_half ? left : right;
                                    int inner_node_insert_position = host_for_node->locate_child_index(
                                            split_.boundary_key);
                                    host_for_node->insert_inner_node(split_.right, split_.boundary_key,
                                                                     inner_node_insert_position + 1);
                                    delete split_.left;
                                    delete split_.right;

                                    // write the remaining content in the split data structure.
                                    split_.left = (left);
                                    split_.right = (right);
                                    split_.boundary_key = right->key_[0];

                                    child_node_split_ = true;

                                    left->serialize(buffer_);
                                    right->serialize(buffer_2);

                                    write_back_completion_count_ = 0;

                                    write_back_completion_target_ = 2;
                                    if (tree_->root_->get_unified_representation() !=
                                        left->get_self_rep())
                                        set_next_state(10);
                                    else
                                        set_next_state(12);
                                    tree_->blk_accessor_->asynch_write(
                                            left->get_self_rep(), buffer_, this);
                                    tree_->blk_accessor_->asynch_write(
                                            right->get_self_rep(), buffer_2, this);
//                                    printf("during is %.2f ns, state: %d [9*2]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return CONTEXT_TRANSIT;
                                }
                            }

                        } else {
                            while (!pending_parent_nodes_.empty()) {
                                parent_node_context parent_context = pending_parent_nodes_.back();
                                pending_parent_nodes_.pop_back();
                                InnerNode<K, V, CAPACITY>* parent_node = parent_context.node;
                                if (parent_node->is_modified()) {
                                    parent_node->serialize(buffer_);
                                    write_back_completion_target_ = 1;
                                    write_back_completion_count_ = 0;
                                    set_next_state(10);
                                    tree_->blk_accessor_->asynch_write(parent_node->get_self_rep(), buffer_, this);
                                    delete parent_node;
//                                    printf("during is %.2f ns, state: %d [9*3]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return CONTEXT_TRANSIT;
                                } else {
                                    delete parent_node;
                                }
                            }
                            set_next_state(11);
                            transition_to_next_state();
//                            printf("during is %.2f ns, state: %d [9*4]\n", cycles_to_nanoseconds(ticks() - last), current);
                            return run();
//                            continue;
                        }
                    }
                    case 10: {
                        last_state_1 = false;
                        write_back_completion_count_ ++;
                        if (write_back_completion_count_ == write_back_completion_target_) {
                            set_next_state(9);
                            transition_to_next_state();
//                            printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                            return run();
//                            continue;
                        }
//                        printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                        return CONTEXT_TRANSIT;
                    }
                    case 11: {
                        last_state_1 = false;
                        release_all_barriers(); // TODO: this can be done earlier.
                        if (request_->cb_f) {
                            (*request_->cb_f)(request_->args);
                        }
                        tree_->pending_request_--;
                        tree_->free_context_slots_++;

                        bool ownership = request_->ownership;
                        request_->graduation = ticks();
                        if (request_->semaphore)
                            request_->semaphore->release();
                        if (ownership) {
                            delete request_;
                            request_ = 0;
                        }
//                        printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                        tree_->return_insert_context(this);
                        return CONTEXT_TERMINATED;
                    }
                    case 12: {
                        last_state_1 = false;
                        // handle root node split
                        write_back_completion_count_ ++;
                        if (write_back_completion_count_ == write_back_completion_target_) {
                            InnerNode<K, V, CAPACITY> *new_inner_node = new InnerNode<K, V, CAPACITY>(split_.left, split_.right, tree_->blk_accessor_);
                            new_inner_node->mark_modified();
                            tree_->root_->copy(new_inner_node->get_self_ref());// the root_ reference which originally referred to a
                            // a leaf will refer to a inner node now. TODO: release the old root_
                            // reference and create a new one.
//                            tree_->root_->bind(new_inner_node);
                            tree_->depth_++;
                            new_inner_node->serialize(buffer_);
                            write_back_completion_count_ = 0;
                            write_back_completion_target_ = 1;
                            child_node_split_ = false;
                            set_next_state(11);
                            tree_->blk_accessor_->asynch_write(new_inner_node->get_self_rep(), buffer_, this);
//                            printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                            return CONTEXT_TRANSIT;
                        }
                        return CONTEXT_TRANSIT;
                    }

                    case 13: {
                        last_state_1 = false;
                        optimistic_ = false;
                        free_slot_available_in_parent_ = false;
                        refer_to_root_ = false;
                        next_visit_is_leaf_node_ = tree_->depth_ == 1;
                        current_node_level_ = tree_->get_height();
                        release_all_barriers();
                        set_next_state(0);
                        transition_to_next_state();
                        node_ref_ = -1;
//                        printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                        return run();
//                        continue;
                    }

                }
                assert(false);
                return CONTEXT_TERMINATED;
            }

        private:
            void store_parent_node(InnerNode<K, V, CAPACITY>* node, int position) {
                pending_parent_nodes_.push_back(parent_node_context(node, position));
            }

            void release_all_barriers() {
                uint64_t barrier_start = ticks();
                while (!obtained_barriers_.empty()) {
                    barrier_token token = obtained_barriers_.back();
                    obtained_barriers_.pop_back();
                    if (token.type_ == READ_BARRIER) {
                        tree_->manager.remove_read_barrier(token.barrier_id_);
                    } else {
                        tree_->manager.remove_write_barrier(token.barrier_id_);
                    }
                }
                tree_->barrier_handling_cycles_ += ticks() - barrier_start;
//                for (auto it = obtained_barriers_.cbegin(); it != obtained_barriers_.cend(); ++it) {
//                    if (it->type_ == READ_BARRIER)
//                        tree_->manager.remove_read_barrier(it->barrier_id_);
//                    else
//                        tree_->manager.remove_write_barrier(it->barrier_id_);
//                }
//                obtained_barriers_.clear();
            }

            struct parent_node_context {
                parent_node_context(InnerNode<K, V, CAPACITY>* n, int p) {
                    node = n;
                    position = p;
                }
                InnerNode<K, V, CAPACITY>* node;
                int position;
            };
        private:
            pull_based_b_plus_tree *tree_;
            blk_address node_ref_;
            void *buffer_, *buffer_2;
            Node<K, V>* current_node_;
            insert_request<K, V>* request_;
            std::deque<parent_node_context> pending_parent_nodes_;

            int write_back_completion_count_;
            int write_back_completion_target_;
            bool child_node_split_;
            Split<K, V> split_;

            bool free_slot_available_in_parent_;
            bool parent_boundary_update_;
            bool refer_to_root_;

            bool optimistic_;
            bool next_visit_is_leaf_node_;
            int current_node_level_;

            bool last_state_1;
        };


        class search_context : public call_back_context {
        public:

            uint64_t last;
            search_context(pull_based_b_plus_tree *tree, search_request<K, V>* request) : call_back_context(),
                                                                                          tree_(tree) {
                buffer_ = tree_->blk_accessor_->malloc_buffer();
                inner_node_ = new InnerNode<K, V, CAPACITY>(tree_->blk_accessor_, false);
                leaf_node_ = new LeafNode<K, V, CAPACITY>(tree_->blk_accessor_, false);
            };
            ~search_context() {
                tree_->blk_accessor_->free_buffer(buffer_);
                delete inner_node_;
                delete leaf_node_;
                buffer_ = 0;
            }

            void init(search_request<K, V> * request) {
                request_ = request;
                node_ref_ = tree_->root_->get_unified_representation();
                last = ticks();
                this->reset_state();
            }

            int run() {
//                if (duration > 10000) {
//                }
                last = ticks();
                int current = this->current_state;

//                printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
//                while(true)
                switch (this->current_state) {
                    case 0: {
                        set_next_state(1);
                        uint64_t barrier_start = ticks();
                        if (tree_->manager.request_read_barrier(node_ref_, this)) {
                            tree_->barrier_handling_cycles_ += ticks() - barrier_start;
                            transition_to_next_state();
//                            printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                            return run();
//                            continue;
                        } else {
                            tree_->barrier_handling_cycles_ += ticks() - barrier_start;
//                            printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                            return CONTEXT_TRANSIT;
                        }
                    }
                    case 1: {
                        assert(this->obtained_barriers_.size() <= 2);
                        if (this->obtained_barriers_.size() == 2) {
                            uint64_t barrier_start = ticks();
                            auto front = this->obtained_barriers_.front();
                            tree_->manager.remove_read_barrier(front.barrier_id_);
                            this->obtained_barriers_.pop_front();
                            tree_->barrier_handling_cycles_ += ticks() - barrier_start;
                        }
                        set_next_state(2);
                        tree_->blk_accessor_->asynch_read(node_ref_, buffer_, this);
//                        printf("during is %.2f ns, state: %d\n", cycles_to_nanoseconds(ticks() - last), current);
                        return CONTEXT_TRANSIT;
                    }
                    case 2: {
                        uint32_t node_type = *reinterpret_cast<uint32_t *>(buffer_);
                        switch (node_type) {
                            case LEAF_NODE: {
//                                int64_t realwork_start = ticks();
                                current_node_ = leaf_node_;
                                current_node_->deserialize(buffer_);
                                *request_->found = current_node_->search(request_->key, *request_->value);
//                                delete current_node_;
                                current_node_ = 0;
//                                int64_t realwork_end = ticks();
//                                int64_t free_work_start = realwork_end;
                                if (request_->cb_f) {
                                    (*request_->cb_f)(request_->args);
                                }
                                tree_->pending_request_--;
                                tree_->free_context_slots_++;
                                uint64_t barrier_start = ticks();
                                for (auto it = this->obtained_barriers_.begin(); it != obtained_barriers_.end(); ++it) {
                                    tree_->manager.remove_read_barrier((*it).barrier_id_);
                                }
                                obtained_barriers_.clear();
                                tree_->barrier_handling_cycles_ += ticks() - barrier_start;
//                                int64_t free_work1_start = realwork_end;
                                request_->graduation = ticks();
                                bool ownership = request_->ownership;
                                if (request_->semaphore)
                                    request_->semaphore->release();
//                                int64_t free_work1_end = ticks();
                                if (ownership) {
                                    delete request_;
                                    request_ = 0;
                                }
//                                else {
//                                    request_->semaphore->post();
//                                }
//                                int64_t free_work_end = ticks();
//                                int64_t barrier_work_start = free_work_end;
//                                int64_t barrier_work_end = ticks();
//                                printf("during is %.2f ns, state: %d [L] read work: %.2f ns, free work: %.2f, barrier work: %.2f, free work 1: %.2f\n",
//                                       cycles_to_nanoseconds(ticks() - last),
//                                       current,
//                                       cycles_to_nanoseconds(realwork_end - realwork_start),
//                                       cycles_to_nanoseconds(free_work_end - free_work_start),
//                                       cycles_to_nanoseconds(barrier_work_end - barrier_work_start),
//                                       cycles_to_nanoseconds(free_work1_end - free_work1_start));
//                                printf("during is %.2f ns, state: %d [L]\n", cycles_to_nanoseconds(ticks() - last), current);
                                tree_->return_search_context(this);
                                return CONTEXT_TERMINATED;
                            }
                            case INNER_NODE: {
                                current_node_ = inner_node_;
                                current_node_->deserialize(buffer_);
                                int child_index = reinterpret_cast<InnerNode<K, V, CAPACITY> *>(current_node_)->locate_child_index(
                                        request_->key);
                                if (child_index < 0) {
                                    *request_->found = false;
//                                    delete current_node_;
                                    current_node_ = 0;
                                    if (request_->cb_f) {
                                        (*request_->cb_f)(request_->args);
                                    }
                                    bool ownership = request_->ownership;
                                    tree_->pending_request_--;
                                    tree_->free_context_slots_++;
                                    uint64_t barrier_start = ticks();
                                    for (auto it = this->obtained_barriers_.begin();
                                         it != obtained_barriers_.end(); ++it) {
                                        tree_->manager.remove_read_barrier((*it).barrier_id_);
                                    }
                                    obtained_barriers_.clear();
                                    tree_->barrier_handling_cycles_ += ticks() - barrier_start;
                                    request_->graduation = ticks();
                                    if (ownership) {
                                        if (request_->semaphore)
                                            request_->semaphore->release();
                                        delete request_;
                                        request_ = 0;
                                    } else {
                                        if (request_->semaphore)
                                            request_->semaphore->release();
                                    }
//                                    printf("during is %.2f ns, state: %d [I1]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    tree_->return_search_context(this);
                                    return CONTEXT_TERMINATED;
                                } else {
                                    node_ref_ = reinterpret_cast<InnerNode<K, V, CAPACITY> *>(current_node_)->child_rep_[child_index];
//                                    delete current_node_;
                                    current_node_ = 0;
                                    set_next_state(0);
                                    transition_to_next_state();
//                                    printf("during is %.2f ns, state: %d [I2]\n", cycles_to_nanoseconds(ticks() - last), current);
                                    return run();
//                                    continue;
                                }
                            }
                        }
                    }

                }
                assert(false);
                return CONTEXT_TERMINATED;
            }

        private:
            pull_based_b_plus_tree *tree_;
            blk_address node_ref_;
            void *buffer_;
            Node<K, V>* current_node_;
            search_request<K, V>* request_;

            LeafNode<K, V, CAPACITY> *leaf_node_; // for reuse among different operations.
            InnerNode<K, V, CAPACITY> *inner_node_; // for reuse among different operations.
        };

        void create_free_contexts() {
            for (int i = 0; i < queue_length_; i++) {
                free_insert_contexts_.push(new insert_context(this, nullptr));
                free_search_contexts_.push(new search_context(this, nullptr));
            }
        }

        void destroy_free_contexts() {
            for (int i = 0; i < queue_length_; i++) {
                delete free_search_contexts_.top();
                free_search_contexts_.pop();
                delete free_insert_contexts_.top();
                free_insert_contexts_.pop();
            }
        }

        search_context *get_free_search_context() {
            assert(!free_search_contexts_.empty());
            search_context* context = free_search_contexts_.top();
            free_search_contexts_.pop();
            return context;
        }

        void return_search_context(search_context* context) {
            free_search_contexts_.push(context);
        }

        void return_insert_context(insert_context* context) {
            free_insert_contexts_.push(context);
        }


        insert_context *get_free_insert_context() {
            assert(!free_insert_contexts_.empty());
            insert_context* context = free_insert_contexts_.top();
            free_insert_contexts_.pop();
            return context;
        }

    public:
        blk_accessor<K, V>* get_blk_accessor() {
            return this->blk_accessor_;
        };

        std::deque<call_back_context*> &get_barrier_ready_contexts_ref() {
            return manager.get_ready_contexts();
        }

    private:
        SpinLock lock_;
        Semaphore queue_free_slots_;
        boost::lockfree::queue<request<K, V>*, boost::lockfree::capacity<256>> request_queue_;

        atomic<int> request_queue_size_;
        pthread_t thread_handle_;
        volatile bool working_thread_terminate_flag_;
        atomic<int> free_context_slots_;
        atomic<int> pending_request_;
        int queue_length_;
        barrier_manager manager;

        std::stack<insert_context*> free_insert_contexts_;
        std::stack<search_context*> free_search_contexts_;

        uint64_t barrier_handling_cycles_;

        friend class scheduler;

        class scheduler {
        public:
            explicit scheduler(pull_based_b_plus_tree* tree):tree_(tree){
                context_id_generator_ = 0;
                blk_ready_contexts_ = &(tree_->blk_accessor_->get_ready_contexts());
                barrier_ready_contexts_ = &(tree_->manager.get_ready_contexts());
            };

            virtual void run() = 0;

        protected:
            int admission(int max = INT_MAX) {
                int free = tree_->free_context_slots_.load();
                int processed = 0;
                request<K, V>* request;
                while(processed < max && free-- && (request = tree_->atomic_dequeue_request())) {
                    request->admission = ticks();
                    request->start = ticks();
                    call_back_context *context;
                    if (request->type == SEARCH_REQUEST) {
                        context = tree_->get_free_search_context();
                        reinterpret_cast<search_context *>(context)->init(
                                reinterpret_cast<search_request<K, V> *>(request));
                    } else {
                        context = tree_->get_free_insert_context();
                        reinterpret_cast<insert_context *>(context)->init(
                                reinterpret_cast<insert_request<K, V> *>(request));
                    }
                    context->set_id(context_id_generator_++);
                    tree_->free_context_slots_--;
                    processed++;
                    admission_contexts_.push_back(context);
                }
                return processed;
            }

            int process_blk_ready_context(int max = INT_MAX, bool sort = false) {
                return process_ready_contexts(*blk_ready_contexts_, max, sort);
            }

            int process_barrier_ready_context(int max = INT_MAX, bool sort = false) {
                return process_ready_contexts(*barrier_ready_contexts_, max, sort);
            }

            int process_new_context(int max = INT_MAX, bool sort = false) {
                return process_ready_contexts(admission_contexts_, max, false);
            }

            int probe_blk_completion(int max = 0) {
                return tree_->blk_accessor_->process_completion(max);
            }

            int get_number_of_ready_context() const {
                return barrier_ready_contexts_->size() + blk_ready_contexts_->size();
            }

            int get_and_reset_recent_write() {
                return tree_->blk_accessor_->get_and_reset_recent_writes();
            }

            int get_and_reset_recent_read() {
                return tree_->blk_accessor_->get_and_reset_recent_reads();
            }

            bool terminated() {
                return tree_->working_thread_terminate_flag_ && tree_->pending_request_.load() <= 0;
            }

        private:

            int process_ready_contexts(std::vector<call_back_context*>& ready_contexts, int max = 1, bool sort = false) {
                if (ready_contexts.empty())
                    return 0;
                if (sort) {
                    std::sort(ready_contexts.begin(), ready_contexts.end(), compare);
                }
                int processed = 0;

                std::vector<call_back_context*> to_be_processed;
                to_be_processed.reserve(max < ready_contexts.size() ? max : ready_contexts.size());
                auto it = ready_contexts.begin();
                for ( ; processed < max && it != ready_contexts.end(); ++it) {
                    to_be_processed.push_back(*it);
                }
                ready_contexts.erase(ready_contexts.begin(), it);

                for (auto it = to_be_processed.cbegin(); it != to_be_processed.cend(); ++it) {
                    (*it)->run();
                    processed++;
                }

                return processed;
            }

        protected:
            pull_based_b_plus_tree* tree_;
        private:
            uint64_t context_id_generator_;
            vector<call_back_context*> admission_contexts_;
            vector<call_back_context*> *blk_ready_contexts_;
            vector<call_back_context*> *barrier_ready_contexts_;
        };


        class naive_scheduler: public scheduler {
        public:
            naive_scheduler(pull_based_b_plus_tree* tree):scheduler(tree){};
            void run() {
                uint64_t last_failed_probe_time = 0;
                while(!this->terminated()) {
                    int processed;
                    const int process_granularity = 16;
                    const int sort = true;
                    processed = this->admission(process_granularity);
//                    printf("admission: %d\n", processed);
                    processed = this->process_new_context(process_granularity, sort);
//                    printf("new: %d\n", processed);
                    processed = this->process_blk_ready_context(process_granularity, sort);
//                    printf("blk: %d\n", processed);
//                    if (ticks() - last_failed_probe_time > 5000) {
                        processed = this->probe_blk_completion();
                        if (processed == 0) {
                            last_failed_probe_time = ticks();
                        }
//                    }
//                    printf("barrier: %d\n", processed);
                    processed = this->process_barrier_ready_context(process_granularity, sort);
//                    printf("completion: %d\n", processed);
                }
            }
        };

        class latency_aware_scheduler : public scheduler {
        public:
            latency_aware_scheduler(pull_based_b_plus_tree *tree) : scheduler(tree) {
                estimator = &(tree->blk_accessor_->get_ready_state_estimator());
                last_probe_tick = 0;
            };

            void run() {

                uint64_t probed = 0;
                double error = 0;
                double estimator_count = 0;
                uint64_t completions = 0;
                uint64_t timeouts = 0;
                uint64_t cpu_yields = 0;

                uint64_t barrier_handling_cycles = 0;
                uint64_t context_processing_cycles = 0;
                uint64_t blk_handling_cycles = 0;
                uint64_t scheduling_cycles = 0;

                uint64_t total_start = ticks();
                uint64_t sleep_cycles = 0;


                while (!this->terminated()) {
                    uint64_t start_processing = ticks();
                    uint64_t start = ticks();
                    int processed = 0;
                    processed += this->admission(process_granularity);
                    processed += this->process_new_context(process_granularity, sort);
                    processed += this->process_blk_ready_context(process_granularity, sort);
                    processed += this->process_barrier_ready_context(process_granularity, sort);
                    uint64_t now = ticks();
                    context_processing_cycles += now - start;
                    uint64_t scheduling_start = now;
                    int estimated_reads = estimator->estimate_number_of_ready_reads(now);
                    int estimated_writes = estimator->estimate_number_of_ready_writes(now);
                    int estimated = 0;
                    if (now - last_probe_tick >= min_probe_delay_cycles &&  // avoiding probing too frequently
                            (now - last_probe_tick >= max_probe_delay_cycles // avoiding probing too infrequently
                                || (estimated = estimated_reads + estimated_writes) >= process_granularity // probing when there might be some completed I/Os.
                            )) {
                        now = ticks();
                        scheduling_cycles += now - scheduling_start;
                        if (estimated == 0) {
                            timeouts++;
                        }
                        uint64_t blk_start_cycles = ticks();
                        const int count = this->probe_blk_completion(probe_granularity);
                        blk_handling_cycles += ticks() - blk_start_cycles;
                        last_probe_tick = ticks();

                        if (estimated > 0) {
                            estimator_count++;
                            error += abs((estimated - count) * (estimated - count));
                        }

                        probed++;
                        completions += count;
                    } else if (this->get_number_of_ready_context() == 0 && estimator->estimate_number_of_ready_reads(now + 100000) == 0 &&
                            estimator->estimate_number_of_ready_writes(now + 100000) == 0) {
                        now = ticks();
                        uint64_t sleep_start = now;
                        scheduling_cycles += now - scheduling_start;
                        usleep(cycles_to_microseconds(100000));
//                        printf("sleep!\n");
//                        pthread_yield();
                        cpu_yields++;
                        sleep_cycles += ticks() - sleep_start;
                    }
                }
//                uint64_t total_cycles = blk_handling_cycles + scheduling_cycles + context_processing_cycles;
                uint64_t total_cycles = ticks() - total_start - sleep_cycles;
                uint64_t barrier_handing_cycles = this->tree_->barrier_handling_cycles_;

                printf("[Working threads] probed: %.4f Million, estimation stand err: %.2f \n",
                       probed / 1000.0 / 1000.0,
                       estimator_count == 0 ? 0 : sqrt(error / estimator_count));

                printf("completions: %d, timeouts: %d\n", completions, timeouts);

                printf("cpu yield: %d, time: %.2f\n", cpu_yields, cycles_to_seconds(10000 * cpu_yields));



                printf("blk handling: %.5f%, scheduling: %.5f%, context processing: %.5f%, barrier handling: %.2f%\n", blk_handling_cycles / (double)total_cycles * 100,
                scheduling_cycles / (double)total_cycles * 100, (context_processing_cycles - barrier_handing_cycles) / (double)total_cycles * 100,
                       barrier_handing_cycles / (double)total_cycles * 100);

            }

        private:
            ready_state_estimator *estimator;
            uint64_t last_probe_tick;
            const int64_t max_probe_delay_cycles = 500000;
            const int64_t min_probe_delay_cycles = 50000;
            const int64_t probe_granularity = 128;
            const int process_granularity = 16;
            const bool sort = true;
        };

    };
}

#endif //NVM_PULL_BASED_B_PLUS_TREE_H
