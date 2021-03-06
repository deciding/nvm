//
// Created by robert on 10/11/17.
//

#ifndef NVM_CALL_BACK_H
#define NVM_CALL_BACK_H
#include <stdio.h>
#include <assert.h>
#include <queue>
#include <vector>
#include <deque>
#include <string>
#include "../utils/sync.h"
//#include "context_barrier.h"

class barrier_token {
public:
    barrier_token(int64_t barrier_id, int32_t type): barrier_id_(barrier_id), type_(type) {
    }

public:
    int64_t barrier_id_;
    int32_t type_;
};

class context_barrier;
class call_back_context;

static std::queue<call_back_context*> submission_queue;

static std::queue<call_back_context*> ready_queue;

static volatile int pending_context = 0;

static SpinLock spin_lock;
static void process_logic(volatile bool*);
static void add_to_queue(call_back_context* context);

#define CONTEXT_TERMINATED 0
#define CONTEXT_TRANSIT 1

#define CONTEXT_WRITE_BARRIER 0
#define CONTEXT_READ_BARRIER 1
#define CONTEXT_WRITE_IO 0
#define CONTEXT_READ_IO 1

using namespace std;

class call_back_context {
public:
    call_back_context(): status(0), current_state(0), next_state(-1) {
//        record_transition(0);
        tag = INT32_MAX;
        id = UINT64_MAX;
    };

    virtual ~call_back_context(){};
    int current_state;
    int next_state;
    int status;
    uint64_t id;
    int tag;

    void set_id(const uint64_t &id) {
        this->id = id;
    }

    void set_tag(int tag) {
        this->tag = tag;
    }

    virtual int run() {
        if (status == 0) {
            add_to_queue(this);
            set_next_state(1);
            return CONTEXT_TRANSIT;
        } else {
            return CONTEXT_TERMINATED;
        }
    }

    void reset_state() {
        status = 0;
        current_state = 0;
        next_state = -1;
    }

    void transition_to_state(int status) {
        this->status = status;
    }

    void set_next_state(int state) {
        next_state = state;
    }

    void transition_to_next_state() {
        assert(next_state >= 0);
        current_state = next_state;
//        record_transition(current_state);
    }

    void add_barrier_token(const barrier_token &token) {
        obtained_barriers_.push_back(token);
    }

//    string get_transition_lineage() {
//        return debug_info.str();
//    }
//
//    void record_debug_info(const char* str) {
////        debug_info << str << ", ";
//    }

protected:
    std::deque<barrier_token> obtained_barriers_;
//    std::vector<string> state_history_;
//    ostringstream debug_info;

private:
//    void record_transition(int state) {
////        debug_info << " |" << state << ": ";
//    }
};

static void process_logic(volatile bool *terminate) {
    printf("thread starts!\n");
    while (!*terminate) {
        spin_lock.acquire();
        if (submission_queue.size() > 0) {
            call_back_context *context = submission_queue.front();
            submission_queue.pop();
            ready_queue.push(context);
            spin_lock.release();
        } else {
            spin_lock.release();
        }
    }
    printf("thread terminates!\n");
}

static void submit_context(call_back_context* context) {
    spin_lock.acquire();
    submission_queue.push(context);
    pending_context ++;
    spin_lock.release();
}

static void add_to_queue(call_back_context* context) {
    spin_lock.acquire();
    submission_queue.push(context);
    spin_lock.release();
}

static int process_completion(int max = 1) {
    int processed = 0;
    for(int i = 0; i < max; i++) {
        spin_lock.acquire();
        if (ready_queue.size() > 0) {
            call_back_context* context = ready_queue.front();
            ready_queue.pop();
            spin_lock.release();
            const int status = context->run();
            if (status == CONTEXT_TERMINATED) {
                spin_lock.acquire();
                pending_context--;
                spin_lock.release();
                printf("terminated: %d -> %d\n", pending_context + 1, pending_context);
            }
            processed++;
        } else {
            spin_lock.release();
        }
    }
    return processed;
}

static int pending_context_size() {
    spin_lock.acquire();
    int ret = pending_context;
    spin_lock.release();
    return ret;
}

#endif //NVM_CALL_BACK_H
