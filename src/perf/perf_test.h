//
// Created by robert on 7/6/17.
//

#include <set>
#include <unistd.h>
#include <string>
#include <algorithm>
#include <thread>
#include <unordered_map>
#include "../tree/vanilla_b_plus_tree.h"
#include "../utils/generator.h"
#include "../utils/rdtsc.h"
#include "../utils/cpu_usage_monitor.h"
#include "tree_operation_executor.h"
#include "insert.h"
#include "update.h"
#include "operation.h"
#include "ThreadPool.h"

using namespace std;
template <typename K, typename V>
void benchmark(BTree<K, V> *tree, const string name, const int runs, const int ntuples, const int reads,
               const int nupdates, double skewness) {
    uint64_t total_start = ticks();
    double build_time = 0, search_time = 0, update_time = 0;
    int run = runs;
    int founds = 0;
    int errors = 0;
    uint64_t search_cycles = 0;
    ZipfGenerator generator(ntuples, skewness);

//    int *tuples = new int[ntuples];

    vector<pair<K, V>> tuples;
    vector<pair<K, V>> updates;

    for (int i = 0; i < ntuples; ++i) {
        tuples.push_back(make_pair(i, i));
    }
    random_shuffle(tuples.begin(), tuples.end());

    for (int i = 0; i < nupdates; ++i) {
        const int key = generator.gen();
        updates.push_back(make_pair(key, key));
    }

    int *search_keys = new int[reads];
    for (int i = 0; i < reads; ++i) {
        search_keys[i] = generator.gen();
    }

    while (run--) {
//        if (run != runs)
        tree->clear();
        std::set<int> s;

        uint64_t begin = ticks();
        insert<K, V>(tree, tuples, 1);
        uint64_t end = ticks();
        double elapsed_secs = cycles_to_seconds(end - begin);
        build_time += elapsed_secs;

        printf("inserted...\n");
//        sleep(1);

        cpu_usage_monitor monitor;
        monitor.init();
        begin = ticks();
        update<K, V>(tree, updates, 1);
        end = ticks();
        printf("CPU usage: %f\n", monitor.get_value());
        update_time += cycles_to_seconds(end - begin);;
        printf("updated...\n");
//        sleep(1);

        begin = ticks();
        for (int i = 0; i < reads; ++i) {
            int value = -1;
            const K key = search_keys[i];
            const bool is_found = tree->search(key, value);
            founds += is_found;
            // avoid the search operator to be wept out by the compile optimizer.
//            if (is_found && value != key) {
//                std::cout << std::endl;
//            }
        }
        tree->sync();
        end = ticks();
        search_time += cycles_to_seconds(end - begin);;
        printf("searched...\n");

    }
    delete[] search_keys;


    cout << "[" << name.c_str() << "]: " << "#. of runs: " << runs << ", #. of tuples: " << ntuples
         << " reads: " << reads * runs <<" found: " << founds
         << ", Insert: " << ntuples * runs / build_time / 1000 << " K tuples / s"
         << ", Update: " << nupdates * runs / update_time / 1000 << " K tuples / s"
         << ", Search: " << reads * runs / search_time / 1000 << " K tuples / s"
         << ", found: " << founds
         << endl;

    uint64_t total_end = ticks();
}



template <typename K, typename V>
void benchmark_mixed_workload(BTree<K, V> *tree, const string name, const int runs, const int ntuples, const int noperations,
               const double write_rate, double skewness) {
    uint64_t total_start = ticks();
    double build_time = 0, search_time = 0, update_time = 0;
    int run = runs;
    int founds = 0;
    int errors = 0;
    uint64_t search_cycles = 0;
    ZipfGenerator generator(ntuples, skewness);

//    int *tuples = new int[ntuples];

    vector<pair<K, V>> tuples;
    vector<operation<K, V>> operations;

    for (int i = 0; i < ntuples; ++i) {
        tuples.push_back(make_pair(i, i));
    }
    random_shuffle(tuples.begin(), tuples.end());

    for (int i = 0; i < noperations; ++i) {
        const int key = generator.gen();
        operation<K, V> op;
        op.key = key;
        op.val = key;
        if (rand() / (double)RAND_MAX < write_rate) {
            op.type = WRITE_OP;
        } else {
            op.type = READ_OP;
        }
        operations.push_back(op);
    }

    printf("begin to run benchmark\n");
    while (run--) {
//        if (run != runs)
        tree->clear();
        std::set<int> s;

        uint64_t begin = ticks();
        insert<K, V>(tree, tuples, 1);
        uint64_t end = ticks();
        double elapsed_secs = cycles_to_seconds(end - begin);
        build_time += elapsed_secs;

        printf("inserted...\n");

//        sleep(1);

        begin = ticks();
//        for (int i = 0; i < reads; ++i) {
//            int value;
//            const K key = search_keys[i];
//            const bool is_found = tree->search(key, value);
//            founds += is_found;
//            // avoid the search operator to be wept out by the compile optimizer.
////            if (is_found && value != key) {
////                std::cout << std::endl;
////            }
//        }
        for (int i = 0; i < noperations; ++i) {
            operation<K,V> op = operations[i];
            if (op.type == WRITE_OP) {
                tree->insert(op.key, op.val);
            } else {
                tree->search(op.key, op.val);
            }
        }
        tree->sync();
        end = ticks();
        search_time += cycles_to_seconds(end - begin);;
        printf("searched...\n");

    }

    cout << ntuples << " tuples." << endl;

    cout << "[" << name.c_str() << "]: " << "#. of runs: " << runs << ", #. of tuples: " << ntuples
         << ", Insert: " << ntuples * runs / build_time / 1000 << " K tuples / s"
         << ", Mix(" << write_rate * 100 <<"% write): " << noperations * runs / search_time / 1000 << " K tuples / s"
         << endl;

    uint64_t total_end = ticks();
    cout << "total execution time: " << cycles_to_seconds(total_end - total_start) << endl;
}

template <typename K, typename V>
void multithread_benchmark_mixed_workload(BTree<K, V> *tree, const string name, const int runs, const int ntuples, const int noperations, const double write_rate, double skewness, int threads, int use_thread_pool) {
    uint64_t total_start = ticks();
    double build_time = 0, search_time = 0, update_time = 0;
    int run = runs;
    int founds = 0;
    int errors = 0;
    uint64_t search_cycles = 0;
    std::thread* tid = new std::thread[threads];
    ZipfGenerator generator(ntuples, skewness);

//    int *tuples = new int[ntuples];

    vector<pair<K, V>> tuples;
    vector<operation<K, V>> operations;

    for (int i = 0; i < ntuples; ++i) {
        tuples.push_back(make_pair(i, i));
    }
    random_shuffle(tuples.begin(), tuples.end());

    for (int i = 0; i < noperations; ++i) {
        const int key = generator.gen();
        operation<K, V> op;
        op.key = key;
        if (rand() / (double)RAND_MAX < write_rate) {
            op.type = WRITE_OP;
            op.val = key;
        } else {
            op.type = READ_OP;
            op.val = -1;
        }
        operations.push_back(op);
    }

    blk_metrics total_blk_metrics;

    while (run--) {
//        if (run != runs)
//        tree->clear();
        std::set<int> s;
        blk_accessor<K, V>* accessor = tree->get_accessor();

        uint64_t begin = ticks();
        const int number_of_insertion_threads = threads;
        std::thread insert_tid[number_of_insertion_threads];
        std::vector<operation<K,V>> insertion_ops;
        for (int i = 0; i < ntuples; i++) {
            operation<K, V> op;
            op.key = tuples[i].first;
            op.val = tuples[i].second;
            op.type = WRITE_OP;
            insertion_ops.push_back(op);
        }
        random_shuffle(insertion_ops.begin(), insertion_ops.end());
        const int inserts_per_thread = insertion_ops.size() / number_of_insertion_threads;
        for (int i = 0; i < number_of_insertion_threads; ++i) {
            insert_tid[i] = std::thread(&execute_operations<K, V>, tree,
                                        insertion_ops.begin() + inserts_per_thread * i,
            i == number_of_insertion_threads - 1 ? insertion_ops.end() : insertion_ops.begin() + inserts_per_thread * (i + 1));
        }
        for (int i = 0; i < number_of_insertion_threads; i++) {
            insert_tid[i].join();
        }
//        insert<K, V>(tree, tuples, 1);
        uint64_t end = ticks();
        double elapsed_secs = cycles_to_seconds(end - begin);
        build_time += elapsed_secs;
        tree->sync();
        printf("inserted...\n");
        printf("Tree hight: %d\n", tree->height());
        printf("%f seconds to insert!\n", cycles_to_seconds(ticks() - total_start));
        sleep(5);

        blk_metrics metrics;
        if(accessor)
            accessor->start_measurement();

        begin = ticks();
//        for (int i = 0; i < reads; ++i) {
//            int value;
//            const K key = search_keys[i];
//            const bool is_found = tree->search(key, value);
//            founds += is_found;
//            // avoid the search operator to be wept out by the compile optimizer.
////            if (is_found && value != key) {
////                std::cout << std::endl;
////            }
//        }


        cpu_usage_monitor monitor;
        monitor.init();

        tree->reset_metrics();

        int ops_per_thread = noperations / threads;
	ThreadPool* pool;
	if(use_thread_pool){
		pool=new ThreadPool(number_of_threads);
	}
        for (int i = 0; i < threads; i++) {
	    if(use_thread_pool){
		    pool->enqueue(&execute_operations<K, V>, tree, operations.begin() + i * ops_per_thread,i == threads - 1 ? operations.end() : operations.begin() + (1 + i) * ops_per_thread);
		    continue;
	    }
            tid[i] = std::thread(&execute_operations<K, V>, tree, operations.begin() + i * ops_per_thread,i == threads - 1 ? operations.end() : operations.begin() + (1 + i) * ops_per_thread);
        }

	if(use_thread_pool)
        	for (int i = 0; i < threads; i++) {
        	    tid[i].join();
        	}
        tree->sync();
        printf("CPU usage: %f\n", monitor.get_value());
        if(accessor) {
            metrics = accessor->end_and_get_measurement();
            total_blk_metrics.merge(metrics);
        }
        end = ticks();
        search_time += cycles_to_seconds(end - begin);
        printf("searched...\n");
        printf("time: %f\n", cycles_to_seconds(end - begin));

    }


    cout << "[" << name.c_str() << "(" << threads << ")" << "]: " << "#. of runs: " << runs << ", #. of tuples: " << ntuples
         << ", Insert: " << ntuples * runs / build_time / 1000 << " K tuples / s"
         << ", Mix(" << write_rate * 100 <<"% write): " << noperations * runs / search_time / 1000 << " K tuples / s"
         << endl;

    total_blk_metrics.print();

    tree->get_metrics().print();

    uint64_t total_end = ticks();
    delete[] tid;
}
