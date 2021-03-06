//
// Created by robert on 1/12/17.
//

#ifndef NVM_CONCURRENT_B_PLUS_TREE_H
#define NVM_CONCURRENT_B_PLUS_TREE_H

#include <deque>
#include <stack>
#include "inner_node.h"
#include "leaf_node.h"
#include "vanilla_b_plus_tree.h"
#include "../sync/lock_manager.h"
#include "../blk/file_blk_accessor.h"
#include "../utils/sync.h"

using namespace std;

namespace tree{
    template<typename K, typename V, int CAPACITY>
    class concurrent_b_plus_tree: public VanillaBPlusTree<K, V, CAPACITY> {
    public:
        concurrent_b_plus_tree(int32_t block_size = 512): block_size_(block_size) {
            this->set_blk_accessor();
        }

        virtual ~concurrent_b_plus_tree() {
        }

        void insert(const K &key, const V &value) {
            uint64_t start = ticks();
            concurrent_insert(key, value);
            this->metrics_.add_write_latency(ticks() - start);
        }

        bool search(const K &key, V &value) {
            uint64_t start = ticks();
            bool result =  concurrent_search(key, value);
            this->metrics_.add_read_latency(ticks() - start);
            return result;
        }

        virtual void clear() {
            VanillaBPlusTree<K, V, CAPACITY>::close();
            this->set_blk_accessor();
            VanillaBPlusTree<K, V, CAPACITY>::init();
        }
    private:

        bool concurrent_search(const K &key, V & value) {
            deque<lock_descriptor> obtained_locks;
            bool found = search_with_concurrency_control(key, value, obtained_locks, get_root_id(), true);
            assert(obtained_locks.size() == 0);
            return found;
        }

        bool search_with_concurrency_control(const K &key, V &value, deque<lock_descriptor> &obtained_locks, blk_address current_node_id, bool is_current_node_root = false) {
            bool is_found;
            lock_descriptor l = manager.get_read_lock(current_node_id);
            if (is_current_node_root && get_root_id() != current_node_id) {
                // root node has been split
                manager.release_lock(l);
                return search_with_concurrency_control(key, value, obtained_locks, get_root_id(), true);
            }

            if (obtained_locks.size() > 0) {
                assert(obtained_locks.size() == 1);
                lock_descriptor parent_lock = obtained_locks.back();
                obtained_locks.pop_back();
                manager.release_lock(parent_lock);
            }

            obtained_locks.push_back(l);

            void* buffer = this->blk_accessor_->malloc_buffer();
            this->blk_accessor_->read(current_node_id, buffer);
            int32_t node_type = *reinterpret_cast<int32_t*>(buffer);
            switch (node_type) {
                case LEAF_NODE: {
                    LeafNode<K, V, CAPACITY> *leaf = new LeafNode<K, V, CAPACITY>(this->blk_accessor_, false);
                    leaf->deserialize(buffer);
                    is_found = leaf->search(key, value);
                    manager.release_lock(l);
                    obtained_locks.pop_back();
                    delete leaf;
                    this->blk_accessor_->free_buffer(buffer);
                    return is_found;
                }
                case INNER_NODE: {
                    InnerNode<K, V, CAPACITY> *inner_node = new InnerNode<K, V, CAPACITY>(this->blk_accessor_, false);
                    inner_node->deserialize(buffer);
                    this->blk_accessor_->free_buffer(buffer);
                    int target_node_index = inner_node->locate_child_index(key);
//                    const bool exceed_left_boundary = target_node_index < 0;
//                    target_node_index = target_node_index < 0 ? 0 : target_node_index;
                    if (target_node_index < 0) {
                        manager.release_lock(l);
                        obtained_locks.pop_back();
                        delete inner_node;
                        return false;
                    }
                    blk_address child = inner_node->child_rep_[target_node_index];
                    is_found = search_with_concurrency_control(key, value, obtained_locks, child);
                    delete inner_node;
                    if (obtained_locks.size() > 0 && current_node_id == obtained_locks.back().id) {
                        manager.release_lock(obtained_locks.back());
                        obtained_locks.pop_back();
                    }
                    return is_found;
                }
                default: {
                    assert(false);
                }
            }
        }

        void concurrent_insert(const K &key, const V &value) {
            int current_node_id = get_root_id();
            deque<lock_descriptor> obtained_locks;
            stack<InnerNode<K, V, CAPACITY>*> parent_nodes;
            bool is_split;
            Split<K, V> split;

            if (!insert_with_optimistic_concurrency_control(key, value, obtained_locks, get_root_id(), this->get_height(), true)) {
                assert(obtained_locks.size() == 0);
                insert_with_pessimistic_concurrency_control(key, value, obtained_locks, parent_nodes, get_root_id(),
                                                            true, is_split, split);
                assert(obtained_locks.size() == 0);
            }
        }

        bool insert_with_optimistic_concurrency_control(const K &key, const V &value, std::deque<lock_descriptor> &obtained_locks, blk_address current_node_id, int current_depth, bool is_current_node_root) {
            lock_descriptor l;
            if (current_depth == 1)
                l = manager.get_write_lock(current_node_id);
            else
                l = manager.get_read_lock(current_node_id);
            if (is_current_node_root && get_root_id() != current_node_id) {
                // root has been updated
                manager.release_lock(l);
                return insert_with_optimistic_concurrency_control(key, value, obtained_locks, get_root_id(), this->get_height(), true);
            }

            if (obtained_locks.size() > 0) {
                assert(obtained_locks.size() == 1);
                lock_descriptor parent_lock = obtained_locks.back();
                obtained_locks.pop_back();
                manager.release_lock(parent_lock);
            }

            obtained_locks.push_back(l);

            void* buffer = this->blk_accessor_->malloc_buffer();
            this->blk_accessor_->read(current_node_id, buffer);
            int32_t node_type = *reinterpret_cast<int32_t*>(buffer);
            switch (node_type) {
                case LEAF_NODE: {
                    LeafNode<K, V, CAPACITY> *leaf = new LeafNode<K, V, CAPACITY>(this->blk_accessor_, false);
                    leaf->deserialize(buffer);
                    if (leaf->size() == CAPACITY) {
                        manager.release_lock(l);
                        obtained_locks.pop_back();
                        this->blk_accessor_->free_buffer(buffer);
                        delete leaf;
                        return false;
                    }

//                    lock_descriptor& lock = obtained_locks.back();
//                    manager.promote_to_write_lock(lock);
//                    assert(lock.type == WRITE_LOCK);
                    leaf->insert(key, value);
                    leaf->serialize(buffer);
                    this->blk_accessor_->write(current_node_id, buffer);
                    manager.release_lock(l);
                    obtained_locks.pop_back();
                    delete leaf;
                    this->blk_accessor_->free_buffer(buffer);
                    return true;
                }
                case INNER_NODE: {
                    InnerNode<K, V, CAPACITY> *inner_node = new InnerNode<K, V, CAPACITY>(this->blk_accessor_, false);
                    inner_node->deserialize(buffer);
                    this->blk_accessor_->free_buffer(buffer);
                    int target_node_index = inner_node->locate_child_index(key);
                    const bool exceed_left_boundary = target_node_index < 0;
                    bool is_succeed = false;
                    if (!exceed_left_boundary) {
                        target_node_index = target_node_index < 0 ? 0 : target_node_index;
                        blk_address child = inner_node->child_rep_[target_node_index];
                        is_succeed = insert_with_optimistic_concurrency_control(key, value, obtained_locks, child, current_depth - 1, false);
                    }
                    delete inner_node;
                    if (obtained_locks.size() > 0 && current_node_id == obtained_locks.back().id) {
                        manager.release_lock(obtained_locks.back());
                        obtained_locks.pop_back();
                    }
                    return is_succeed;
                }
                default: {
                    assert(false);
                }
            }

        }

        void insert_with_pessimistic_concurrency_control(const K &key, const V &value, std::deque<lock_descriptor> &obtained_locks,
                                                         std::stack<InnerNode<K, V, CAPACITY>*> &parent_nodes, blk_address current_node_id, bool is_current_node_root, bool &is_split, Split<K, V> &split) {

            lock_descriptor l = manager.get_write_lock(current_node_id);
            if (is_current_node_root && get_root_id() != current_node_id) {
                // root has been updated
                manager.release_lock(l);
                insert_with_pessimistic_concurrency_control(key, value, obtained_locks, parent_nodes, get_root_id(), true, is_split, split);
                return;
            }

            // lock is obtained

            obtained_locks.push_back(l);

            void* buffer = this->blk_accessor_->malloc_buffer();
            this->blk_accessor_->read(current_node_id, buffer);
            int32_t node_type = *reinterpret_cast<int32_t*>(buffer);
            switch(node_type) {
                case LEAF_NODE: {
                    LeafNode<K, V, CAPACITY>* leaf = new LeafNode<K, V, CAPACITY>(this->blk_accessor_, false);
                    leaf->deserialize(buffer);
                    is_split = leaf->insert_with_split_support(key, value, split);
                    leaf->serialize(buffer);
                    this->blk_accessor_->write(leaf->get_self_rep(), buffer);
                    if (is_split) {
                        split.right->serialize(buffer);
                        this->blk_accessor_->write(split.right->get_self_rep(), buffer);
                        if (is_current_node_root) {
                            // the root node was split.
                            InnerNode<K, V, CAPACITY> *new_inner_node = new InnerNode<K, V, CAPACITY>(split.left, split.right, this->blk_accessor_);
                            new_inner_node->mark_modified();
//                            printf("root update: %lld --> %lld\n", this->root_->get_unified_representation(),
//                                   new_inner_node->get_self_ref()->get_unified_representation());
                            new_inner_node->serialize(buffer);
                            this->blk_accessor_->write(new_inner_node->get_self_rep(), buffer);
                            update_root_node_id_and_increase_tree_height(new_inner_node->get_self_rep());
                            delete new_inner_node;
                            delete split.left;
                            delete split.right;
                            // a leaf will refer to a inner node now. TODO: release the old root_
                            // reference and create a new one.
//                            tree_->root_->bind(new_inner_node);
                        }
//                        delete split.right;
                    } else {
                        delete leaf;
                    }
                    manager.release_lock(l);
                    obtained_locks.pop_back();
                    this->blk_accessor_->free_buffer(buffer);
                    return;
                }
                case INNER_NODE: {
                    InnerNode<K, V, CAPACITY>* inner_node = new InnerNode<K, V, CAPACITY>(this->blk_accessor_, false);
                    inner_node->deserialize(buffer);
                    int target_node_index = inner_node->locate_child_index(key);
                    const bool exceed_left_boundary = target_node_index < 0;
                    if (exceed_left_boundary) {
                        inner_node->key_[0] = key;
                        inner_node->mark_modified();
                    }
                    target_node_index = target_node_index < 0 ? 0 : target_node_index;
                    blk_address child_node_id = inner_node->child_rep_[target_node_index];
                    parent_nodes.push(inner_node);
                    bool is_child_split;
                    Split<K, V> child_split;
                    insert_with_pessimistic_concurrency_control(key, value, obtained_locks, parent_nodes, child_node_id, false, is_child_split, child_split);

                    // the key value pair was inserted.
                    if (is_child_split) {
                        // handle the new child node;
                        if (inner_node->size() < CAPACITY) {
                            // the inner node has free slot for the new node.
                            inner_node->insert_inner_node(child_split.right, child_split.boundary_key,
                                                          target_node_index + 1);

                            inner_node->serialize(buffer);

                            this->blk_accessor_->write(current_node_id, buffer);
                            is_split = false;
                            lock_descriptor self_lock = obtained_locks.back();
                            obtained_locks.pop_back();
                            assert(self_lock.id == current_node_id);
                            manager.release_lock(self_lock);
                            delete inner_node;
                        } else {
                            // the inner node need to split to accommodate the new child node.
                            bool insert_to_first_half = target_node_index < CAPACITY / 2;

                            //
                            int start_index_for_right = CAPACITY / 2;
                            InnerNode<K, V, CAPACITY> *left = reinterpret_cast<InnerNode<K, V, CAPACITY>*>(inner_node);
                            InnerNode<K, V, CAPACITY> *right = new InnerNode<K, V, CAPACITY>(
                                    this->blk_accessor_);
                            right->mark_modified();
//                            node_reference<K, V> *right_ref = right->get_self_ref();

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
                                    child_split.boundary_key);
                            host_for_node->insert_inner_node(child_split.right, child_split.boundary_key,
                                                             inner_node_insert_position + 1);

                            // write the remaining content in the split data structure.
                            split.left = (left);
                            split.right = (right);
                            split.boundary_key = right->key_[0];

                            is_split = true;

                            // flush the inner node
                            left->serialize(buffer);
                            this->blk_accessor_->write(current_node_id, buffer);


                            // flush the new inner node.
                            right->serialize(buffer);
                            this->blk_accessor_->write(right->get_self_rep(), buffer);

                            if (is_current_node_root) {
                                // the root node was split.
                                InnerNode<K, V, CAPACITY> *new_inner_node = new InnerNode<K, V, CAPACITY>(split.left, split.right, this->blk_accessor_);
                                new_inner_node->mark_modified();
                                new_inner_node->serialize(buffer);
                                this->blk_accessor_->write(new_inner_node->get_self_rep(), buffer);
                                update_root_node_id_and_increase_tree_height(new_inner_node->get_self_rep());
                                delete new_inner_node;
                                delete left;
                                delete right;
                            }

//                            delete child_split.left;
//                            delete child_split.right;
                            // We do not need to delete child_split.left, because it is identical to inner_node, which
                            // will be deleted shortly.

                            lock_descriptor self_lock = obtained_locks.back();
                            obtained_locks.pop_back();
                            assert(self_lock.id == current_node_id);
                            manager.release_lock(self_lock);
                        }
                        delete child_split.left;
                        delete child_split.right;
                    } else {
                        is_split = false;
                        // close the parent nodes and flush the update if any.
                        if (inner_node->is_modified()) {
                            inner_node->serialize(buffer);
                            this->blk_accessor_->write(current_node_id, buffer);
                        }
                        delete inner_node;
                    }
                }
                if (obtained_locks.size() > 0 && current_node_id == obtained_locks.back().id) {
                    manager.release_lock(obtained_locks.back());
                    obtained_locks.pop_back();
                }
                this->blk_accessor_->free_buffer(buffer);
                return;
            }
        }

        blk_address get_root_id() {
            root_lock_.acquire();
            blk_address addr = this->root_->get_unified_representation();
            root_lock_.release();
            return addr;
        }

        void update_root_node_id_and_increase_tree_height(int64_t id) {
            root_lock_.acquire();
            this->root_->restore_by_unified_representation(id);
            this->depth_++;
            root_lock_.release();
        }

//        virtual void set_blk_accessor() override {
//            // should not be called;
//            assert(false);
//        }
    protected:
        const int block_size_;
    private:
        lock_manager manager;
        SpinLock root_lock_;
    };
}
#endif //NVM_CONCURRENT_B_PLUS_TREE_H
