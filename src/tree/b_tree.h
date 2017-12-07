//
// Created by robert on 14/9/17.
//

#ifndef B_TREE_B_TREE_H
#define B_TREE_B_TREE_H

template <typename K, typename V>
class blk_accessor;

namespace tree {
    template<typename K, typename V>
    class BTree {
    public:
        virtual void insert(const K &k, const V &v) = 0;

        virtual bool delete_key(const K &k) = 0;

        virtual bool search(const K &k, V &v) = 0;

        virtual void clear() = 0;

        virtual void close() {};

        virtual void sync() {};

        class Iterator {
        public:
            virtual ~Iterator(){};
            virtual bool next(K &key, V &val) {
                return false;
            };
        };

        virtual Iterator *range_search(const K &key_low, const K &key_high) = 0;

        virtual blk_accessor<K, V>* get_accessor() {};
    };
}
#endif //B_TREE_B_TREE_H
