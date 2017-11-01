//
// Created by Li Wang on 10/27/17.
//

#ifndef NVM_NODE_REFERENCE_H
#define NVM_NODE_REFERENCE_H

#include "node.h"
namespace tree {

    template<typename K, typename V>
    class Node;

    template<typename K, typename V>
    class node_reference {
    public:
        // get an instance of the node referred to.
        virtual Node<K, V>* get() {
            return nullptr;
        };

        // flush the change.
        virtual void close() = 0;

        // remove the reference as well as the referred node.
        virtual void remove() = 0;

        virtual void copy(node_reference* ref) = 0;

        virtual bool is_null_ptr() const = 0;

        // bind this reference to a particular node
        virtual void bind(Node<K, V>* node) = 0;
    };
}
#endif //NVM_NODE_REFERENCE_H