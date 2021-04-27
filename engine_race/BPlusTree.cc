#include "BPlusTree.h"

#include <stdlib.h>

#include <list>
#include <algorithm>
using std::swap;
using std::binary_search;
using std::lower_bound;

namespace b_plus_tree {

/* helper iterating function */
template<class T>
inline typename T::child begin(T &node) {
	return node.children; 
}
template<class T>
inline typename T::child end(T &node) {
	return node.children + node.n;
}

/* helper searching function */
template<class T>
inline record *find(T &node, const polar_race::PolarString &key) {
	return lower_bound(begin(node), end(node), key);
}

//锁操作相关函数
inline void bplus_node_rlock(internalNode *bn)
{
  latch_rlock(bn->lock);
}

inline void bplus_node_wlock(internalNode *bn)
{
  latch_wlock(bn->lock);
}

inline void bplus_node_unlock(internalNode *bn)
{
  latch_unlock(bn->lock);
}

inline void bplus_node_rlock(leafNode *bn)
{
  latch_rlock(bn->lock);
}

inline void bplus_node_wlock(leafNode *bn)
{
  latch_wlock(bn->lock);
}

inline void bplus_node_unlock(leafNode *bn)
{
  latch_unlock(bn->lock);
}

RetCode bplus_tree::init(const char *p)
{
	bzero(path, sizeof(path));
	strcpy(path, p);
	if (disk_read(&meta, OFFSET_META) != 0) {
		RetCode ret = open_file("w+");
		if (ret != polar_race::kSucc) {
			return ret;
		}

		// init default meta
		bzero(&meta, sizeof(metaData));
		meta.order = childSize;
		meta.height = 1;
		meta.slot = OFFSET_BLOCK;

		// init root node
		internalNode root;
		latch_init(root.lock);
		root.next = root.prev = root.parent = 0;
		meta.root_offset = alloc(&root);

		// init empty leaf
		leafNode leaf;
		latch_init(leaf.lock);
		leaf.next = leaf.prev = 0;
		leaf.parent = meta.root_offset;
		meta.leaf_offset = root.children[0].child = alloc(&leaf);
		leaf.n = 1;

		// set lastChar
		char maxChar[1] = {(char)127};
		strcpy(root.children[0].key, maxChar);
		strcpy(leaf.children[0].key, maxChar);

		// save
		disk_write(&meta, OFFSET_META);
		disk_write(&root, meta.root_offset);
		disk_write(&leaf, root.children[0].child);
		ret = close_file();
		return ret;
	}
	return polar_race::kSucc;
}

off_t bplus_tree::search_index(const polar_race::PolarString &key) const
{
	off_t org = meta.root_offset;
	int height = meta.height;
	while (height > 1) {
		internalNode node;
		disk_read(&node, org);
		bplus_node_rlock(&node);
		disk_write(&node,org);
		index* i = lower_bound(begin(node), end(node), key);
		org = i->child;
		--height;
		bplus_node_unlock(&node);
		disk_write(&node,org);
	}
	return org;
}

off_t bplus_tree::search_leaf(off_t index, const polar_race::PolarString &key) const
{
	internalNode node;
	disk_read(&node, index);
	bplus_node_rlock(&node);
	disk_write(&node,index);
	b_plus_tree::index* i = lower_bound(begin(node), end(node), key);
	return i->child;
}

RetCode bplus_tree::search(const polar_race::PolarString &key, std::string *value) const
{
	leafNode leaf;
	disk_read(&leaf, search_leaf(key));
	// finding the record
	record *record = find(leaf, key);
	if (record != leaf.children + leaf.n) {
		// always return the lower bound
		bplus_node_rlock(&leaf);
		disk_write(&leaf,search_leaf(key));
		char *valueBlock = new char[record->valueSize + 1];
		bzero(valueBlock, record->valueSize + 1);
		disk_read(valueBlock, record->valueOff, record->valueSize);
		*value = valueBlock;
		bplus_node_unlock(&leaf);
		disk_write(&leaf,search_leaf(key));
		return record->key == key ? polar_race::kSucc : polar_race::kNotFound;
	} else {
		return polar_race::kNotFound;
	}
}

// int bplus_tree::search_range(polar_race::PolarString *left, const polar_race::PolarString &right,
//                              value_t *values, size_t max, bool *next) const
// {
//     if (left == NULL || keycmp(*left, right) > 0)
//         return -1;

//     off_t off_left = search_leaf(*left);
//     off_t off_right = search_leaf(right);
//     off_t off = off_left;
//     size_t i = 0;
//     record *b, *e;

//     leafNode leaf;
//     while (off != off_right && off != 0 && i < max) {
//         disk_read(&leaf, off);

//         // start point
//         if (off_left == off) 
//             b = find(leaf, *left);
//         else
//             b = begin(leaf);

//         // copy
//         e = leaf.children + leaf.n;
//         for (; b != e && i < max; ++b, ++i)
//             values[i] = b->value;

//         off = leaf.next;
//     }

//     // the last leaf
//     if (i < max) {
//         disk_read(&leaf, off_right);

//         b = find(leaf, *left);
//         e = upper_bound(begin(leaf), end(leaf), right);
//         for (; b != e && i < max; ++b, ++i)
//             values[i] = b->value;
//     }

//     // mark for next iteration
//     if (next != NULL) {
//         if (i == max && b != e) {
//             *next = true;
//             *left = b->key;
//         } else {
//             *next = false;
//         }
//     }

//     return i;
// }

RetCode bplus_tree::insert_or_update(const polar_race::PolarString& key, polar_race::PolarString value)
{
	off_t parent = search_index(key);
	off_t offset = search_leaf(parent, key);
	leafNode leaf;
	disk_read(&leaf, offset);
	bplus_node_wlock(&leaf);
	disk_write(&leaf,offset);
	// check if we have the same key
	record *where = find(leaf, key);
	if (where != leaf.children + leaf.n) {
		if (where->key == key) {
			// rewrite the value
			where->valueSize = value.size();
			where->valueOff = alloc(value.size());
			disk_write(value.data(), where->valueOff, where->valueSize);
			bplus_node_unlock(&leaf);
			disk_write(&leaf, offset);
			return polar_race::kSucc;
		}
	}

	if (leaf.n == meta.order) {
		// split when full

		// new sibling leaf
		leafNode new_leaf;
		latch_init(new_leaf.lock);
		node_create(offset, &leaf, &new_leaf);

		// find even split point
		size_t point = leaf.n / 2;
		bool place_right = key.compare(leaf.children[point].key) > 0;
		if (place_right)
			++point;

		// split
		std::copy(begin(leaf), begin(leaf) + point, begin(new_leaf));
		std::copy(begin(leaf) + point, end(leaf), begin(leaf));
		new_leaf.n = point;
		leaf.n = leaf.n - point;

		// which part do we put the key
		if (place_right)
			insert_record_no_split(&leaf, key, value);
		else
			insert_record_no_split(&new_leaf, key, value);

		// save leafs
		bplus_node_unlock(&leaf);
		disk_write(&leaf, offset);
		disk_write(&new_leaf, leaf.prev);

		// insert new index key
		insert_key_to_index(parent, new_leaf.children[new_leaf.n - 1].key,
							offset, leaf.prev);
	} else {
		insert_record_no_split(&leaf, key, value);
		bplus_node_unlock(&leaf);
		disk_write(&leaf, offset);
	}

	return polar_race::kSucc;
}

void bplus_tree::insert_record_no_split(leafNode *leaf, const polar_race::PolarString &key, 
										const polar_race::PolarString &value)
{
	record *where = lower_bound(begin(*leaf), end(*leaf), key);
	std::copy_backward(where, end(*leaf), end(*leaf) + 1);

	strcpy(where->key, key.data());
	where->valueSize = value.size();
	where->valueOff = alloc(where->valueSize);
	disk_write(value.data(), where->valueOff, where->valueSize);
	leaf->n++;
}

void bplus_tree::insert_key_to_index(off_t offset, const polar_race::PolarString &key,
									 off_t old, off_t before)
{
	if (offset == 0) {
		assert(before == 0 || old == 0);
		// create new root node
		internalNode root;
		latch_init(root.lock);
		root.next = root.prev = root.parent = 0;
		meta.root_offset = alloc(&root);
		meta.height++;

		// insert `old` and `before`
		root.n = 2;
		strcpy(root.children[0].key, key.data());
		root.children[0].child = before;
		root.children[1].child = old;

		// set last key
		char maxChar[1] = {(char)127};
		strcpy(root.children[1].key, maxChar);

		disk_write(&meta, OFFSET_META);
		disk_write(&root, meta.root_offset);

		// update children's parent
		reset_index_children_parent(begin(root), end(root),
									meta.root_offset);
		return;
	}

	internalNode node;
	disk_read(&node, offset);
	assert(node.n <= meta.order);

	if (node.n == meta.order) {
		// split when full

		internalNode new_node;
		latch_init(new_node.lock);
		node_create(offset, &node, &new_node);

		// find even split point
		size_t point = node.n / 2;
		bool place_right = key.compare(node.children[point].key) > 0;
		if (place_right)
			++point;

		std::copy(begin(node), begin(node) + point, begin(new_node));
		std::copy(begin(node) + point, end(node), begin(node));
		new_node.n = point;
		node.n = node.n - point;

		// put the new key
		if (place_right)
			insert_key_to_index_no_split(node, key, before);
		else
			insert_key_to_index_no_split(new_node, key, before);

		bplus_node_unlock(&node);
		disk_write(&node, offset);
		disk_write(&new_node, node.prev);

		// update children's parent
		reset_index_children_parent(begin(new_node), end(new_node), node.prev);

		// give the middle key to the parent
		// note: middle key's child is reserved
		internalNode parent_node;
		disk_read(&parent_node,node.parent);
		bplus_node_wlock(&parent_node);
		insert_key_to_index(node.parent, new_node.children[new_node.n - 1].key, 
							offset, node.prev);
	} else {
		insert_key_to_index_no_split(node, key, before);
		bplus_node_unlock(&node);
		disk_write(&node, offset);
	}
}

void bplus_tree::insert_key_to_index_no_split(internalNode &node,
											  const polar_race::PolarString &key, off_t value)
{
	index *where = lower_bound(begin(node), end(node), key);

	// move later index forward
	std::copy_backward(where, end(node), end(node) + 1);

	// insert this key
	strcpy(where->key, key.data());
	where->child = value;

	node.n++;
}

void bplus_tree::reset_index_children_parent(index *begin, index *end,
											 off_t parent)
{
	// this function can change both internalNode and leafNode's parent
	// field, but we should ensure that:
	// 1. sizeof(internalNode) <= sizeof(leafNode)
	// 2. parent field is placed in the beginning and have same size
	internalNode node;
	while (begin != end) {
		disk_read(&node, begin->child);
		bplus_node_rlock(&node);
		node.parent = parent;
		bplus_node_unlock(&node);
		disk_write(&node, begin->child);
		++begin;
	}
}

template<class T>
void bplus_tree::node_create(off_t offset, T *node, T *prev)
{
	// new sibling node
	prev->parent = node->parent;
	prev->prev = node->prev;
	prev->next = offset;
	node->prev = alloc(prev);
	// update prev node's next
	if (prev->prev != 0) {
		T old_prev;
		disk_read(&old_prev, prev->prev, SIZE_NO_CHILDREN);
		old_prev.next = node->prev;
		disk_write(&old_prev, prev->prev, SIZE_NO_CHILDREN);
	}
	disk_write(&meta, OFFSET_META);
}

}
