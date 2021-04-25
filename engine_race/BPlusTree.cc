#include "BPlusTree.h"

#include <stdlib.h>

#include <list>
#include <algorithm>
using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

namespace b_plus_tree {

/* custom compare operator for STL algorithms */
// OPERATOR_KEYCMP(index)
// OPERATOR_KEYCMP(record)

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
inline index *find(internalNode &node, const polar_race::PolarString &key) {
	return upper_bound(begin(node), end(node) - 1, key);
}
inline record *find(leafNode &node, const polar_race::PolarString &key) {
	return lower_bound(begin(node), end(node), key);
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
		root.next = root.prev = root.parent = 0;
		meta.root_offset = alloc(&root);

		// init empty leaf
		leafNode leaf;
		leaf.next = leaf.prev = 0;
		leaf.parent = meta.root_offset;
		meta.leaf_offset = root.children[0].child = alloc(&leaf);

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
		printf("internalNode: ");
		node_printf(&node);
		index* i = upper_bound(begin(node), end(node) - 1, key);
		printf("pos: %d\n", upper_bound(begin(node), end(node) - 1, key) - begin(node));
		org = i->child;
		--height;
	}

	return org;
}

off_t bplus_tree::search_leaf(off_t index, const polar_race::PolarString &key) const
{
	internalNode node;
	disk_read(&node, index);
	printf("internalNode: ");
	node_printf(&node);
	b_plus_tree::index* i = upper_bound(begin(node), end(node) - 1, key);
	printf("pos: %d\n", upper_bound(begin(node), end(node) - 1, key) - begin(node));
	return i->child;
}

RetCode bplus_tree::search(const polar_race::PolarString &key, std::string *value) const
{
	tree_printf();
	puts("");
	leafNode leaf;
	disk_read(&leaf, search_leaf(key));

	// finding the record
	printf("leafNode: ");
	node_printf(&leaf);
	record *record = find(leaf, key);
	if (record != leaf.children + leaf.n) {
		// always return the lower bound
		printf("%s %s\n\n", key.data(), record->key);
		char *valueBlock = new char[record->valueSize + 1];
		bzero(valueBlock, record->valueSize + 1);
		disk_read(valueBlock, record->valueOff, record->valueSize);
		*value = valueBlock;
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

	// check if we have the same key
	record *where = find(leaf, key);
	if (where != leaf.children + leaf.n) {
		if (where->key == key) {
			// rewrite the value
			where->valueSize = value.size();
			where->valueOff = alloc(value.size());
			disk_write(value.data(), where->valueOff, where->valueSize);
			disk_write(&leaf, offset);
			return polar_race::kSucc;
		}
	}

	if (leaf.n == meta.order) {
		// split when full

		// new sibling leaf
		leafNode new_leaf;
		node_create(offset, &leaf, &new_leaf);

		// find even split point
		size_t point = leaf.n / 2;
		bool place_right = key.compare(leaf.children[point].key) > 0;
		if (place_right)
			++point;

		// split
		std::copy(leaf.children + point, leaf.children + leaf.n,
				  new_leaf.children);
		new_leaf.n = leaf.n - point;
		leaf.n = point;

		// which part do we put the key
		if (place_right)
			insert_record_no_split(&new_leaf, key, value);
		else
			insert_record_no_split(&leaf, key, value);

		// save leafs
		disk_write(&leaf, offset);
		disk_write(&new_leaf, leaf.next);

		// insert new index key
		insert_key_to_index(parent, new_leaf.children[0].key,
							offset, leaf.next);
	} else {
		insert_record_no_split(&leaf, key, value);
		disk_write(&leaf, offset);
	}

	return polar_race::kSucc;
}

void bplus_tree::change_parent_child(off_t parent, const polar_race::PolarString &o,
									 const polar_race::PolarString &n)
{
	internalNode node;
	disk_read(&node, parent);

	index *w = find(node, o);
	assert(w != node.children + node.n); 

	strcpy(w->key, n.data());
	disk_write(&node, parent);
	if (w == node.children + node.n - 1) {
		change_parent_child(node.parent, o, n);
	}
}

void bplus_tree::insert_record_no_split(leafNode *leaf, const polar_race::PolarString &key, 
										const polar_race::PolarString &value)
{
	record *where = upper_bound(begin(*leaf), end(*leaf), key);
	std::copy_backward(where, end(*leaf), end(*leaf) + 1);

	strcpy(where->key, key.data());
	where->valueSize = value.size();
	where->valueOff = alloc(where->valueSize);
	disk_write(value.data(), where->valueOff, where->valueSize);
	leaf->n++;
}

void bplus_tree::insert_key_to_index(off_t offset, const polar_race::PolarString &key,
									 off_t old, off_t after)
{
	if (offset == 0) {
		// create new root node
		internalNode root;
		root.next = root.prev = root.parent = 0;
		meta.root_offset = alloc(&root);
		meta.height++;

		// insert `old` and `after`
		root.n = 2;
		strcpy(root.children[0].key, key.data());
		root.children[0].child = old;
		root.children[1].child = after;

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
		node_create(offset, &node, &new_node);

		// find even split point
		size_t point = (node.n - 1) / 2;
		bool place_right = key.compare(node.children[point].key) > 0;
		if (place_right)
			++point;

		// prevent the `key` being the right `middle_key`
		// example: insert 48 into |42|45| 6|  |
		if (place_right && key.compare(node.children[point].key) < 0)
			point--;

		polar_race::PolarString middle_key = node.children[point].key;

		// split
		std::copy(begin(node) + point + 1, end(node), begin(new_node));
		new_node.n = node.n - point - 1;
		node.n = point + 1;

		// put the new key
		if (place_right)
			insert_key_to_index_no_split(new_node, key, after);
		else
			insert_key_to_index_no_split(node, key, after);

		disk_write(&node, offset);
		disk_write(&new_node, node.next);

		// update children's parent
		reset_index_children_parent(begin(new_node), end(new_node), node.next);

		// give the middle key to the parent
		// note: middle key's child is reserved
		insert_key_to_index(node.parent, middle_key, offset, node.next);
	} else {
		insert_key_to_index_no_split(node, key, after);
		disk_write(&node, offset);
	}
}

void bplus_tree::insert_key_to_index_no_split(internalNode &node,
											  const polar_race::PolarString &key, off_t value)
{
	index *where = upper_bound(begin(node), end(node) - 1, key);

	// move later index forward
	std::copy_backward(where, end(node), end(node) + 1);

	// insert this key
	strcpy(where->key, key.data());
	where->child = (where + 1)->child;
	(where + 1)->child = value;

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
		node.parent = parent;
		disk_write(&node, begin->child, SIZE_NO_CHILDREN);
		++begin;
	}
}

template<class T>
void bplus_tree::node_create(off_t offset, T *node, T *next)
{
	// new sibling node
	next->parent = node->parent;
	next->next = node->next;
	next->prev = offset;
	node->next = alloc(next);
	// update next node's prev
	if (next->next != 0) {
		T old_next;
		disk_read(&old_next, next->next, SIZE_NO_CHILDREN);
		old_next.prev = node->next;
		disk_write(&old_next, next->next, SIZE_NO_CHILDREN);
	}
	disk_write(&meta, OFFSET_META);
}

}
