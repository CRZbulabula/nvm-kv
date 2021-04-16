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

// bplus_tree::bplus_tree(const char *p, bool force_empty)
// 	: fp(NULL), fp_level(0)
// {
// 	bzero(path, sizeof(path));
// 	strcpy(path, p);

// 	if (!force_empty)
// 		// read tree from file
// 		if (map(&meta, OFFSET_META) != 0)
// 			force_empty = true;

// 	if (force_empty) {
// 		//open_file("w+"); // truncate file

// 		// create empty tree if file doesn't exist
// 		init();
// 		//close_file();
// 	}
// }

RetCode bplus_tree::init(const char *p)
{
	bzero(path, sizeof(path));
	strcpy(path, p);
	if (map(&meta, OFFSET_META) != 0) {
		RetCode ret = open_file("w+");
		if (ret != polar_race::kSucc) {
			return ret;
		}

		// init default meta
		bzero(&meta, sizeof(metaData));
		meta.order = childSize;
		meta.value_size = sizeof(polar_race::PolarString);
		meta.key_size = sizeof(polar_race::PolarString);
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
		unmap(&meta, OFFSET_META);
		unmap(&root, meta.root_offset);
		unmap(&leaf, root.children[0].child);
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
		map(&node, org);

		index* i = upper_bound(begin(node), end(node) - 1, key);
		org = i->child;
		--height;
	}

	return org;
}

off_t bplus_tree::search_leaf(off_t index, const polar_race::PolarString &key) const
{
	internalNode node;
	map(&node, index);

	b_plus_tree::index* i = upper_bound(begin(node), end(node) - 1, key);
	return i->child;
}

RetCode bplus_tree::search(const polar_race::PolarString &key, std::string *value) const
{
	leafNode leaf;
	map(&leaf, search_leaf(key));

	// finding the record
	record *record = find(leaf, key);
	if (record != leaf.children + leaf.n) {
		// always return the lower bound
		*value = record->value.data();
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
//         map(&leaf, off);

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
//         map(&leaf, off_right);

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
	map(&leaf, offset);

	// check if we have the same key
	record *where = find(leaf, key);
	if (where != leaf.children + leaf.n) {
		if (where->key == key) {
			where->value = value;
			unmap(&leaf, offset);
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
		unmap(&leaf, offset);
		unmap(&new_leaf, leaf.next);

		// insert new index key
		insert_key_to_index(parent, new_leaf.children[0].key,
							offset, leaf.next);
	} else {
		insert_record_no_split(&leaf, key, value);
		unmap(&leaf, offset);
	}

	return polar_race::kSucc;
}

bool bplus_tree::borrow_key(bool from_right, internalNode &borrower,
							off_t offset)
{
	typedef typename internalNode::child child;

	off_t lender_off = from_right ? borrower.next : borrower.prev;
	internalNode lender;
	map(&lender, lender_off);

	assert(lender.n >= meta.order / 2);
	if (lender.n != meta.order / 2) {
		child where_to_lend, where_to_put;

		internalNode parent;

		// swap keys, draw on paper to see why
		if (from_right) {
			where_to_lend = begin(lender);
			where_to_put = end(borrower);

			map(&parent, borrower.parent);
			child where = lower_bound(begin(parent), end(parent) - 1,
										(end(borrower) -1)->key);
			where->key = where_to_lend->key;
			unmap(&parent, borrower.parent);
		} else {
			where_to_lend = end(lender) - 1;
			where_to_put = begin(borrower);

			map(&parent, lender.parent);
			child where = find(parent, begin(lender)->key);
			where_to_put->key = where->key;
			where->key = (where_to_lend - 1)->key;
			unmap(&parent, lender.parent);
		}

		// store
		std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
		*where_to_put = *where_to_lend;
		borrower.n++;

		// erase
		reset_index_children_parent(where_to_lend, where_to_lend + 1, offset);
		std::copy(where_to_lend + 1, end(lender), where_to_lend);
		lender.n--;
		unmap(&lender, lender_off);
		return true;
	}

	return false;
}

bool bplus_tree::borrow_key(bool from_right, leafNode &borrower)
{
	off_t lender_off = from_right ? borrower.next : borrower.prev;
	leafNode lender;
	map(&lender, lender_off);

	assert(lender.n >= meta.order / 2);
	if (lender.n != meta.order / 2) {
		typename leafNode::child where_to_lend, where_to_put;

		// decide offset and update parent's index key
		if (from_right) {
			where_to_lend = begin(lender);
			where_to_put = end(borrower);
			change_parent_child(borrower.parent, begin(borrower)->key,
								lender.children[1].key);
		} else {
			where_to_lend = end(lender) - 1;
			where_to_put = begin(borrower);
			change_parent_child(lender.parent, begin(lender)->key,
								where_to_lend->key);
		}

		// store
		std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
		*where_to_put = *where_to_lend;
		borrower.n++;

		// erase
		std::copy(where_to_lend + 1, end(lender), where_to_lend);
		lender.n--;
		unmap(&lender, lender_off);
		return true;
	}

	return false;
}

void bplus_tree::change_parent_child(off_t parent, const polar_race::PolarString &o,
									 const polar_race::PolarString &n)
{
	internalNode node;
	map(&node, parent);

	index *w = find(node, o);
	assert(w != node.children + node.n); 

	w->key = n;
	unmap(&node, parent);
	if (w == node.children + node.n - 1) {
		change_parent_child(node.parent, o, n);
	}
}

void bplus_tree::merge_leafs(leafNode *left, leafNode *right)
{
	std::copy(begin(*right), end(*right), end(*left));
	left->n += right->n;
}

void bplus_tree::merge_keys(index *where,
							internalNode &node, internalNode &next)
{
	//(end(node) - 1)->key = where->key;
	//where->key = (end(next) - 1)->key;
	std::copy(begin(next), end(next), end(node));
	node.n += next.n;
	node_remove(&node, &next);
}

void bplus_tree::insert_record_no_split(leafNode *leaf, const polar_race::PolarString &key, 
										const polar_race::PolarString &value)
{
	record *where = upper_bound(begin(*leaf), end(*leaf), key);
	std::copy_backward(where, end(*leaf), end(*leaf) + 1);

	where->key = key;
	where->value = value;
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
		root.children[0].key = key;
		root.children[0].child = old;
		root.children[1].child = after;

		unmap(&meta, OFFSET_META);
		unmap(&root, meta.root_offset);

		// update children's parent
		reset_index_children_parent(begin(root), end(root),
									meta.root_offset);
		return;
	}

	internalNode node;
	map(&node, offset);
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

		unmap(&node, offset);
		unmap(&new_node, node.next);

		// update children's parent
		reset_index_children_parent(begin(new_node), end(new_node), node.next);

		// give the middle key to the parent
		// note: middle key's child is reserved
		insert_key_to_index(node.parent, middle_key, offset, node.next);
	} else {
		insert_key_to_index_no_split(node, key, after);
		unmap(&node, offset);
	}
}

void bplus_tree::insert_key_to_index_no_split(internalNode &node,
											  const polar_race::PolarString &key, off_t value)
{
	index *where = upper_bound(begin(node), end(node) - 1, key);

	// move later index forward
	std::copy_backward(where, end(node), end(node) + 1);

	// insert this key
	where->key = key;
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
		map(&node, begin->child);
		node.parent = parent;
		unmap(&node, begin->child, SIZE_NO_CHILDREN);
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
		map(&old_next, next->next, SIZE_NO_CHILDREN);
		old_next.prev = node->next;
		unmap(&old_next, next->next, SIZE_NO_CHILDREN);
	}
	unmap(&meta, OFFSET_META);
}

template<class T>
void bplus_tree::node_remove(T *prev, T *node)
{
	unalloc(node, prev->next);
	prev->next = node->next;
	if (node->next != 0) {
		T next;
		map(&next, node->next, SIZE_NO_CHILDREN);
		next.prev = node->prev;
		unmap(&next, node->next, SIZE_NO_CHILDREN);
	}
	unmap(&meta, OFFSET_META);
}

}
