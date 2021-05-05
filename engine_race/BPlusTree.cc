#include "BPlusTree.h"

#include <stdlib.h>

#include <list>
#include <set>
using std::swap;
using std::binary_search;

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
	return node.lower_bound(key);
}

/* helper read key function */
polar_race::PolarString readKey(char *block, off_t keyOff, size_t keySize) {
	char *keyBlock = new char[keySize + 1];
	bzero(keyBlock, keySize + 1);
	strncpy(keyBlock, block + keyOff, keySize);
	return polar_race::PolarString(keyBlock, keySize);
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
		//meta.order = childSize;
		meta.height = 1;
		meta.slot = OFFSET_BLOCK;

		// init root node
		internalNode root;
		root.id = meta.number++;
		root.next = root.prev = root.parent = 0;
		meta.root_offset = alloc(&root);

		// init empty leaf
		leafNode leaf;
		leaf.id = meta.number++;
		leaf.next = leaf.prev = 0;
		leaf.parent = meta.root_offset;
		meta.leaf_offset = alloc(&leaf);

		// set lastChar
		char maxChar[2] = {(std::numeric_limits<char>::max)(), (char)0};
		polar_race::PolarString maxPolar = polar_race::PolarString(maxChar);
		root.append(maxChar, meta.leaf_offset);
		leaf.append(maxChar, -1, -1);

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
		index* i = node.lower_bound(key);
		org = i->child;
		--height;
	}

	return org;
}

off_t bplus_tree::search_leaf(off_t index, const polar_race::PolarString &key) const
{
	internalNode node;
	disk_read(&node, index);
	b_plus_tree::index* i = node.lower_bound(key);
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
		polar_race::PolarString findKey = leaf.getKey(record->keyOff, record->keySize);
		if (findKey != key) {
			return polar_race::kNotFound;
		}
		char *valueBlock = new char[record->valueSize + 1];
		bzero(valueBlock, record->valueSize + 1);
		disk_read(valueBlock, record->valueOff, record->valueSize);
		*value = std::string(valueBlock, record->valueSize);
		return polar_race::kSucc;
	} else {
		return polar_race::kNotFound;
	}
}

RetCode bplus_tree::search_range(const polar_race::PolarString &left, 
							const polar_race::PolarString &right, polar_race::Visitor& visitor) const
{
	if (right != "" && left.compare(right) > 0) {
		return RetCode::kInvalidArgument;
	}

	off_t first_off, last_off;
	if (left == "") {
		first_off = meta.leaf_offset;
		leafNode leaf;
		while (true) {
			disk_read(&leaf, first_off);
			if (leaf.prev == 0) {
				break;
			}
			first_off = leaf.prev;
		}
	}
	else {
		first_off = search_leaf(left);
	}
	if (right == "") {
		last_off = meta.leaf_offset;
	}
	else {
		last_off = search_leaf(right);
	}

	off_t cur_off = first_off;
	size_t i = 0;
	record *First, *Last;
	leafNode leaf;

	while (cur_off != last_off && cur_off != 0) {
		disk_read(&leaf, cur_off);
		if (cur_off == first_off && left != "") {
			First = find(leaf, left);
		}
		else {
			First = begin(leaf);
		}
		Last = end(leaf);
		for (; First != Last; First++) {
			char *valueBlock = new char[First->valueSize + 1];
			bzero(valueBlock, First->valueSize + 1);
			disk_read(valueBlock, First->valueOff, First->valueSize);
			// printf("append: %s %s\n", leaf.getKey(First->keyOff, First->keySize).data(),
			// 		polar_race::PolarString(valueBlock, First->valueSize).data());
			visitor.Visit(leaf.getKey(First->keyOff, First->keySize), 
						  polar_race::PolarString(valueBlock, First->valueSize));
		}
		cur_off = leaf.next;
	}

	// the last leaf
	disk_read(&leaf, last_off);
	if (cur_off == first_off) {
		First = find(leaf, left);
	}
	else {
		First = begin(leaf);
	}
	
	if (right == "") {
		Last = end(leaf);
	}
	else {
		Last = find(leaf, right);
	}
	for (; First != Last; First++) {
		char *valueBlock = new char[First->valueSize + 1];
		bzero(valueBlock, First->valueSize + 1);
		disk_read(valueBlock, First->valueOff, First->valueSize);
		// printf("append: %s %s\n", leaf.getKey(First->keyOff, First->keySize).data(),
		// 		polar_race::PolarString(valueBlock, First->valueSize).data());
		visitor.Visit(leaf.getKey(First->keyOff, First->keySize), 
						polar_race::PolarString(valueBlock, First->valueSize));
	}

	return RetCode::kSucc;
}

RetCode bplus_tree::insert_or_update(const polar_race::PolarString& key, polar_race::PolarString value)
{
	off_t parent = search_index(key);
	off_t offset = search_leaf(parent, key);
	//printf("%d %d\n", parent, offset);
	leafNode leaf;
	disk_read(&leaf, offset);

	// check if we have the same key
	record *where = find(leaf, key);
	if (where != leaf.children + leaf.n) {
		polar_race::PolarString findKey = leaf.getKey(where->keyOff, where->keySize);
		if (findKey == key) {
			// rewrite the value
			where->valueSize = value.size();
			where->valueOff = alloc(value.size());
			disk_write(value.data(), where->valueOff, where->valueSize);
			disk_write(&leaf, offset);

			disk_write(&meta, OFFSET_META);
			fsync();
			return polar_race::kSucc;
		}
	}

	// split when full
	//size_t delta = key.size() < minKeyLength ? minKeyLength : key.size();
	if (leaf.slot + key.size() > poolSize || leaf.n == childSize) {
		// new sibling leaf
		leafNode new_leaf;
		node_create(offset, &leaf, &new_leaf);

		// find even split point
		size_t point = leaf.n / 2;
		bool place_right = key.compare(leaf.getKey(point)) > 0;
		if (place_right)
			++point;

		// split
		int oldSize = leaf.n;
		char *oldPool = new char[poolSize + 1];
		bzero(oldPool, poolSize + 1);
		strncpy(oldPool, leaf.pool, poolSize);
		leaf.n = new_leaf.n = 0;
		leaf.slot = new_leaf.slot = 0;
		//printf("pool: %s\n", leaf.pool);
		for (int Index = 0; Index < point; Index++) {
			//printf("append to new: %d %d\n", leaf.children[Index].keyOff, leaf.children[Index].keySize);
			new_leaf.append(readKey(oldPool, leaf.children[Index].keyOff, leaf.children[Index].keySize),
							leaf.children[Index].valueOff, leaf.children[Index].valueSize);
		}
		for (int Index = point; Index < oldSize; Index++) {
			//printf("append to old: %d %d\n", leaf.children[Index].keyOff, leaf.children[Index].keySize);
			leaf.append(readKey(oldPool, leaf.children[Index].keyOff, leaf.children[Index].keySize),
						leaf.children[Index].valueOff, leaf.children[Index].valueSize);
		}
		//printf("%d %d\n", new_leaf.slot, leaf.slot);

		// which part do we put the key
		if (place_right)
			insert_record_no_split(&leaf, key, value);
		else
			insert_record_no_split(&new_leaf, key, value);
		// save leafs
		disk_write(&leaf, offset);
		disk_write(&new_leaf, leaf.prev);

		// insert new index key
		insert_key_to_index(parent, new_leaf.getKey(new_leaf.n - 1),
							offset, leaf.prev);
	} else {
		insert_record_no_split(&leaf, key, value);
		disk_write(&leaf, offset);
	}

	//tree_printf();
	disk_write(&meta, OFFSET_META);
	fsync();
	return polar_race::kSucc;
}

void bplus_tree::insert_record_no_split(leafNode *leaf, const polar_race::PolarString &key, 
										const polar_race::PolarString &value)
{
	record *where = leaf->lower_bound(key);
	std::copy_backward(where, end(*leaf), end(*leaf) + 1);
	leaf->insert_key(key, where);
	where->valueSize = value.size();
	where->valueOff = alloc(where->valueSize);
	disk_write(value.data(), where->valueOff, where->valueSize);
}

void bplus_tree::insert_key_to_index(off_t offset, const polar_race::PolarString &key,
									 off_t old, off_t before)
{
	if (offset == 0) {
		assert(before == 0 || old == 0);
		// create new root node
		internalNode root;
		root.id = meta.number++;
		root.next = root.prev = root.parent = 0;
		meta.root_offset = alloc(&root);
		meta.height++;

		// insert `old` and `before`
		root.insert_key(key.data(), &root.children[0]);
		char maxChar[2] = {(std::numeric_limits<char>::max)(), (char)0};
		root.insert_key(maxChar, &root.children[1]);
		root.children[0].child = before;
		root.children[1].child = old;

		disk_write(&meta, OFFSET_META);
		disk_write(&root, meta.root_offset);

		// update children's parent
		reset_index_children_parent(begin(root), end(root),
									meta.root_offset);
		return;
	}

	internalNode node;
	disk_read(&node, offset);

	// split when full
	//size_t delta = key.size() < minKeyLength ? minKeyLength : key.size();
	if (node.slot + key.size() > poolSize || node.n == childSize) {
		internalNode new_node;
		node_create(offset, &node, &new_node);

		// find even split point
		size_t point = node.n / 2;
		bool place_right = key.compare(node.getKey(point)) > 0;
		if (place_right)
			++point;

		int oldSize = node.n;
		char *oldPool = new char[poolSize];
		bzero(oldPool, poolSize);
		strncpy(oldPool, node.pool, poolSize);
		node.n = new_node.n = 0;
		node.slot = new_node.slot = 0;
		//printf("pool: %s\n", node.pool);
		for (int Index = 0; Index < point; Index++) {
			new_node.append(readKey(oldPool, node.children[Index].keyOff, node.children[Index].keySize),
							node.children[Index].child);
		}
		for (int Index = point; Index < oldSize; Index++) {
			node.append(readKey(oldPool, node.children[Index].keyOff, node.children[Index].keySize),
						node.children[Index].child);
		}

		// put the new key
		if (place_right)
			insert_key_to_index_no_split(node, key, before);
		else
			insert_key_to_index_no_split(new_node, key, before);

		disk_write(&node, offset);
		disk_write(&new_node, node.prev);

		// update children's parent
		reset_index_children_parent(begin(new_node), end(new_node), node.prev);

		// give the middle key to the parent
		// note: middle key's child is reserved
		insert_key_to_index(node.parent, new_node.getKey(new_node.n - 1), 
							offset, node.prev);
	} else {
		insert_key_to_index_no_split(node, key, before);
		disk_write(&node, offset);
	}
}

void bplus_tree::insert_key_to_index_no_split(internalNode &node,
											  const polar_race::PolarString &key, off_t value)
{
	index *where = node.lower_bound(key);
	polar_race::PolarString lastKey = node.getKey(where->keyOff, where->keySize);
	if (lastKey.compare(key) >= 0) {
		std::copy_backward(where, end(node), end(node) + 1);
		node.insert_key(key, where);
		where->child = value;
	}
	else {
		node.append(key, value);
	}
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
	prev->id = meta.number++;
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
