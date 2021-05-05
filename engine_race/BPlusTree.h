#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <limits>
#include <algorithm>
#include <mutex>

#include "include/polar_string.h"
#include "include/engine.h"

#include "latch.h"
#include <vector>

using polar_race::RetCode;

typedef long off_t;
typedef unsigned short s_off_t;

namespace b_plus_tree {

const size_t minKeyLength = 20;
const int poolSize = 2048;
//const int maxKeyLength = 256;
const int childSize = poolSize / minKeyLength;

/* meta data of B+ tree */
struct metaData{
	// std::atomic<size_t> internal_node_num;
	// std::atomic<size_t> leaf_node_num;     // how many leafs
	// std::atomic<size_t> height;            // height of tree (exclude leafs)
	// std::atomic<off_t> slot;        // where to store new block
	// std::atomic<off_t> root_offset; // where is the root of internal node
	// std::atomic<off_t> leaf_offset; // where is the last leaf node

	// std::atomic<int> number;		   // node count

	size_t internal_node_num; // how many internal nodes
	size_t leaf_node_num;     // how many leafs
	size_t height;            // height of tree (exclude leafs)
	off_t slot;        // where to store new block
	off_t root_offset; // where is the root of internal node
	off_t leaf_offset; // where is the last leaf node

	int number;		   // node count
};

/* internal nodes' index segment */
class index {
	public:
		off_t child; // child's offset
		s_off_t keyOff; // key's offset in pool
		s_off_t keySize;

		index() {
			//bzero(key, maxKeyLength);
		}
};

/* internal node block */
class internalNode {
	public:
		typedef index* child;
		
		off_t parent; // parent node offset
		off_t next;
		off_t prev;
		size_t n; // how many children

		int id;
		int status = 0;
		off_t slot;
		char pool[poolSize];
		index children[childSize];

		internalNode() {
			slot = 0;
			bzero(pool, poolSize);
		}

		index* lower_bound(const polar_race::PolarString &key) {
			int left = 0, right = this->n;
			while (left < right) {
				int mid = (left + right) >> 1;
				const polar_race::PolarString curKey = getKey(mid);
				if (curKey.compare(key) >= 0) {
					right = mid;
				}
				else {
					left = mid + 1;
				}
			}
			if (left >= this->n) {
				--left;
			}
			//printf("record next: %d %d\n", left, this->n);
			return &children[left];
		}

		polar_race::PolarString getKey(int Index) const {
			char *keyBlock = new char[children[Index].keySize + 1];
			bzero(keyBlock, children[Index].keySize + 1);
			strncpy(keyBlock, pool + children[Index].keyOff, children[Index].keySize);
			return polar_race::PolarString(keyBlock, children[Index].keySize);
		}

		polar_race::PolarString getKey(off_t keyOff, size_t keySize) const {
			char *keyBlock = new char[keySize + 1];
			bzero(keyBlock, keySize + 1);
			strncpy(keyBlock, pool + keyOff, keySize);
			return polar_race::PolarString(keyBlock, keySize);
		}

		void insert_key(const polar_race::PolarString &key, index *where) {
			strncpy(pool + slot, key.data(), key.size());
			where->keyOff = slot;
			where->keySize = key.size();
			slot += key.size();
			++n;
		}

		void append(const polar_race::PolarString &key, off_t child) {
			strncpy(pool + slot, key.data(), key.size());
			children[n].keyOff = slot;
			children[n].keySize = key.size();
			children[n].child = child;
			slot += key.size();
			++n;
		}
};

/* the record of value */
class record {
	public:
		off_t valueOff; // record's offset
		off_t valueSize;
		s_off_t keyOff; // key's offset in pool
		s_off_t keySize;

		record() {
			//bzero(key, maxKeyLength);
		}
};

/* leaf node block */
class leafNode {
	public:
		typedef record* child;

		off_t parent; // parent node offset
		off_t next;
		off_t prev;
		size_t n;

		int id;
		int status = 0;
		off_t slot;
		char pool[poolSize];
		record children[childSize];

		leafNode() {
			slot = 0;
			bzero(pool, poolSize);
		}

		record* lower_bound(const polar_race::PolarString &key) {
			int left = 0, right = this->n;
			while (left < right) {
				int mid = (left + right) >> 1;
				const polar_race::PolarString curKey = getKey(mid);
				if (curKey.compare(key) >= 0) {
					right = mid;
				}
				else {
					left = mid + 1;
				}
			}
			//printf("leaf next: %d %d\n", left, this->n);
			return &children[left];
		}

		polar_race::PolarString getKey(int Index) const {
			char *keyBlock = new char[children[Index].keySize + 1];
			bzero(keyBlock, children[Index].keySize + 1);
			strncpy(keyBlock, pool + children[Index].keyOff, children[Index].keySize);
			return polar_race::PolarString(keyBlock, children[Index].keySize);
		}

		polar_race::PolarString getKey(off_t keyOff, size_t keySize) const {
			char *keyBlock = new char[keySize + 1];
			bzero(keyBlock, keySize + 1);
			strncpy(keyBlock, pool + keyOff, keySize);
			return polar_race::PolarString(keyBlock, keySize);
		}

		void insert_key(const polar_race::PolarString &key, record *where) {
			strncpy(pool + slot, key.data(), key.size());
			where->keyOff = slot;
			where->keySize = key.size();
			slot += key.size();
			++n;
		}

		void append(const polar_race::PolarString &key, off_t valueOff, size_t valueSize) {
			strncpy(pool + slot, key.data(), key.size());
			children[n].keyOff = slot;
			children[n].keySize = key.size();
			children[n].valueOff = valueOff;
			children[n].valueSize = valueSize;
			slot += key.size();
			++n;
		}
};

//锁函数声明
void bplus_node_rlock(latch* lock);
void bplus_node_wlock(latch* lock);
void bplus_node_unlock(latch* lock);

const int OFFSET_META = 0;
const int OFFSET_BLOCK = sizeof(metaData);
const int SIZE_NO_CHILDREN = sizeof(leafNode) - childSize * sizeof(record) - poolSize * sizeof(char);

/* the encapulated B+ tree */
class bplus_tree {
	private:
		metaData meta;
		char path[512];
		std::vector<latch*> latchpool;
		std::mutex meta_mutex;

	public:
		bplus_tree(): fp(NULL), fp_level(0) {
			for(int i = 0; i < meta.number; i++)
				{
					latch* latch_insert = new latch;
					latch_init(latch_insert);
					latchpool.push_back(latch_insert);
				}
		}

		/* abstract operations */
		RetCode search(const polar_race::PolarString& key, std::string *value) const;
		RetCode search_range(const polar_race::PolarString &lower, 
							 const polar_race::PolarString &upper, polar_race::Visitor& visitor) const;
		
		RetCode insert_or_update(const polar_race::PolarString& key, polar_race::PolarString value);
		metaData getMeta() const {
			return meta;
		};

		/* init empty tree */
		RetCode init(const char *path);

		/* find index */
		off_t search_index(const polar_race::PolarString &key) const;

		/* find leaf */
		off_t search_leaf(off_t index, const polar_race::PolarString &key) const;
		off_t search_leaf(const polar_race::PolarString &key) const
		{
			return search_leaf(search_index(key), key);
		}

		/* insert into leaf without split */
		void insert_record_no_split(leafNode *leaf,
								const polar_race::PolarString &key, const polar_race::PolarString &value);

		/* add key to the internal node */
		void insert_key_to_index(off_t offset, const polar_race::PolarString &key,
								off_t value, off_t after);
		void insert_key_to_index_no_split(internalNode &node, const polar_race::PolarString &key,
										off_t value);

		/* change children's parent */
		void reset_index_children_parent(index *begin, index *end,
										off_t parent);

		template<class T>
		void node_create(off_t offset, T *node, T *prev);
	
		mutable FILE *fp;
		mutable int fp_level;
		RetCode open_file(const char *mode = "rb+") const
		{
			// `rb+` will make sure we can write everywhere without truncating file
			if (fp_level == 0)
				fp = fopen(path, mode);
			++fp_level;
			return fp < 0 ? polar_race::kIOError : polar_race::kSucc;
		}

		RetCode close_file() const
		{
			if (fp_level == 1)
				if (fclose(fp) != 0) {
					return polar_race::kIOError;
				}
			--fp_level;
			return polar_race::kSucc;
		}

		// alloc from disk
		off_t alloc(size_t size)
		{
			meta_mutex.lock();
			off_t slot = meta.slot;
			meta.slot += size;
			meta_mutex.unlock();
			return slot;
		}

		off_t alloc(leafNode *leaf)
		{
			meta_mutex.lock();
			leaf->n = 0;
			meta.leaf_node_num++;
			meta_mutex.unlock();
			return alloc(sizeof(leafNode));
		}

		off_t alloc(internalNode *node)
		{
			node->n = 0;
			meta_mutex.lock();
			meta.internal_node_num++;
			meta_mutex.unlock();
			return alloc(sizeof(internalNode));
		}

		void unalloc(leafNode *leaf, off_t offset)
		{
			meta_mutex.lock();
			--meta.leaf_node_num;
			meta_mutex.unlock();
		}

		void unalloc(internalNode *node, off_t offset)
		{
			meta_mutex.lock();
			--meta.internal_node_num;
			meta_mutex.unlock();
		}

		// read block from disk
		int disk_read(void *block, off_t offset, size_t size) const
		{
			open_file();
			fseek(fp, offset, SEEK_SET);
			size_t rd = fread(block, size, 1, fp);
			close_file();

			return rd - 1;
		}

		template<class T>
		int disk_read(T *block, off_t offset) const
		{
			return disk_read(block, offset, sizeof(T));
		}

		// write block to disk
		int disk_write(const char *block, off_t offset, size_t size) const
		{
			open_file();
			fseek(fp, offset, SEEK_SET);
			size_t wd = fwrite(block, size, 1, fp);
			close_file();

			return wd - 1;
		}

		int disk_write(void *block, off_t offset, size_t size) const
		{
			open_file();
			fseek(fp, offset, SEEK_SET);
			size_t wd = fwrite(block, size, 1, fp);
			close_file();

			return wd - 1;
		}

		template<class T>
		int disk_write(T *block, off_t offset) const
		{
			return disk_write(block, offset, sizeof(T));
		}

		// debug print
		template<class T>
		void node_printf(const T *node) const {
			printf("node size: %d node id: %d\n", node->n, node->id);
			printf("node status:%d\n",node->status);
			printf("node id:%d\n",node->id);
			for (int i = 0; i < node->n; i++) {
				printf("%s%c", node->getKey(i).data(), i == node->n - 1 ? '\n' : ' ');
			}
		}

		void tree_printf() {
			meta_mutex.lock();

			off_t org = meta.root_offset;
			int height = meta.height;
			internalNode node, nxtInternal;
			leafNode nxtLeafNode;
			disk_read(&node, org);
			printf("********************************\n");
			printf("internalCnt: %lld leafCnt: %lld\n", meta.internal_node_num, meta.leaf_node_num);
			while (height > 0) {
				disk_read(&nxtInternal, node.children[0].child);
				printf("--------------------------------\n");
				printf("height: %d\n", height);
				while (true) {
					node_printf(&node);
					if (node.next == 0) {
						break;
					}
					disk_read(&node, node.next);
				}
				node = nxtInternal;
				--height;
			}
			disk_read(&nxtLeafNode, meta.leaf_offset);
			printf("--------------------------------\n");
			printf("leaf nodes:\n");
			while (true) {
				node_printf(&nxtLeafNode);
				if (nxtLeafNode.prev == 0) {
					break;
				}
				disk_read(&nxtLeafNode, nxtLeafNode.prev);
			}
			printf("--------------------------------tree printf end\n");
			meta_mutex.unlock();
		}
};

// inline bool operator < (const record& x, const polar_race::PolarString& y) {
// 	return y.compare(x.key) > 0;
// }

// inline bool operator < (const polar_race::PolarString& x, const record& y) {
// 	return x.compare(y.key) < 0;
// }

// inline bool operator < (const index& x, const polar_race::PolarString& y) {
// 	return y.compare(x.key) > 0;
// }

// inline bool operator < (const polar_race::PolarString& x, const index& y) {
// 	return x.compare(y.key) < 0;
// }

}
#endif