#ifndef LOGGER_H_
#define LOGGER_H_

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/engine.h"


#define CACHE_META_SIZE (1<<26)
// cache size = (1<<28)B = 256 MB
#define CACHE_NODE_SIZE (1<<28)


using polar_race::RetCode;

struct loggerMetaData {
	//size_t item_cnt; //how many log items
    time_t final_timestamp;
    off_t last_item_head; //where's the head of last item
};

struct loggerItemHead {
    size_t data_size;
    off_t last_head;
    time_t timestamp;
    unsigned int operation_id;
};

struct loggerItem {
    loggerItemHead head;
    off_t cache_begin;
    off_t cache_end;
};

enum NodeType{
    meta,
    inter,
    leaf,
};

class Logger {
    private:
		mutable FILE *data_file;
        mutable FILE *log_file;
        size_t meta_size;
        size_t inter_size;
        size_t leaf_size;
        bool recover();
    public:
        unsigned char cache_meta[CACHE_META_SIZE];
        unsigned char cache_inter[CACHE_NODE_SIZE];
        unsigned char cache_leaf[CACHE_NODE_SIZE];
        Logger() : data_file(nullptr), log_file(nullptr){}
        ~Logger() {
            if (data_file != nullptr)
                fclose(data_file);
            if (log_file != nullptr)
                fclose(log_file);
        }
        RetCode init(const char *data_path, const char *log_path, 
            size_t _meta_size, size_t _inter_size, size_t _leaf_size);
		// read block from disk

		template<class T>
		int read_node(T *block, NodeType type, int id, off_t offset) const;

        //读结点的一部分
        template<class T>
		int read_node(T *block, NodeType type, int id, off_t offset, 
            off_t offset_in_node, size_t length) const;

		// write block to disk
		int disk_write(const char *block, off_t offset, size_t size) const;

		int disk_write(void *block, off_t offset, size_t size) const;

		template<class T>
		int disk_write(T *block, off_t offset) const;

};

#endif