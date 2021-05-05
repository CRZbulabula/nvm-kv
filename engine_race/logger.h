#ifndef LOGGER_H_
#define LOGGER_H_

#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_map>

#include "CacheQueue.h"

#include "include/engine.h"

using polar_race::RetCode;

enum NodeType{
    meta,
    inter,
    leaf,
};

//省点空间，不要 off_t 和 size_t 了
struct logItem {
    int offset; //数据文件中的偏移
    int length; //数据文件中的长度
    int timestamp; //写入的时间戳
    int node_id; //结点id
    int cache_id; //对应的cache的id
    NodeType type; //结点类型
};

//log文件最大 4MB
#define LOG_SIZE_LIMIT (1<<22)

//log函数的前四个字节标记log是否已提交
//如不正确说明log未提交，需要恢复
const int COMMITTED_CODE = 4396;
const int NOT_SUBMIT_CODE = 443;

typedef int TransactionId;
typedef std::vector<logItem> Transaction;

class Logger {
    private:
        pthread_mutex_t mu_; 
        unsigned int timestamp; //用操作时自增来模拟时间戳（真用time系统调用的话开销很大）
        std::vector<Transaction> transactions; //记录每个transaction的每次写操作在缓存里的id
        bool recover(int log_file_len);
        //三个缓存队列，分别存三种结点
        CacheQueue meta_que;
        CacheQueue inter_que;
        CacheQueue leaf_que;
        //B+树结点id->缓存块(如有多个，只记录最新)
        std::unordered_map<int, int> node2cache;
        //反向索引，用于删缓存时同时删上面的 map 中的信息
        std::unordered_map<int, int> cache2node[3];
        //缓存某个offset对应的结点id
        //如果查不到，说明没读写过这个块，那么强行把它从盘上读出来肯定是合理的
        std::unordered_map<int, int> off2node;
        std::unordered_map<int, int> latest_write_time; //最后一次写入

        // read block from disk
		int disk_read(void *block, off_t offset, size_t size);

		template<class T>
		int disk_read(T *block, off_t offset)
        {
            return disk_read(block, offset, sizeof(T));
        }

		int disk_write(void *block, off_t offset, size_t size);

		template<class T>
		int disk_write(T *block, off_t offset)
        {
            return disk_write(block, offset, sizeof(T));
        }
    public:
        FILE *data_file; //存数据本体的文件
        size_t data_file_size; //数据文件大小
        FILE *log_file; //存log的文件
        Logger(size_t _meta_size, size_t _inter_size, size_t _leaf_size)
            : mu_(PTHREAD_MUTEX_INITIALIZER),
            timestamp(0),
            meta_que(_meta_size), inter_que(_inter_size), leaf_que(_leaf_size),
            data_file(nullptr), log_file(nullptr)
            {

        }
        ~Logger() {
            if (data_file != nullptr)
                fclose(data_file);
            if (log_file != nullptr)
                fclose(log_file);
        }
        RetCode init(const char *data_path, const char *log_path);
        //新增写事务，需要在每次对B+树插入(可能要write若干个结点)时调用
        TransactionId open_transaction();
        //提交事务
        int commit_transaction(TransactionId tid);
        //强行写入文件
        void write_disk_raw(void *block, off_t offset, size_t size);
        //强行读出文件
        int read_disk_raw(void *block, off_t offset, size_t size);

        //读取结点，返回0为成功,返回-1为失败
		template<typename T>
		int read_node(T *block, NodeType type, int id, off_t offset)
        {
            pthread_mutex_lock(&mu_);
            //printf("read Type %d, node_id %d, offset %d\n", type, id, offset);
            CacheQueue& que = type == NodeType::meta? meta_que : (type == NodeType::inter? inter_que : leaf_que);
            //超出文件大小，返回-1，读入不成功
            if(offset + que.get_item_size() > data_file_size)
            {
                //printf("read fault\n");
                pthread_mutex_unlock(&mu_);
                return -1;
            }
            //约定Metadata的结点编号-1
            if(type == NodeType::meta)
                id = -1;
            int read_succ = -1;
            auto iter = node2cache.find(id);
            //已缓存，则直接从内存读取
            if(iter != node2cache.end() && iter->second != -1)
            {
                que.get_node(block, iter->second);
                //printf("get cache! (type %d) node_id %d -> cache_id %d\n", type, id, iter->second);
                read_succ = 0;
            }
            //否则尝试添加缓存
            else
            {
                read_succ = disk_read(block, offset);
                int switch_node = que.get_new_node();
                //记录全局缓存
                auto& c2n = cache2node[type];
                iter = c2n.find(switch_node);
                if(iter != c2n.end())
                {
                    //删去原来的缓存信息
                    //printf("remove cache  (type %d) node_id %d -> cache_id %d\n", type, iter->second, switch_node);
                    //只有当这个缓存没有再被iter->second使用时，才替换
                    if(node2cache[iter->second] == switch_node)
                        node2cache[iter->second] = -1;
                }
                int cache_id = que.add_node(block, true, switch_node);
                //printf("cache (type %d) node_id %d -> cache_id %d\n", type, id, cache_id);
                node2cache[id] = cache_id;
                c2n[cache_id] = id;
                //如果没有成功插入缓存，可以输出信息来报告。
                //非调试时不输出信息，这样退化为没有缓存直接硬盘读写，也能工作
                //注意“缺少缓存”是指每个现有缓存块都不能被替换
                
                //if(cache_id == -1)
                //{
                //    //printf("shortage of cache");
                //}
                //
            }
            pthread_mutex_unlock(&mu_);
            return read_succ;

        }

        //读取结点，返回0为成功,返回-1为失败
		template<typename T>
		int read_node(T *block, NodeType type, off_t offset)
        {
            pthread_mutex_lock(&mu_);
            auto iter = off2node.find(offset);
            //如果已缓存过这个点
            if(iter != off2node.end())
            {
                pthread_mutex_unlock(&mu_);
                return read_node(block, type, iter->second, offset);
            }
            //否则尝试添加缓存
            else
            {
                int read_succ = disk_read(block, offset);
                int node_id = block->id;
                CacheQueue& que = type == NodeType::meta? meta_que : (type == NodeType::inter? inter_que : leaf_que);
                int switch_node = que.get_new_node();
                //printf("read (force) Type %d, node_id %d, offset %d\n", type, node_id, offset);
                //记录全局缓存
                auto& c2n = cache2node[type];
                iter = c2n.find(switch_node);
                if(iter != c2n.end())
                {
                    //删去原来的缓存信息
                    //printf("remove cache  (type %d) node_id %d -> cache_id %d\n", type, iter->second, switch_node);
                    if(node2cache[iter->second] == switch_node)
                        node2cache[iter->second] = -1;
                }
                int cache_id = que.add_node(block, true, switch_node);
                //printf("cache (type %d) node_id %d -> cache_id %d\n", type, node_id, cache_id);
                node2cache[node_id] = cache_id;
                c2n[cache_id] = node_id;
                //更新offset缓存
                off2node[offset] = node_id;
                //如果没有成功插入缓存，可以输出信息来报告。
                //非调试时不输出信息，这样退化为没有缓存直接硬盘读写，也能工作
                //注意“缺少缓存”是指每个现有缓存块都不能被替换
                
                //if(cache_id == -1)
                //{
                //    //printf("shortage of cache");
                //}
                //
                pthread_mutex_unlock(&mu_);
                return read_succ;
            }

        }
        //写入结点，返回0为成功,返回-1为失败。写入失败时上层应用应报错
        template<typename T>
		int write_node(T *block, NodeType type, int node_id, off_t offset, TransactionId tid)
        {
            pthread_mutex_lock(&mu_);
            //printf("write Type %d, node_id %d, offset %d, tid %d\n", type, node_id, offset, tid);
            if(tid < 0 || tid >= transactions.size()) //非法写入
            {
                pthread_mutex_unlock(&mu_);
                return -1;
            }
            //时间戳自增
            ++timestamp;
            CacheQueue& que = type == NodeType::meta? meta_que : (type == NodeType::inter? inter_que : leaf_que);
            //约定Metadata的结点编号-1
            if(type == NodeType::meta)
                node_id = -1;
            
            int switch_node = que.get_new_node();
            
            //记录全局缓存
            auto& c2n = cache2node[type];
            auto iter = c2n.find(switch_node);
            if(iter != c2n.end())
            {
                //删去原来的缓存信息
                //printf("remove cache  (type %d) node_id %d -> cache_id %d\n", type, iter->second, switch_node);
                if(node2cache[iter->second] == switch_node)
                    node2cache[iter->second] = -1;
            }
            //写入缓存
            int cache_id = que.add_node(block, false, switch_node);
            //printf("cache (type %d) node_id %d -> cache_id %d\n", type, node_id, cache_id);
            node2cache[node_id] = cache_id;
            c2n[cache_id] = node_id;
            //更新offset缓存
            off2node[offset] = node_id;
            //记录进transaction
            transactions[tid].push_back((logItem){offset, que.get_item_size(), timestamp, node_id, cache_id, type});
            pthread_mutex_unlock(&mu_);
            return cache_id >= 0? 0 : -1;
        }

};

#endif