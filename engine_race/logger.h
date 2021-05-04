#ifndef LOGGER_H_
#define LOGGER_H_

#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <unordered_map>

#include "CacheQueue.h"

//#include "include/engine.h"
#include "../include/engine.h"

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

        size_t data_file_size; //数据文件大小
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
        std::unordered_map<int, int> cache2node;
        std::unordered_map<int, int> latest_write_time; //最后一次写入
    public:
        FILE *data_file; //存数据本体的文件
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
        //读取结点，返回0为成功,返回-1为失败
		template<typename T>
		int read_node(T *block, NodeType type, int id, off_t offset);
        //写入结点，返回0为成功,返回-1为失败。写入失败时上层应用应报错
        template<typename T>
		int write_node(T *block, NodeType type, int node_id, off_t offset, TransactionId tid);
        //新增写事务，需要在每次对B+树插入(可能要write若干个结点)时调用
        TransactionId open_transaction();
        //提交事务
        int commit_transaction(TransactionId tid);

		// read block from disk
		int disk_read(void *block, off_t offset, size_t size);

		template<class T>
		int disk_read(T *block, off_t offset);

		int disk_write(void *block, off_t offset, size_t size);

		template<class T>
		int disk_write(T *block, off_t offset);

};

template<typename T>
int Logger::read_node(T *block, NodeType type, int id, off_t offset)
{
    pthread_mutex_lock(&mu_);
    CacheQueue& que = type == NodeType::meta? meta_que : (type == NodeType::inter? inter_que : leaf_que);
    //约定Metadata的结点编号-1
    if(type == NodeType::meta)
        id = -1;
    int read_succ = -1;
    auto iter = node2cache.find(id);
    //已缓存，则直接从内存读取
    if(iter != node2cache.end() && iter->second != -1)
    {
        que.get_node(block, iter->second);
        read_succ = 0;
    }
    //否则尝试添加缓存
    else
    {
        read_succ = disk_read(block, offset);
        int cache_id = que.add_node(block, true);
        //记录全局缓存
        iter = cache2node.find(cache_id);
        if(iter != cache2node.end())
        {
            //删去原来的缓存信息
            node2cache[iter->second] = -1;
        }
        node2cache[id] = cache_id;
        cache2node[cache_id] = id;
        //如果没有成功插入缓存，可以输出信息来报告。
        //非调试时不输出信息，这样退化为没有缓存直接硬盘读写，也能工作
        //注意“缺少缓存”是指每个现有缓存块都不能被替换
        /*
        if(cache_id == -1)
        {
            printf("shortage of cache");
        }
        */
    }
    pthread_mutex_unlock(&mu_);
    return read_succ;
}

template<typename T>
int Logger::write_node(T *block, NodeType type, int node_id, off_t offset, TransactionId tid)
{
    pthread_mutex_lock(&mu_);
    if(tid < 0 || tid >= transactions.size()) //非法写入
        return -1;
    //时间戳自增
    ++timestamp;
    CacheQueue& que = type == NodeType::meta? meta_que : (type == NodeType::inter? inter_que : leaf_que);
    //约定Metadata的结点编号-1
    if(type == NodeType::meta)
        node_id = -1;
    //写入缓存
    int cache_id = que.add_node(block, false);
    //记录全局缓存
    auto iter = cache2node.find(cache_id);
    if(iter != cache2node.end())
    {
        //删去原来的缓存信息
        node2cache[iter->second] = -1;
    }
    node2cache[node_id] = cache_id;
    cache2node[cache_id] = node_id;
    //记录进transaction
    transactions[tid].push_back((logItem){offset, que.get_item_size(), timestamp, node_id, cache_id, type});
    pthread_mutex_unlock(&mu_);
    return cache_id >= 0? 0 : -1;
}

TransactionId Logger::open_transaction()
{
    pthread_mutex_lock(&mu_);
    transactions.push_back(Transaction());
    int tid = transactions.size() - 1;
    pthread_mutex_unlock(&mu_);
    return (TransactionId)tid;
}

template<typename T>
void add_to_log(char* log, int& len, T* data)
{
    strncpy(log + len, (char*)data, sizeof(T));
    len += sizeof(T);
}

void add_to_log(char* log, int& len, const char* data, int data_len)
{
    strncpy(log + len, data, data_len);
    len += data_len;
}

int Logger::commit_transaction(TransactionId tid)
{
    pthread_mutex_lock(&mu_);
    static char log[LOG_SIZE_LIMIT];
    int log_len = 0;
    add_to_log(log, log_len, &NOT_SUBMIT_CODE);
    //这个位置存log_len，需要后续计算才知道，所以先保留位置
    log_len += sizeof(int);
    Transaction& tx = transactions[tid];
    for(auto iter = tx.begin(); iter != tx.end(); iter++)
    {
        //获得同结点上次写入的时间
        auto time_iter = latest_write_time.find(iter->node_id);
        int last_write = (time_iter == latest_write_time.end())? -1: time_iter->second;
        CacheQueue& que = iter->type == NodeType::meta? meta_que : (iter->type == NodeType::inter? inter_que : leaf_que);
        //如果硬盘上没有理应覆盖这个操作的写入
        if(last_write < iter->timestamp)
        {
            //记录该结点最后一次磁盘写对应的内存时间
            latest_write_time[iter->node_id] = iter->timestamp;
            //log中记录元信息，要写入的位置和长度
            add_to_log(log, log_len, &iter->offset);
            add_to_log(log, log_len, &iter->length);
            //再写入数据本身
            char* data = que.get_node_ptr(iter->cache_id);
            add_to_log(log, log_len, data, iter->length);
        }
        else //否则为过期信息，不需要commit。这里标记方便后续更新的时候跳过
        {
            iter->timestamp = -1;    
        }
        //标记结点为与磁盘同步，即可以从缓存中被删除
        que.sync_node(iter->cache_id);
    }
    //最后写入 log_len
    strncpy(log + sizeof(NOT_SUBMIT_CODE), (char*)(&log_len), sizeof(log_len));
    //把log写入文件
    fseek(log_file, 0, SEEK_SET);
    fwrite(log, sizeof(char), log_len, log_file);

    //写入文件
    for(auto iter = tx.begin(); iter != tx.end(); iter++)
    {
        CacheQueue& que = iter->type == NodeType::meta? meta_que : (iter->type == NodeType::inter? inter_que : leaf_que);
        //如果上面记录log时没有标记过这个结点
        if(iter->timestamp != -1)
        {
            //记录该结点最后一次磁盘写对应的内存时间
            latest_write_time[iter->node_id] = iter->timestamp;
            //log中记录元信息，要写入的位置和长度
            add_to_log(log, log_len, &iter->offset);
            add_to_log(log, log_len, &iter->length);
            //再写入数据本身
            char* data = que.get_node_ptr(iter->cache_id);
            //如果数据文件长度小于 offset
            //注意，如果只是新增结点，则两者相等，不会触发这一句
            //只有后增加的结点比先增加的结点早commit才会触发
            if(data_file_size < iter->offset)
            {
                fseek(data_file, data_file_size, SEEK_SET);
                //就只好先写入一些垃圾数据了，不过不会有结点指向这些数据
                fwrite(log, sizeof(char), iter->offset - data_file_size, data_file);
                data_file_size = iter->offset;
            }
            else //否则直接指向要写的位置
            {
                fseek(data_file, iter->offset, SEEK_SET);
            }
            fwrite(data, sizeof(char), iter->length, data_file);
        }
    }

    //标记log为完成状态，这样它不会被恢复
    fseek(log_file, 0, SEEK_SET);
    fwrite(&COMMITTED_CODE, sizeof(COMMITTED_CODE), 1, log_file);
    pthread_mutex_unlock(&mu_);
    return 0;
}

int Logger::disk_read(void *block, off_t offset, size_t size) 
{
	fseek(data_file, offset, SEEK_SET);
	size_t rd = fread(block, size, 1, data_file);

	return rd - 1;
}

template<class T>
int Logger::disk_read(T *block, off_t offset)
{
	return disk_read(block, offset, sizeof(T));
}

int Logger::disk_write(void *block, off_t offset, size_t size)
{
	fseek(data_file, offset, SEEK_SET);
	size_t wd = fwrite(block, size, 1, data_file);

	return wd - 1;
}

template<class T>
int Logger::disk_write(T *block, off_t offset)
{
	return disk_write(block, offset, sizeof(T));
}
#endif