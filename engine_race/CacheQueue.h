#ifndef CACHEQUEUE_H_
#define CACHEQUEUE_H_

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>

#define CACHE_SIZE (1<<28)

struct CacheNode
{
    int prev;
    int next;
    bool sync_with_disk; //是否与盘上数据同步(不同步指write操作未commit，这样的块不能被GC)
};


class CacheQueue {
    private:
        char cache[CACHE_SIZE];
        size_t item_size;
        int valid_cnt; //目前用到多少个缓存块
        int max_item_cnt;
        std::vector<CacheNode> nodes;
        CacheNode sync_head; //与盘上数据同步的链表表头
        CacheNode out_sync_head; //非同步结点的链表的表头

        inline off_t offset_of_node(int id) { return id * item_size; }
        
    public:
        CacheQueue(size_t item_size) : item_size(item_size) {
            valid_cnt = 0;
            max_item_cnt = CACHE_SIZE / item_size;
            nodes.resize(max_item_cnt);
            sync_head = (CacheNode){-1, -1, true};
            out_sync_head = (CacheNode){-1, -1, false};
        }
        CacheQueue(const CacheQueue&) = delete;
        CacheQueue(CacheQueue&&) = delete;
        //同步一个缓存结点
        //当结点 commit 时会调用
        //返回0表示正常, 返回-1表示操作失败
        int sync_node(int id);

        //获取指向结点对应cache处的指针
        char* get_node_ptr(int id);

        size_t get_item_size();
        
        //输出调试信息
        void print_node(int id);
        void print_all();

        int get_new_node(); //新增一个缓存结点

        //增加一个缓存结点，要求switch_node使用get_new_node()获得
        //且这两次调用应为原子操作
        //要求 T 的大小等于一开始传入的 item_size,实际执行中未做检查
        //返回-1表示缓存已满
        template<typename T>
        int add_node(T* data, bool is_sync, int switch_node)
        {
            int id = switch_node;
            //分配失败，则返回
            if(id == -1)
                return id;
            CacheNode& nd = nodes[id];
            CacheNode& head = is_sync? sync_head: out_sync_head;
            //插入表头
            nd = (CacheNode){-1, head.next, is_sync};
            CacheNode& old_head = head.next != -1? nodes[head.next]: head;
            old_head.prev = id;
            head.next = id;
            //写入缓存
            *((T*)(cache + offset_of_node(id))) = *data;
            
            return id;
        }

        //获取一个缓存结点,存入data
        template<typename T>
        void get_node(T* data, int id)
        {
            //从链表中删除
            CacheNode& nd = nodes[id];
            CacheNode& head = nd.sync_with_disk? sync_head: out_sync_head;
            CacheNode& prev_node = nd.prev != -1? nodes[nd.prev]: head;
            CacheNode& next_node = nd.next != -1? nodes[nd.next]: head;
            prev_node.next = nd.next;
            next_node.prev = nd.prev;
            //重新插入表头
            nd.prev = -1;
            nd.next = head.next;
            CacheNode& old_head = head.next != -1? nodes[head.next]: head;
            old_head.prev = id;
            head.next = id;
            *data = *(T*)(cache + offset_of_node(id));
        }
};

#endif