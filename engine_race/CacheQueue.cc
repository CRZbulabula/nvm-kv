#include "CacheQueue.h"

int CacheQueue::get_new_node()
{
    //有空位，则直接新增结点
    if (valid_cnt < max_item_cnt)
        return valid_cnt++;
    //如果没有和硬盘同步的结点，则无法分配
    //因为非同步结点都是没有commit的write操作，所以它们不能写到盘上
    if(sync_head.prev == -1)
        return -1;
    //按照LRU原则，取最后一个结点
    int result = sync_head.prev;
    //printf("switch node %d to reuse cache\n", result);
    sync_head.prev = nodes[sync_head.prev].prev;
    CacheNode& new_last = sync_head.prev != -1? nodes[sync_head.prev]: sync_head;
    new_last.next = -1;
    //这里result结点里的是脏数据，所以 get_new_node 需要是 private，否则外部直接调用会出错
    return result;
}

/*
template<typename T>
int CacheQueue::add_node(T* data, bool is_sync)
{
    int id = get_new_node();
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

template<typename T>
void CacheQueue::get_node(T* data, int id)
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
*/

int CacheQueue::sync_node(int id)
{
    CacheNode& nd = nodes[id];
    //结点已同步
    if(nd.sync_with_disk != false)
        return -1;
    nd.sync_with_disk = true;
    //从非同步表删除
    CacheNode& prev_node = nd.prev != -1? nodes[nd.prev]: out_sync_head;
    CacheNode& next_node = nd.next != -1? nodes[nd.next]: out_sync_head;
    prev_node.next = nd.next;
    next_node.prev = nd.prev;
    //插入同步表表头
    nd.prev = -1;
    nd.next = sync_head.next;
    CacheNode& old_head = sync_head.next != -1? nodes[sync_head.next]: sync_head;
    old_head.prev = id;
    sync_head.next = id;
    return 0;
}

char* CacheQueue::get_node_ptr(int id)
{
    return cache + offset_of_node(id);
}

size_t CacheQueue::get_item_size()
{
    return item_size;
}

void CacheQueue::print_node(int id)
{
    CacheNode& nd = nodes[id];
    printf("node %d: prev = %d, next = %d, sync = %d\n", id, nd.prev, nd.next, nd.sync_with_disk);
    printf("sync head: prev = %d, next = %d\n", sync_head.prev, sync_head.next);
    printf("out sync head: prev = %d, next = %d\n", out_sync_head.prev, out_sync_head.next);
}

void CacheQueue::print_all()
{
    printf("item_size %ld, valid_cnt %d, max_item_cnt %d\n", item_size, valid_cnt, max_item_cnt);
    for(int id = 0; id < valid_cnt; id++)
    {
        CacheNode& nd = nodes[id];
        printf("node %d: prev = %d, next = %d, sync = %d\n", id, nd.prev, nd.next, nd.sync_with_disk);
    }
    printf("sync head: prev = %d, next = %d\n", sync_head.prev, sync_head.next);
    printf("out sync head: prev = %d, next = %d\n", out_sync_head.prev, out_sync_head.next);
}