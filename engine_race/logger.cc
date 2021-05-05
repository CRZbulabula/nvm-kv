#include "logger.h"

/*
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
        
        //if(cache_id == -1)
        //{
        //    printf("shortage of cache");
        //}
        //
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
*/

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
    //printf("commit transaction %d\n", tid);
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
            //printf("add to log: offset %d, len %d\n", iter->offset, iter->length);
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
    
    //printf("log len = %d\n", log_len);
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
            //printf("write nodeid %d offset %d len %d to disk\n", iter->node_id, iter->offset, iter->length);
            //更新文件大小
            if(iter->offset + iter->length > data_file_size)
                data_file_size = iter->offset + iter->length;
        }
    }

    //释放transaction占用的空间
    //std::swap(tx, Transaction());
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

/*
template<class T>
int Logger::disk_read(T *block, off_t offset)
{
	return disk_read(block, offset, sizeof(T));
}
*/

int Logger::disk_write(void *block, off_t offset, size_t size)
{
	fseek(data_file, offset, SEEK_SET);
	size_t wd = fwrite(block, size, 1, data_file);
    if(offset + size > data_file_size)
        data_file_size = offset + size;
	return wd - 1;
}

/*
template<class T>
int Logger::disk_write(T *block, off_t offset)
{
	return disk_write(block, offset, sizeof(T));
}
*/

void Logger::write_disk_raw(void *block, off_t offset, size_t size)
{
    pthread_mutex_lock(&mu_);
    disk_write(block, offset, size);
    pthread_mutex_unlock(&mu_);
}

int Logger::read_disk_raw(void *block, off_t offset, size_t size)
{
    pthread_mutex_lock(&mu_);
    int result = disk_read(block, offset, size);
    pthread_mutex_unlock(&mu_);
    return result;
}

RetCode Logger::init(const char *data_path, const char *log_path)
{
    data_file = fopen(data_path, "r+");
    if(data_file == nullptr)
        data_file = fopen(data_path, "w+");
    else
        fseek(data_file, 0, SEEK_END);
    log_file = fopen(log_path, "r+");
    if(log_file == nullptr)
        log_file = fopen(log_path, "w+");
    else
        fseek(log_file, 0, SEEK_END);
    
    data_file_size = ftell(data_file);

    if(data_file < 0 || log_file < 0)
        return polar_race::kIOError;
    int commit_info = 0;
    int log_file_len = ftell(log_file);
    //如果log文件存在且够长
    if(log_file_len >= sizeof(commit_info))
    {
        fseek(log_file, 0, SEEK_SET);
        fread(&commit_info, sizeof(commit_info), 1, log_file);
        //如果前4个字节不是提交标记，则需要恢复
        if(commit_info != COMMITTED_CODE)
        {
            recover(log_file_len);
        }
    }
    //printf("init file size of data: %d / of log: %d\n", data_file_size, log_file_len);
    return ftell(data_file) > 0? polar_race::kSucc: polar_race::kNotFound;
}

template<typename T>
void read_from_log(char* log, int len, T* data)
{
    *data = *(T*)(log + len);
}

bool Logger::recover(int log_file_len)
{
    int log_len;
    static char log[LOG_SIZE_LIMIT];
    //读取log长度
    fseek(log_file, sizeof(int), SEEK_SET);
    fread(&log_len, sizeof(log_len), 1, log_file);
    //如果标示的log长度比实际要长，则log出错
    //注意标示的log长度可能比实际要短，
    //因为先写了某次长log再写一次短log，这时被覆盖掉的长log占用的文件空间不会被释放
    if(log_len > log_file_len)
        return false;
    //注意读log的时候，略去开头的提交码和log_len
    log_len -= 2 * sizeof(int);
    fread(&log_len, sizeof(char), log_len, log_file);
    int data_offset;
    int data_len;
    int len_now = 0;
    //直到读完所有log为止
    while(len_now < log_len)
    {
        //读出log中记录的offset和长度
        read_from_log(log, len_now, &data_offset);
        len_now += sizeof(data_offset);
        read_from_log(log, len_now, &data_len);
        len_now += sizeof(data_len);
        //将要恢复的数据写回data文件
        fseek(data_file, data_offset, SEEK_SET);
        fwrite(log + len_now, sizeof(char), data_len, data_file);
        len_now += data_len;
    }
    //结束时应该刚好读完，如果 len_now > log_len, 说明数据出错
    return len_now == log_len;
}
