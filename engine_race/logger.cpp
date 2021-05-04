#include "logger.h"

RetCode Logger::init(const char *data_path, const char *log_path)
{
    data_file = fopen(data_path, "a+");
    log_file = fopen(log_path, "a+");
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
    return polar_race::kSucc;
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