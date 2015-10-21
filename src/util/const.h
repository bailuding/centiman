#ifndef __UTIL_CONST_H__
#define __UTIL_CONST_H__

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <util/type.h>

class Const
{
    public:
        static void load(const char * filename);
        
        static const int MAX_NETWORK_FD = 1024;
        static const int MAX_STORAGE_NUM = 32;
        static const int MAX_VALIDATOR_NUM = 32;
        static const Seqnum SEQNUM_NULL = UINT64_MAX;
        static const int NETWORK_EPOLL_SIZE = 32;
        static const int NETWORK_MAX_EVENTS = 128;

        static int DB_SIZE;

        static int NETWORK_PROCESS_BATCH_NUM;
        static int NETWORK_TCP_NODELAY;
        static int NETWORK_BUF_SIZE;
        static int NETWORK_SEND_BATCH_SIZE;
        static int NETWORK_SWIPE_TIME_MS;

        static int UTIL_QUEUE_BATCH_SIZE;
        
        static int PROCESSOR_NUM;
        static int PROCESSOR_XCTION_QUEUE_SIZE;
        static int PROCESSOR_IN_QUEUE_SIZE;

        static int STORAGE_NUM;
        static int STORAGE_HASH_TABLE_SIZE;
        static int STORAGE_HASH_BUF_SIZE;
        static int STORAGE_IN_QUEUE_SIZE;
        static int STORAGE_OUT_QUEUE_SIZE;
        
        static int VALIDATOR_NUM;
        static int VALIDATOR_HASH_TABLE_SIZE;
        static int VALIDATOR_HASH_BUF_SIZE;
        static int VALIDATOR_REQUEST_BUF_SIZE;
        static int VALIDATOR_BUF_LV;
        static int VALIDATOR_IN_QUEUE_SIZE;
        static int VALIDATOR_OUT_QUEUE_SIZE;
       
        static int WATERMARK_FREQUENCY;

        static int XCTION_KEY_SIZE;
        static int XCTION_VALUE_SIZE;
        static int AVG_WRITE_CNT;

        static int CLIENT_RUNTIME;
        static int SERVER_RUNTIME;
        static int SHUTDOWN_TIME;

        static int NO_TIMER;
        static int LOG_LEVEL;
        static int NO_COUNTER_LIST;

        static int SLOW_TXN_FREQUENCY;
        static int SLOW_TXN_LAG;

        static int MODE_NO_VALID;
        static int MODE_NO_ABORT;
        static int MODE_NO_OP;
        static int MODE_SI;
        static int MODE_TPCC;
        static int MODE_TPCC_TRACE;
        static int MODE_TPCC_SI;
        static int MODE_FALSE_ABORT;

        static int TPCC_WAREHOUSE_NUM;
        static int TPCC_WORKER_NUM;

    private:

        static int get(FILE * file, char * attr, char * val);
        static int fill(char * attr, char * val);
        static int test(const char * attr, const char * val, const char * str, int * value);
        static int test(const char * attr, const char * val, const char * str, char * value);
        static void set();
};

#endif // __UTIL_CONST_H__
