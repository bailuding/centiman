/* util/const.h */
#include <util/const.h>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

#include <util/logger.h>

/* constant */
const int Const::MAX_STORAGE_NUM;
const int Const::MAX_VALIDATOR_NUM;
const int Const::MAX_NETWORK_FD;
const Seqnum Const::SEQNUM_NULL;
const int Const::NETWORK_EPOLL_SIZE;
const int Const::NETWORK_MAX_EVENTS;

int Const::DB_SIZE = 10007;

int Const::NETWORK_PROCESS_BATCH_NUM = 1;
int Const::NETWORK_TCP_NODELAY = 1;
int Const::NETWORK_BUF_SIZE = 1024;
int Const::NETWORK_SEND_BATCH_SIZE = 512;
int Const::NETWORK_SWIPE_TIME_MS = 100;

int Const::UTIL_QUEUE_BATCH_SIZE = 4;

int Const::PROCESSOR_NUM = 1;
int Const::PROCESSOR_XCTION_QUEUE_SIZE = 1024;
int Const::PROCESSOR_IN_QUEUE_SIZE = 1024;

int Const::STORAGE_NUM = 1;
int Const::STORAGE_HASH_TABLE_SIZE = 1024;
int Const::STORAGE_HASH_BUF_SIZE = 1024;
int Const::STORAGE_IN_QUEUE_SIZE = 1024;
int Const::STORAGE_OUT_QUEUE_SIZE = 1024;

int Const::VALIDATOR_NUM = 1;
int Const::VALIDATOR_HASH_TABLE_SIZE = 1024;
int Const::VALIDATOR_HASH_BUF_SIZE = 1024;
int Const::VALIDATOR_REQUEST_BUF_SIZE = 0;
int Const::VALIDATOR_BUF_LV = 1;
int Const::VALIDATOR_IN_QUEUE_SIZE = 1024;
int Const::VALIDATOR_OUT_QUEUE_SIZE = 1024;

int Const::XCTION_KEY_SIZE = 8;
int Const::XCTION_VALUE_SIZE = 8;
int Const::AVG_WRITE_CNT = 1;

int Const::WATERMARK_FREQUENCY = 1;

int Const::CLIENT_RUNTIME = 1;
int Const::SERVER_RUNTIME = 1;
int Const::SHUTDOWN_TIME = 1;

int Const::NO_TIMER = 1;
int Const::LOG_LEVEL = 0;
int Const::NO_COUNTER_LIST = 0;

int Const::SLOW_TXN_FREQUENCY = 0;
int Const::SLOW_TXN_LAG = 0;

int Const::MODE_NO_VALID = 0;
int Const::MODE_NO_ABORT = 0;
int Const::MODE_NO_OP = 0;
int Const::MODE_SI = 0;
int Const::MODE_TPCC = 0;
int Const::MODE_TPCC_TRACE = 0;
int Const::MODE_TPCC_SI = 0;
int Const::MODE_FALSE_ABORT = 0;

int Const::TPCC_WAREHOUSE_NUM = 1;
int Const::TPCC_WORKER_NUM = 1;



void Const::load(const char * filename)
{
    FILE * file = fopen(filename, "r");
    assert(file != NULL);

    char attr[128];
    char val[128];
    while (get(file, attr, val) == 2) {
        fill(attr, val);
    }
    fclose(file);
    set();
}

int Const::get(FILE * file, char * attr, char * val)
{
    return fscanf(file, "%[^=]%*c%s ", attr, val);
}

int Const::fill(char * attr, char * val)
{
    if (test(attr, val, "DB_SIZE", &DB_SIZE)) {
        return 0;
    } else if (test(attr, val, "NETWORK_PROCESS_BATCH_NUM", &NETWORK_PROCESS_BATCH_NUM)) {
        return 0;
    } else if (test(attr, val, "NETWORK_TCP_NODELAY", &NETWORK_TCP_NODELAY)) {
        return 0;
    } else if (test(attr, val, "NETWORK_BUF_SIZE", &NETWORK_BUF_SIZE)) {
        return 0;
    } else if (test(attr, val, "NETWORK_SEND_BATCH_SIZE", &NETWORK_SEND_BATCH_SIZE)) {
        return 0;
    } else if (test(attr, val, "NETWORK_SWIPE_TIME_MS", &NETWORK_SWIPE_TIME_MS)) {
        return 0;
    } else if (test(attr, val, "UTIL_QUEUE_BATCH_SIZE", &UTIL_QUEUE_BATCH_SIZE)) {
        return 0;
    } else if (test(attr, val, "PROCESSOR_NUM", &PROCESSOR_NUM)) {
        return 0;
    } else if (test(attr, val, "PROCESSOR_XCTION_QUEUE_SIZE", &PROCESSOR_XCTION_QUEUE_SIZE)) {
        return 0;
    } else if (test(attr, val, "STORAGE_NUM", &STORAGE_NUM)) {
        return 0;
    } else if (test(attr, val, "VALIDATOR_NUM", &VALIDATOR_NUM)) {
        return 0;
    } else if (test(attr, val, "VALIDATOR_BUF_LV", &VALIDATOR_BUF_LV)) {
        return 0;
    } else if (test(attr, val, "VALIDATOR_REQUEST_BUF_SIZE", &VALIDATOR_REQUEST_BUF_SIZE)) {
        return 0;
    } else if (test(attr, val, "XCTION_KEY_SIZE", &XCTION_KEY_SIZE)) {
        return 0;
    } else if (test(attr, val, "XCTION_VALUE_SIZE", &XCTION_VALUE_SIZE)) {
        return 0;
    } else if (test(attr, val, "AVG_WRITE_CNT", &AVG_WRITE_CNT)) {
        return 0;
    } else if (test(attr, val, "WATERMARK_FREQUENCY", &WATERMARK_FREQUENCY)) {
        return 0;
    } else if (test(attr, val, "CLIENT_RUNTIME", &CLIENT_RUNTIME)) {
        return 0;
    } else if (test(attr, val, "SERVER_RUNTIME", &SERVER_RUNTIME)) {
        return 0;
    } else if (test(attr, val, "SHUTDOWN_TIME", &SHUTDOWN_TIME)) {
        return 0;
    } else if (test(attr, val, "NO_TIMER", &NO_TIMER)) {
        return 0;
    } else if (test(attr, val, "NO_COUNTER_LIST", &NO_COUNTER_LIST)) {
        return 0;
    } else if (test(attr, val, "LOG_LEVEL", &LOG_LEVEL)) {
        return 0;
    } else if (test(attr, val, "SLOW_TXN_LAG", &SLOW_TXN_LAG)) {
        return 0;
    } else if (test(attr, val, "SLOW_TXN_FREQUENCY", &SLOW_TXN_FREQUENCY)) {
        return 0;
    } else if (test(attr, val, "MODE_NO_VALID", &MODE_NO_VALID)) {
        return 0;
    } else if (test(attr, val, "MODE_NO_ABORT", &MODE_NO_ABORT)) {
        return 0;
    } else if (test(attr, val, "MODE_NO_OP", &MODE_NO_OP)) {
        return 0;
    } else if (test(attr, val, "MODE_SI", &MODE_SI)) {
        return 0;
    } else if (test(attr, val, "MODE_TPCC", &MODE_TPCC)) {
        return 0;
    } else if (test(attr, val, "MODE_TPCC_TRACE", &MODE_TPCC_TRACE)) {
        return 0;
    } else if (test(attr, val, "MODE_TPCC_SI", &MODE_TPCC_SI)) {
        return 0;
    } else if (test(attr, val, "MODE_FALSE_ABORT", &MODE_FALSE_ABORT)) {
        return 0;
    } else if (test(attr, val, "TPCC_WAREHOUSE_NUM", &TPCC_WAREHOUSE_NUM)) {
        return 0;
    } else if (test(attr, val, "TPCC_WORKER_NUM", &TPCC_WORKER_NUM)) {
        return 0;
    }
    Logger::out(0, "Const: OPTION %s NOT RECOGNIZED\n", attr);
    exit(-1);
}

int Const::test(const char * attr, const char * val, const char * str,  int * value)
{
    if (strcmp(attr, str) == 0) {
        *(value) = atoi(val);
        Logger::out(1, "%s=%d\n", attr, *(value));
        return 1;
    }
    return 0;
}

int Const::test(const char * attr, const char * val, const char * str, char * value)
{
    if (strcmp(attr, str) == 0) {
        strcpy(value, val);
        Logger::out(1, "%s=%s\n", attr, value);
        return 1;
    }
    return 0;
}

void Const::set()
{
    STORAGE_HASH_TABLE_SIZE = DB_SIZE / STORAGE_NUM;
    VALIDATOR_HASH_TABLE_SIZE = DB_SIZE / VALIDATOR_NUM;

    int clientNum = 1;
    if (MODE_TPCC) {
        clientNum = TPCC_WORKER_NUM;
    }

    PROCESSOR_IN_QUEUE_SIZE = (VALIDATOR_NUM + STORAGE_NUM) * PROCESSOR_XCTION_QUEUE_SIZE * clientNum;

    STORAGE_IN_QUEUE_SIZE = PROCESSOR_NUM * PROCESSOR_XCTION_QUEUE_SIZE * clientNum;
    STORAGE_OUT_QUEUE_SIZE = STORAGE_IN_QUEUE_SIZE; 

    STORAGE_HASH_BUF_SIZE = DB_SIZE * AVG_WRITE_CNT / STORAGE_NUM * 2;

    // prevent SeqnumProcessor overwrites slots being processed by ValidatorWorker
    // set buf size automatically if it is not given by config
    if (VALIDATOR_REQUEST_BUF_SIZE == 0)
        VALIDATOR_REQUEST_BUF_SIZE = 2 * PROCESSOR_NUM * PROCESSOR_XCTION_QUEUE_SIZE * VALIDATOR_BUF_LV * clientNum; 
    VALIDATOR_IN_QUEUE_SIZE = VALIDATOR_REQUEST_BUF_SIZE;
    VALIDATOR_HASH_BUF_SIZE = VALIDATOR_REQUEST_BUF_SIZE * AVG_WRITE_CNT;
    VALIDATOR_OUT_QUEUE_SIZE = VALIDATOR_IN_QUEUE_SIZE;
}

