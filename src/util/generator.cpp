/* util/generator.cpp */
#include <util/generator.h>

XctionTemplate Generator::mXctions[32];
int Generator::mSize;
Random Generator::mRand(getpid() ^ (pthread_self() + 1) ^ clock());

int Generator::getXctionIdx(int val)
{
    int tmp = 0;
    for (int i = 0; i < mSize; ++i) {
        tmp += mXctions[i].mRate;
        if (tmp >= val) {
            return i;
        }
    }
    return -1;
}

void Generator::load(const char * filename)
{
    FILE * file = fopen(filename, "r");
    char attr[128];
    char val[128];
    while (get(file, attr, val) == 2) {
        fill(attr, val);
    }
    fclose(file);

    for (int i = 0; i < mSize; ++i)
        mXctions[i].print();
}

int Generator::get(FILE * file, char * attr, char * val)
{
    return fscanf(file, "%[^=]%*c%s ", attr, val);
}

int Generator::fill(char * attr, char * val)
{
    static int idx = -1;
    if (test(attr, val, "NUM", &mSize)) {
        return 0;
    } else if (test(attr, val, "TYPE", &mXctions[idx + 1].mType)) {
        ++idx;
        return 0;
    } else if (test(attr, val, "RATE", &mXctions[idx].mRate)) {
        return 0;
    } else if (test(attr, val, "KEY_SIZE", &mXctions[idx].mKeySize)) {
        return 0;
    } else if (test(attr, val, "VALUE_SIZE", &mXctions[idx].mValueSize)) {
        return 0;
    } else if (test(attr, val, "READ_CNT", &mXctions[idx].mReadCnt)) {
        return 0;
    } else if (test(attr, val, "WRITE_CNT", &mXctions[idx].mWriteCnt)) {
        return 0;
    } else if (test(attr, val, "XCTION_SIZE", &mXctions[idx].mXctionSize)) {
        return 0;
    } else if (test(attr, val, "ITEM_UPDATE", &mXctions[idx].mItemUpdate)) {
        return 0;
    } else if (test(attr, val, "UPDATE", &mXctions[idx].mUpdate)) {
        return 0;
    } else if (test(attr, val, "ALPHA", &mXctions[idx].mAlpha)) {
        return 0;
    }
    Logger::out(0, "Generator: OPTION %s NOT RECOGNIZED\n", attr);
    exit(-1);
}

int Generator::test(const char * attr, const char * val, const char * str,  int * value)
{
    if (strcmp(attr, str) == 0) {
        *(value) = atoi(val);
        Logger::out(1, "%s=%d\n", attr, *(value));
        return 1;
    }
    return 0;
}

int Generator::test(const char * attr, const char * val, const char * str,  double * value)
{
    if (strcmp(attr, str) == 0) {
        *(value) = atof(val);
        Logger::out(1, "%s=%f\n", attr, *(value));
        return 1;
    }
    return 0;
}

int Generator::genPacketRand(Packet * tmp, int pid)
{
    static Zipfs zrand;

    tmp->mPid = pid;
    /* get xction template */
    int idx = getXctionIdx(mRand.rand() % 100 + 1);
    
    /* set read cnt and write cnt */
    if (mXctions[idx].mType == XID_UNIFORM) {
        tmp->mReadCnt = mXctions[idx].mReadCnt;
        tmp->mWriteCnt = mXctions[idx].mWriteCnt;
    } else if (mXctions[idx].mType == XID_SYN || mXctions[idx].mType == XID_ZIPFS) {
        int val = mRand.rand() % 100;
        if (val < mXctions[idx].mUpdate) {
            /* update */
            tmp->mWriteCnt = mXctions[idx].mXctionSize * mXctions[idx].mItemUpdate / 100;
            tmp->mReadCnt = mXctions[idx].mXctionSize - tmp->mWriteCnt;
        } else {
            /* read only */
            tmp->mReadCnt = mXctions[idx].mXctionSize;
            tmp->mWriteCnt = 0;
        }
        if (mXctions[idx].mType == XID_ZIPFS)
            mXctions[idx].mNum = 256;
    }

    if (pid == PID_NULL || pid == PID_READ_REQ || pid == PID_VALID_REQ || pid == PID_CLIENT_REQ) {
        tmp->mReadKeys.fill(tmp->mReadCnt);
        for (int i = 0; i < tmp->mReadCnt; ++i) {
            tmp->mReadKeys.get(i)->getRand(mXctions[idx].mKeySize, Const::DB_SIZE);
        }
        if (mXctions[idx].mType == XID_ZIPFS) {
            for (int i = 0; i < tmp->mReadCnt; ++i) {
                tmp->mReadKeys.get(i)->mVal[0] 
                    = zrand.next(mXctions[idx].mNum, mXctions[idx].mAlpha) - 1;
            }
        }
    }
    if (pid == PID_NULL || pid == PID_READ_RSP || pid == PID_VALID_REQ) {
        tmp->mReadSeqnums.fill(tmp->mReadCnt);
        for (size_t i = 0; i < tmp->mReadCnt; ++i) {
            tmp->mReadSeqnums.set(i, Seqnum(0));
        }
    }
    if (pid == PID_NULL || pid == PID_READ_RSP) {
        tmp->mReadValues.fill(tmp->mReadCnt);
        for (size_t i = 0; i < tmp->mReadCnt; ++i) {
            tmp->mReadValues.get(i)->setSize(mXctions[idx].mValueSize);
        }
    }
    if (pid == PID_NULL || pid == PID_VALID_REQ || pid == PID_WRITE_REQ || pid == PID_CLIENT_REQ) {
        tmp->mWriteKeys.fill(tmp->mWriteCnt);
        for (size_t i = 0; i < tmp->mWriteCnt; ++i) {
            tmp->mWriteKeys.get(i)->getRand(mXctions[idx].mKeySize, Const::DB_SIZE);
        }
        if (mXctions[idx].mType == XID_ZIPFS) {
            for (int i = 0; i < tmp->mWriteCnt; ++i) {
                tmp->mWriteKeys.get(i)->mVal[0] 
                    = zrand.next(mXctions[idx].mNum, mXctions[idx].mAlpha) - 1;
            }
        }
    }
    if (pid == PID_NULL || pid == PID_WRITE_REQ || pid == PID_CLIENT_REQ) {
        tmp->mWriteValues.fill(tmp->mWriteCnt);
        for (size_t i = 0; i < tmp->mWriteCnt; ++i) {
            tmp->mWriteValues.get(i)->setSize(mXctions[idx].mValueSize);
        }
    }
/*
    for (int i = 0; i < tmp->mReadCnt; ++i) {
        Logger::out(0, "gk %d ", tmp->mReadKeys.get(i)->toInt());
    }
    Logger::out(0, "\n");
  */
    return idx;
}

