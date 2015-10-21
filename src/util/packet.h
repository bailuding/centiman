#ifndef __UTIL_PACKET_H__
#define __UTIL_PACKET_H__

#include <atomic>
#include <pthread.h>
#include <stdint.h>
#include <sys/time.h>
#include <vector>

#include "util/timer.h"
#include <util/array.h>
#include <util/dec.h>
#include <util/logger.h>
#include <util/packet-id.h>
#include <util/type.h>

class Packet
{
    public:
        Packet();
        ~Packet();
        void set(int, int, int);
        void reset();
        void clean();

        // latency
        long getLap(int idx);

        void setStart()
        {
            gettimeofday(&mStart, NULL);
        }
        void setEnd()
        {
            gettimeofday(&mEnd, NULL);
        }
        int getLatency();

        static void getReadReq(Packet * req, Packet * xction);
        static void getWriteReq(Packet * req, Packet * xction);
        static void getCommitReq(Packet * req, Packet * xction);
        static void getReadRsp(Packet * req, Packet * xction);

        void addValidReqRead(Key * key, Seqnum seqnum);
        void addValidReqWrite(Key * key);

        void print();

        void marshal(uint8_t * buf, int * pCnt);
        int marshalSize();
        void unmarshal(uint8_t * buf, int cnt);

        void padding(uint16_t & idx)
        {
            idx += idx & 1;
        }
        
        int mType;

        int mFirst;
        int mState;

        timeval mStart;
        timeval mEnd;

        int mSfd;
        int mXid;
        int mPid;
        int sid; // server id

        Seqnum mSeqnum;
        Seqnum mWaterMark;
        Seqnum mReadSeqnum; // read snapshot

        int mDec;
        int mFirstDec;
        int mDiffDec;

        XctionSize mReadCnt;
        ArrayKey mReadKeys;
        ArrayValue mReadValues;
        Array<Seqnum> mReadSeqnums;
        Array<XctionSize> mReadIdx;
        
        XctionSize mWriteCnt;
        ArrayKey mWriteKeys;
        ArrayValue mWriteValues;

        ArrayKey * mpDelKeys;
        int mReuse;
        Seqnum prevSeq;

        Packet * mpOutPacket;

        // latency
        Timer timer;
};

inline
Packet::Packet()
{
    reset();

    timer.resize(2);
    mpDelKeys = NULL;
}

inline
Packet::~Packet()
{
}

inline
void Packet::reset()
{
    mState = 0;
    
    mSfd = 0;
    mSeqnum = Const::SEQNUM_NULL;
    mWaterMark = Const::SEQNUM_NULL;
    mReadSeqnum = Const::SEQNUM_NULL;
    mXid = 0;
    mPid = PID_NULL;
    sid = 0;

    mReadCnt = 0;
    mWriteCnt = 0;
    mReadKeys.mCnt = 0;
    mReadValues.mCnt = 0;
    mWriteKeys.mCnt = 0;
    mWriteValues.mCnt = 0;

    mDec = DEC_COMMIT;
    mFirstDec = DEC_FULL;
    mDiffDec = 0;
    mFirst = 1;

    timer.reset();
}

inline
void Packet::clean()
{
    // Holder for Queue
}

inline
long Packet::getLap(int idx)
{
    return timer.lap(idx);
}

#endif
