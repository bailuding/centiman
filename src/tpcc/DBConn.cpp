//
//  DBConn.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 10/22/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include "tpcc/DBConn.h"
#include "tpcc/LocalKVStore.h"

#define KVS (*pKVS)

#ifdef NET_STATS
#   define BYTES_FOR_PAYLOAD(p) ( (p)->mCnt + 8 )
#endif

namespace TPCC {
    
    DBConn::DBConn(unsigned which, int id, Queue<Packet> * pInQueue, Queue<Packet> * pOutQueue) :
            mId(id),
            mpInQueue(pInQueue), mpOutQueue(pOutQueue),
            mStat(),
#           ifdef NET_STATS
            stats(),
#           endif
            mCurrentTxnID(0), mTxnStepCnt(0), mLastTxnID(0), 
            mReadResultPos(0), mLastReadPos(0),
            TRACE(false)
    {
        pKVS = new LocalKVStore(which);
        mXction.reset();
    }

    void DBConn::reset() {
        mXction.reset();
        mReadResultPos = 0; mLastReadPos = 0;
    }
    
    DBConn::~DBConn() {
        reset();
    }
    
    int DBConn::requestRead( Key * pKey ) {
        if( mCurrentTxnID == 0) return (-1);
        mXction.mReadKeys.fillAndSet(mXction.mReadKeys.mCnt++, pKey->mVal, pKey->mCnt);
        mTxnStepCnt += 1;
#       ifdef NET_STATS
        {
            stats.sentPackets += 1;
            stats.sentPayload += 2 * BYTES_FOR_PAYLOAD(pKey); // read request and commit request
        }
#       endif
        return mXction.mReadKeys.mCnt;
    }

    int DBConn::getReadResult( Value ** ppValue, Seqnum ** ppSeqnum ){
        if( mCurrentTxnID == 0) return (-1);
        if( mReadResultPos >= (int)mXction.mReadKeys.mCnt ) return (-1);
        assert(ppValue != NULL);
        assert(ppSeqnum != NULL);
        if (mpInQueue == NULL) {
            /* local storage */
            /* batch read request */
            for (int i = mLastReadPos; i < (int)mXction.mReadKeys.mCnt; ++i) {
                Key * pKey = mXction.mReadKeys.get(i);
                Value * pValue = mXction.mReadValues.fillAndGet(i);
                Seqnum * pSeqnum = mXction.mReadSeqnums.fillAndGet(i);
                KVS.get(pKey, pValue, pSeqnum);
            }
            mLastReadPos = mXction.mReadKeys.mCnt;
        } else if (mLastReadPos < (int)mXction.mReadKeys.mCnt) {
            /* remote storage */
            int mPrevReadPos = mLastReadPos;
            Logger::out(2, "DBConn[%d]: start read %d %d\n", mId, mLastReadPos, mXction.mReadKeys.mCnt);
            while (mLastReadPos < (int)mXction.mReadKeys.mCnt) {
                /* request 20 records at most */
                int mCurrentRead = mLastReadPos + 20;
                if (mCurrentRead > (int)mXction.mReadKeys.mCnt) {
                    mCurrentRead = mXction.mReadKeys.mCnt;
                }

                /* prepare read request */
                Packet * out = mpOutQueue->getWriteSlot();

                /* check isolation level */
                if (mXction.mPid == PID_NULL || mXction.mPid == PID_SERIAL) {
                    /* serialization by default */
                    out->set(PID_READ_REQ, 0, mId);
                } else if (mXction.mPid == PID_SI) {
                    /* snapshot isolation */
                    out->set(PID_READ_SI_REQ, 0, mId);
                } else {
                    assert(0);
                }

                out->mReadCnt = mCurrentRead - mLastReadPos;
                out->mReadKeys.fill(out->mReadCnt);
                for (int i = mLastReadPos; i < mCurrentRead; ++i) {
                    Key * pKey = mXction.mReadKeys.get(i);
                    /* copy the key */
                    out->mReadKeys.set(i - mLastReadPos, pKey->mVal, pKey->mCnt);
                }
                mpOutQueue->putWriteSlot(out);
                Logger::out(2, "DBConn[%d]: send read request %d %d\n", mId, mLastReadPos, mCurrentRead);
                /* get read result */
                Packet * in = mpInQueue->getReadSlot();
                Logger::out(2, "DBConn[%d]: get read result %d %d\n", mId, mLastReadPos, mCurrentRead);
                assert(in->mReadCnt == mCurrentRead - mLastReadPos);
                mXction.mReadValues.fill(mCurrentRead);
                mXction.mReadSeqnums.fill(mCurrentRead);
                /* prepare read response */
                for (int i = mLastReadPos; i < mCurrentRead; ++i) {
                    /* swap the pointers to value */
                    Value * pValue = in->mReadValues.get(i - mLastReadPos);
                    Value * tmp = mXction.mReadValues.get(i);
                    mXction.mReadValues.set(i, pValue);
                    in->mReadValues.set(i - mLastReadPos, tmp);
                    Seqnum seqnum = *(in->mReadSeqnums.get(i - mLastReadPos));
                    mXction.mReadSeqnums.set(i, seqnum);
                }

                mpInQueue->putReadSlot(in);
                mLastReadPos = mCurrentRead;
            }
            Logger::out(2, "DBConn[%d]: finish read %d %d\n", mId, mPrevReadPos, mXction.mReadKeys.mCnt);
        }

        /* return result */
        *ppValue = mXction.mReadValues.get(mReadResultPos);
        *ppSeqnum = mXction.mReadSeqnums.get(mReadResultPos);
        mReadResultPos += 1;
#       ifdef NET_STATS
        {
            stats.recvdPackets += 1;
            stats.recvdPayload += BYTES_FOR_PAYLOAD(*ppValue) + sizeof(Seqnum);
        }
#       endif
        return ((*(*ppSeqnum) != Const::SEQNUM_NULL) ? (mReadResultPos-1) : -1);
    }
    
    int DBConn::requestWrite( Key *pKey, Value *pValue ) {
        if( mCurrentTxnID == 0) return (-1);
        assert(pValue != NULL);
        assert(pKey != NULL);
        mXction.mWriteKeys.fillAndSet(mXction.mWriteKeys.mCnt++, pKey->mVal, pKey->mCnt);
        mXction.mWriteValues.fillAndSet(mXction.mWriteValues.mCnt++, pValue->mVal, pValue->mCnt);
        return mXction.mWriteValues.mCnt - 1;
    }

    int DBConn::commit( Seqnum * pS ) {
        if( mCurrentTxnID == 0) {
            return (-1);
        }
        int dec = DEC_NULL;
        /* print all read keys, write keys, and write values */
        if (TRACE) {
            if (mFile) {
                /* readCnt writeCnt
                 * [readKey]
                 * [writeKey
                 * writeValueCnt]
                 */
                fprintf(mFile, "%d %d\n", (int)mXction.mReadKeys.mCnt, (int)mXction.mWriteKeys.mCnt);
                for (int i = 0; i < (int)mXction.mReadKeys.mCnt; ++i) {
                    mXction.mReadKeys.get(i)->print(mFile);
                }
                for (int i = 0; i < (int)mXction.mWriteKeys.mCnt; ++i) {
                    mXction.mWriteKeys.get(i)->print(mFile);
                    fprintf(mFile, "%d\n", mXction.mWriteValues.get(i)->mCnt);
                }
            }
        }

        if (mpInQueue == NULL) {
            Seqnum cmtsn = KVS.getSeqnum();
            if( pS ) *pS = cmtsn;
#       ifdef NET_STATS
            {
                stats.sentPackets += 1;   // commit request
                stats.recvdPackets += 1;  // commit response
            }
#       endif
            for( int r = 0; r < (int)mXction.mWriteKeys.mCnt; r++ ) {
                const Key * k = mXction.mWriteKeys.get(r);
                assert( k != 0 );
                Value * v = mXction.mWriteValues.get(r);
                assert( v != 0 );
#           ifdef NET_STATS
                {
                    stats.sentPayload += BYTES_FOR_PAYLOAD(k);
                    stats.sentPayload += BYTES_FOR_PAYLOAD(v);
                }
#           endif
                int ans = KVS.put( k, v, cmtsn );
                assert( ans >= 0 );
            }
            mXction.setEnd();
            mStat.latency(mXction.getLatency());
            mStat.update(DEC_COMMIT);
            dec = DEC_COMMIT;
        } else {
            /* prepare commit request */
            Packet * out = mpOutQueue->getWriteSlot();

            /* check isolation level */
            if (mXction.mPid == PID_NULL || mXction.mPid == PID_SERIAL) {
                /* serialization by default */
                out->set(PID_COMMIT_REQ, 0, mId);
            } else if (mXction.mPid == PID_SI) {
                out->set(PID_COMMIT_SI_REQ, 0, mId);
            } else {
                assert(0);
            }
            out->mReadCnt = mXction.mReadKeys.mCnt;
            out->mReadKeys.fill(out->mReadCnt);
            out->mReadSeqnums.fill(out->mReadCnt);
            for (int i = 0; i < (int)mXction.mReadKeys.mCnt; ++i) {
                /* copy keys */
                Key * pKey = mXction.mReadKeys.get(i);
                out->mReadKeys.set(i, pKey->mVal, pKey->mCnt);
                out->mReadSeqnums.set(i, *(mXction.mReadSeqnums.get(i)));
            }
            out->mWriteCnt = mXction.mWriteKeys.mCnt;
            out->mWriteKeys.fill(out->mWriteCnt);
            out->mWriteValues.fill(out->mWriteCnt);
            for (int i = 0; i < (int)mXction.mWriteKeys.mCnt; ++i) {
                /* copy keys */
                Key * pKey = mXction.mWriteKeys.get(i);
                out->mWriteKeys.set(i, pKey->mVal, pKey->mCnt);
                /* swap pointers to values */
                Value * pValue = mXction.mWriteValues.get(i);
                Value * tmp = out->mWriteValues.get(i);
                out->mWriteValues.set(i, pValue);
                mXction.mWriteValues.set(i, tmp);
            }
            mpOutQueue->putWriteSlot(out);
            Logger::out(2, "DBConn: send commit request\n");
            /* get commit response */
            Packet * in = mpInQueue->getReadSlot();
            Logger::out(2, "DBConn: get commit result\n");
            mXction.setEnd();
            mStat.latency(mXction.getLatency());
            mStat.update(in->mDec);
            dec = in->mDec;
            mpInQueue->putReadSlot(in);
        }
        reset();
        return dec;
    }

}; // TPCC
