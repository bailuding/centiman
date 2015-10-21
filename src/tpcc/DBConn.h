//
//  DBConn.h
//  centiman TPCC
//
//  A client connection to a database
//
//  TODO: constructor needs more than the "which" parameter to identify the DB
//
//  Created by Alan Demers on 10/22/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__DBConn__
#define __TPCC__DBConn__

#include <vector>

#include <util/array.h>
#include <util/array-bytes.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/type.h>

#undef NET_STATS
#define NET_STATS

#include <util/stat.h>

namespace TPCC {

    /* opaque */ class LocalKVStore;
    
    class DBConn {
        int mId;

        LocalKVStore * pKVS;

        Queue<Packet> * mpInQueue;
        Queue<Packet> * mpOutQueue;
        
        int mCurrentTxnID;
        unsigned mTxnStepCnt;
        
        int mLastTxnID;
        
        Packet mXction;
        int mReadResultPos;
        int mLastReadPos;

        void reset();

        bool TRACE;
        FILE * mFile;

    public:
        Stat mStat;
        
#       ifdef NET_STATS
        struct Stats {
            uint32_t sentPackets;
            uint32_t recvdPackets;
            uint64_t sentPayload;
            uint64_t recvdPayload;
            Stats() : sentPackets(0), recvdPackets(0), sentPayload(0), recvdPayload(0) {}
        };
        Stats stats;
        void getStats(Stats * sp, bool clear = false) {
            sp->sentPackets = stats.sentPackets;  sp->recvdPackets = stats.recvdPackets;
            sp->sentPayload = stats.sentPayload;  sp->recvdPayload = stats.recvdPayload;
        }
        void clearStats() {
            stats.sentPackets = stats.recvdPackets = 0;
            stats.sentPayload = stats.recvdPayload = 0;
        }
#       endif
       
        
        /* set isolation level */
        void setIsolation(int pid)
        {
            mXction.mPid = pid;
        }
        
        /* set TRACE */
        void setTRACE(bool true4on)
        {
            TRACE = true4on;
        }

        /* set output file */
        void setFile(FILE * file = NULL) 
        {
            mFile = file;
        }

        /*
         * int beginTxn();
         *
         * begin a txn
         *   (if there is a current txn it aborts)
         * return new txn id
         */
        int beginTxn() {
            if( mTxnStepCnt > 0 ) (void)(this->abort());
            mCurrentTxnID = (mLastTxnID += 1);
            mXction.setStart();
            return mCurrentTxnID;
        }
        
        /*
         * int getCurrentTxn();
         *
         * return current txn id (0 if no txn is active)
         */
        int getCurrentTxn() { 
            return mCurrentTxnID;
        }
        
        /*
         * int requestRead( const Key * pKey )
         *
         * issue a read request for *pKey
         * return the ordinal of the read in the current txn
         * *** the key is copied to request key buffer ***
         */
        int requestRead( Key * pKey );
        
        /*
         * int getReadResult( Value ** ppValue = 0, Seqnum * pSeqnum = 0 );
         *
         * get the next read result associated with the current txn,
         *   waiting for it to be available.
         * return the ordinal of the read wrt the current txn
         * *** the caller receives the pointers of value and seqnum ***
         */
        int getReadResult( Value ** ppValue, Seqnum ** ppSeqnum );
        
        /*
         * int read( const Key * pKey, Value ** ppValue = 0, Seqnum * pSeqnum = 0 );
         *
         * request a read and immediately wait for the result
         */
        int read( Key * pKey, Value ** ppValue, Seqnum ** ppSeqnum ) {
            if( (int)mXction.mReadKeys.mCnt > mReadResultPos ) return (-1);
            int ans = requestRead( pKey );
            if( ans >= 0 ) ans = getReadResult( ppValue, ppSeqnum );
            return ans;
        }
        
        /*
         * int requestWrite( const Key * pKey, const Value * pValue );
         *
         * issue a write request
         * return ordinal of write in the current txn
         * *** the key and value are copied to the buffer in DBConn ***
         */
        int requestWrite( Key * pKey, Value * pValue );
        
        /*
         * int commit();
         * int abort();
         *
         * end the current txn
         * return DEC
         */
        int commit( Seqnum * pS = 0);
        int abort() {
            reset();  mCurrentTxnID = 0;  mTxnStepCnt = 0;
            return (-1);
        }
        
        DBConn(unsigned which = 0, int id = 0, Queue<Packet> * pInQueue = NULL, Queue<Packet> * pOutQueue = NULL);
        ~DBConn();
    };
    
};

#endif /* defined(__TPCC__DBConn__) */
