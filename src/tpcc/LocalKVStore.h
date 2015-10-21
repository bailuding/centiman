//
//  LocalKVStore.h
//
//  An in-memory key-value store for testing
//  Simulates a connection to a server.
//  Multiple connections work.
//  NOT THREAD SAFE
//
//  Nothing is ever deleted -- deletions are simulated by setting entries to NULL
//  
//  TODO: THIS SHOULD BE MADE TO FREE MEMORY WHEN DELETED
//
//  Created by Alan Demers on 10/21/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__LocalKVStore__
#define __TPCC__LocalKVStore__

#include <stdint.h>
#include <unordered_map>

#include <util/type.h>


namespace TPCC {
    
    class LocalKVStore {

        static const unsigned MAX_STORES = 10;
        
    public:
        
        typedef uint64_t K;
        typedef uint64_t V;
        
        struct VObj : public Value {
            Seqnum sn;
            VObj() : Value(), sn(0) {}
            ~VObj() {}
        };
        
    private:
        struct KHash {
            size_t operator()( const K &k) const {
                size_t ans = 0;
                Key * p = (Key *)(k);
                if( p->mVal ) {
                    for( uint16_t pos = 0; pos < p->mCnt; pos++ ) {
                        ans = (ans << 1) ^ p->mVal[pos];
                    }
                }
                return ans;
            }
        };
        struct KEqual {
            bool operator()( const K &lhs, const K &rhs) const {
                return ((Key *)(lhs))->isEqual( *((Key *)(rhs)) );
            }
        };
        
        typedef std::unordered_map< K, V, KHash, KEqual > Rep;
        
        unsigned which;
        
        static Rep *(theReps[MAX_STORES]);
        Rep * pRep;

        static Seqnum theNextSeqnums[MAX_STORES];
        Seqnum * pNextSeqnum;

    public:
        int get( const Key * apKey, Value * apValue, Seqnum * apSeqnum );
            // get the stored value and seqNum associated with aKey
            // copy the stored value into *apValue using apValue->copyFrom(...)
            //   (if apValue is not 0)
            // put the stored seqnum into *apSeqnum (if apSeqnum is not 0)
            // return 0 if found, 1 if not found, (-1) on error
        
        int put( const Key * apKey, Value * apValue, Seqnum aSeqNum );
            // store *apValue associated with aKey
            //   (provided aSeqnum >= current seqnum)
            // copy the value of apValue and apKey
            // return 0 if value is inserted, 1 if stale, (-1) on error
        
        Seqnum getSeqnum() { Seqnum ans = (*pNextSeqnum);  (*pNextSeqnum) += 1;  return ans; }

        LocalKVStore(unsigned which = 0);
        LocalKVStore( LocalKVStore & kvs );
        ~LocalKVStore();
    };
    
}; // TPCC
    

#endif /* defined(__TPCC__LocalKVStore__) */
