//
//  LocalKVStore.cpp
//  centiman TPCC
//
//  A local Key-Value store used for standalone tests of the TPCC client code
//
//  Created by Alan Demers on 10/21/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <assert.h>

#include <tpcc/LocalKVStore.h>
#include <util/const.h>

namespace TPCC {
    
    LocalKVStore::Rep * LocalKVStore::theReps[LocalKVStore::MAX_STORES] = {};

    Seqnum LocalKVStore::theNextSeqnums[LocalKVStore::MAX_STORES] = {};

    LocalKVStore::LocalKVStore(unsigned which) {
        assert( which < MAX_STORES );
        this->which = which;
        if( theReps[which] == 0 ) theReps[which] = new Rep;
        pRep = theReps[which];
        pNextSeqnum = &(theNextSeqnums[which]);
    }
    
    LocalKVStore::LocalKVStore( LocalKVStore & kvs ) : which(kvs.which), pRep(kvs.pRep), pNextSeqnum(kvs.pNextSeqnum) {}

    LocalKVStore::~LocalKVStore() {}
    

    int LocalKVStore::get( const Key * apKey, Value * apValue, Seqnum * apSeqnum ) {
        assert(apValue != NULL);
        assert(apSeqnum != NULL);
        Rep::iterator it = pRep->find((K)(apKey));
        if( it != pRep->end() ) /* mapping exists */ {
            VObj * v = ((VObj *)( (*it).second ));
            assert( v != 0 );
            apValue->copyFrom( *v );
            *apSeqnum = v->sn;
            return 0;
        } else /* mapping does not exist */ {
            *apSeqnum = Const::SEQNUM_NULL;
            return -1;
        }
    }

    int LocalKVStore::put( const Key * apKey, Value * apValue, Seqnum aSeqnum ) {
        assert(apKey != NULL);
        assert(apValue != NULL);
        int ans = 0;
        VObj * v;
        Rep::iterator it = pRep->find((K)(apKey));
        if( it != pRep->end() ) /* mapping exists */ {
            v = (VObj *)((*it).second);
            assert( v != 0 );
            if( v->sn > aSeqnum ) { ans = 1;  goto Out; }
        } else /* mapping does not exist -- create one */ {
            v = new VObj;
            Key * k = new Key();  k->copyFrom(*apKey);
            std::pair<K, V> thePair(((K)(k)), ((V)(v)));
            (void)pRep->insert( thePair );
        }
        /* copy value from *apValue ... */
        v->copyFrom(*apValue);
        v->sn = aSeqnum;
    Out: ;
        return ans;
    }
    
};

