//
//  DBtxnStockLevel.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 10/31/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <tpcc/Tables.h>
#include <tpcc/DB.h>
#include <unordered_set>

#include <util/const.h>

namespace TPCC {

    /*
     * TPCC Stock Level Transaction ...
     */
    
    int DB::txnStockLevel() {
        int ans;
        
        uint16_t theT_ID = terminal_id;
        uint16_t theW_ID = terminal_w_id;
        uint16_t theD_ID = dg.uniformInt(0, numDISTRICTs_per_WAREHOUSE);
        uint16_t threshold = dg.uniformInt(10, 21);
        
        /*
         * read DISTRICT row to get d_next_oid ...
         */

        Seqnum * pSeqnum = NULL;
/*        TblDISTRICT::Key * pkD = new TblDISTRICT::Key(theW_ID, theD_ID);
        TblDISTRICT::Row * prD = 0;
        ans = conn.read( pkD, ((::Value **)(&prD)), &pSeqnum );  pkD = 0;
        assert( !(prD->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );*/
        /* read DISTRICT_NEXT_O_ID table ... */
        TblDISTRICT_NEXT_O_ID::Key * pkD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Key(theW_ID, theD_ID, theT_ID);
        TblDISTRICT_NEXT_O_ID::Row * prD_NEXT_O_ID = 0;
        ans = conn.read( pkD_NEXT_O_ID, ((::Value **)(&prD_NEXT_O_ID)), &pSeqnum );  pkD_NEXT_O_ID = 0;
        assert( !(prD_NEXT_O_ID->isNULL(TblDISTRICT_NEXT_O_ID::Row::D_NEXT_O_ID)) );
        uint32_t d_next_oid = prD_NEXT_O_ID->getCol_uint32(TblDISTRICT_NEXT_O_ID::Row::D_NEXT_O_ID);
        
        /*
         * compute a set of ITEMs from the last 20 orders ...
         */
        std::unordered_set<int> i_ids;
        uint32_t first_oid = ( (d_next_oid >= 20) ? (d_next_oid - 20) : 0 );
        for( uint32_t o_id = first_oid; o_id < d_next_oid; o_id++ ) {
            /* get ol_cnt by reading ORDER row */
            TblORDER::Key * pkO = new TblORDER::Key(theW_ID, theD_ID, o_id, theT_ID);
            TblORDER::Row * prO = 0;
            ans = conn.read( pkO, ((::Value **)(&prO)), &pSeqnum );  pkO = 0;
            assert( !(prO->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
            assert( !(prO->isNULL(TblORDER::Row::O_OL_CNT)) );
            uint16_t ol_cnt = prO->getCol_uint16((::Key *)pkO, TblORDER::Row::O_OL_CNT);
            /* issue reads of ORDER_LINE rows */
            for( uint16_t oln = 0; oln < ol_cnt; oln++ ) {
                TblORDER_LINE::Key * pkOL = new TblORDER_LINE::Key(theW_ID, theD_ID, o_id, oln, theT_ID);
                ans = conn.requestRead(pkOL);  pkOL = 0;
            }
            /* get results and make an unordered set of item ids */
            TblORDER_LINE::Row * prOL = 0;
            for( uint16_t oln = 0; oln < ol_cnt; oln++ ) {
                ans = conn.getReadResult( ((::Value **)(&prOL)), &pSeqnum);
                if (ans == -1) {
                    fprintf(stdout, "first_oid %u d_next_oid %u oid %u ol_cnt %u oln %u\n", 
                            first_oid, d_next_oid, o_id, ol_cnt, oln); fflush(stdout);
                }
                assert(ans >= 0);
                assert( !(prOL->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
                assert( !(prOL->isNULL(TblORDER_LINE::Row::OL_I_ID)) );
                uint32_t i_id = prOL->getCol_uint32(TblORDER_LINE::Row::OL_I_ID);
                if( i_ids.count(i_id) == 0 ) {
                    (void) i_ids.insert( i_id );
                }
            }
        }
        
        /*
         * traverse the set of ITEMs and generate STOCK read requests ...
         */
        for( std::unordered_set<int>::const_iterator it = i_ids.cbegin(); it != i_ids.cend(); it++ ) {
            TblSTOCK::Key * pkS = new TblSTOCK::Key( theW_ID, *it );
            ans = conn.requestRead(pkS);  pkS = 0;
        }
        
        /*
         * get STOCK read results and compute low_stock ...
         */
        unsigned low_stock = 0;
        TblSTOCK::Row * prS = 0;
        for( int i = (int)(i_ids.size()); i > 0; --i ) {
            ans = conn.getReadResult( ((::Value **)(&prS)), &pSeqnum);
            assert( !(prS->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
            assert( !(prS->isNULL(TblSTOCK::Row::S_QUANTITY)) );
            uint16_t s_quantity = prS->getCol_uint16(NULL, TblSTOCK::Row::S_QUANTITY);
            if( s_quantity < threshold ) low_stock += 1;
        }
        ans = low_stock;
        
        return ans;
    }

};

