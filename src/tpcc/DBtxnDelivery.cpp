//
//  DBtxnDelivery.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 11/1/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <tpcc/DB.h>
#include <tpcc/Tables.h>

#include <util/const.h>

namespace TPCC {
    
    /*
     * Delivery Transaction ...
     */
    int DB::txnDelivery() {
        int ans;

        uint16_t theW_ID = terminal_w_id;
        uint16_t theT_ID = terminal_id;

        /*
         * generate the input data ...
         */
        uint16_t theCARRIER_ID = dg.uniformInt(0, 10);  // 10 is the number of CARRIER_ID values
        
        /*
         * Do it ...
         */
        int num_districts_processed = 0;
        time_t now = time(0);
        
        Seqnum * pSeqnum = NULL;

        for( uint16_t d_id = 0;  d_id < numDISTRICTs_per_WAREHOUSE;  d_id++ ) {
            /*
             * get the oldest unprocessed order for this district ...
             */
            
            /* read from the NEW_ORDER_INDEX table ... */
            TblNEW_ORDER_INDEX::Key * pkNOX = new TblNEW_ORDER_INDEX::Key(theW_ID, d_id, theT_ID);
            TblNEW_ORDER_INDEX::Row * prNOX = 0;
            ans = conn.read( pkNOX, ((::Value **)(&prNOX)), &pSeqnum );  pkNOX = 0;
            uint32_t nox_o_id = 0;
            if( !(prNOX->isNULL() || *pSeqnum == Const::SEQNUM_NULL) ) {
                assert( !(prNOX->isNULL(TblNEW_ORDER_INDEX::Row::NOX_O_ID)) );
                nox_o_id = prNOX->getCol_uint32(TblNEW_ORDER_INDEX::Row::NOX_O_ID);
            } else {
                /* NULL row in NOX table means oldest O_ID is 0 or nonexistent */
            }
            
            /* read the NEW_ORDER row ... */
            TblNEW_ORDER::Key * pkNO = new TblNEW_ORDER::Key(theW_ID, d_id, nox_o_id, theT_ID);
            TblNEW_ORDER::Row * prNO = 0;
            ans = conn.read( pkNO, ((::Value **)(&prNO)), &pSeqnum );  pkNO = 0;
            if( prNO->isNULL() || *pSeqnum == Const::SEQNUM_NULL ) /* expected NEW_ORDER has not been placed yet ... */ {
                continue;
            }
            
            num_districts_processed += 1;
            
            /*
             * get data from ORDER ...
             */
            TblORDER::Key * pkO = new TblORDER::Key(theW_ID, d_id, nox_o_id, theT_ID);
            TblORDER::Row * prO = 0;
            ans = conn.read( pkO, ((::Value **)(&prO)), &pSeqnum );  pkO = 0;
            assert( !(prO->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
            assert( !(prO->isNULL(TblORDER::Row::O_C_ID)) );
            uint32_t o_c_id = prO->getCol_uint32(TblORDER::Row::O_C_ID);
            assert( !(prO->isNULL(TblORDER::Row::O_OL_CNT)) );
            uint16_t o_ol_cnt = prO->getCol_uint16((::Key *)pkO, TblORDER::Row::O_OL_CNT);
            
            /*
             * update ORDER ...
             */
            pkO = new TblORDER::Key(theW_ID, d_id, nox_o_id, theT_ID);
            prO->putCol_uint16(TblORDER::Row::O_CARRIER_ID, theCARRIER_ID);
            ans = conn.requestWrite(pkO, prO);  pkO = 0;  prO = 0;
            
            /*
             * issue reads to ORDER_LINE and CUSTOMER_PAYMENT table ...
             */
            TblORDER_LINE::Key * pkOL = 0;
            for( uint16_t ol = 0; ol < o_ol_cnt; ol++ ) {
                pkOL = new TblORDER_LINE::Key(theW_ID, d_id, nox_o_id, ol, theT_ID);
                ans = conn.requestRead(pkOL);  pkOL = 0;
            }
            TblCUSTOMER_PAYMENT::Key * pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(theW_ID, d_id, o_c_id);
            ans = conn.requestRead(pkC_PAYMENT);  pkC_PAYMENT = 0;
 /*           TblCUSTOMER::Key * pkC = new TblCUSTOMER::Key(theW_ID, d_id, o_c_id);
            ans = conn.requestRead(pkC);  pkC = 0;*/
            
            /*
             * get ORDER_LINE read results and update ORDER_LINE rows ...
             */
            double sum_ol_amts = 0.0;
            for( uint16_t ol = 0; ol < o_ol_cnt; ol++ ) {
                TblORDER_LINE::Row * prOL = 0;
                ans = conn.getReadResult( ((::Value **)(&prOL)), &pSeqnum);
                assert( !(prOL->isNULL() || *pSeqnum != Const::SEQNUM_NULL) );
                assert( !(prOL->isNULL(TblORDER_LINE::Row::OL_AMOUNT)) );
                double ol_amount = prOL->getCol_double(TblORDER_LINE::Row::OL_AMOUNT);
                sum_ol_amts += ol_amount;
                prOL->putCol_time(TblORDER_LINE::Row::OL_DELIVERY_D, now);
                pkOL = new TblORDER_LINE::Key(theW_ID, d_id, nox_o_id, ol, theT_ID);
                ans = conn.requestWrite(pkOL, prOL);  pkOL = 0;  prOL = 0;
            }
            
            /*
             * get CUSTOMER_PAYMENT read results and update CUSTOMER_PAYMENT row ...
             */
            TblCUSTOMER_PAYMENT::Row * prC_PAYMENT = 0;
            ans = conn.getReadResult( ((::Value **)(&prC_PAYMENT)), &pSeqnum) ;
            assert( !(prC_PAYMENT->isNULL() || *pSeqnum != Const::SEQNUM_NULL) );
            assert( !(prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_BALANCE)) );
            double c_balance = prC_PAYMENT->getCol_double(TblCUSTOMER_PAYMENT::Row::C_BALANCE);
            prC_PAYMENT->putCol_double(TblCUSTOMER_PAYMENT::Row::C_BALANCE, c_balance + sum_ol_amts);
            uint16_t c_delivery_cnt = 0;
            if( !(prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_DELIVERY_CNT)) ) {
                c_delivery_cnt = prC_PAYMENT->getCol_uint32(TblCUSTOMER_PAYMENT::Row::C_DELIVERY_CNT);
            }
            prC_PAYMENT->putCol_uint16(TblCUSTOMER_PAYMENT::Row::C_DELIVERY_CNT, c_delivery_cnt + 1);
            pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(theW_ID, d_id, o_c_id);
            ans = conn.requestWrite(pkC_PAYMENT, prC_PAYMENT);  pkC_PAYMENT = 0;  prC_PAYMENT = 0;
            
            /*
             * delete the NEW_ORDER row ...
             */
            pkNO = new TblNEW_ORDER::Key(theW_ID, d_id, nox_o_id, theT_ID);
            prNO = new TblNEW_ORDER::Row;
            ans = conn.requestWrite(pkNO, prNO);  pkNO = 0;  prNO = 0;
            /*
             * update the NEW_ORDER_INDEX row ...
             */
            pkNOX = new TblNEW_ORDER_INDEX::Key(theW_ID, d_id, theT_ID);
            prNOX = new TblNEW_ORDER_INDEX::Row;  prNOX->reset();
            prNOX->putCol_uint32( TblNEW_ORDER_INDEX::Row::NOX_O_ID, nox_o_id+1 );
            ans = conn.requestWrite(pkNOX, prNOX);  pkNOX = 0;  prNOX = 0;
        }
        ans = num_districts_processed;
        return ans;
    }
    
};

