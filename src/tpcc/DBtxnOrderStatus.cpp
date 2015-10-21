//
//  DBtxnOrderStatus.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 11/1/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <tpcc/Tables.h>
#include <tpcc/DB.h>

#include <util/const.h>

namespace TPCC {
    
    /*
     * Order Status Transaction ...
     */
    int DB::txnOrderStatus() {
        int ans;
        
        uint16_t theW_ID = terminal_w_id;
        uint16_t theT_ID = terminal_id;
        
        /*
         * generate the input data ...
         */
        uint16_t theD_ID = dg.uniformInt(0, numDISTRICTs_per_WAREHOUSE);
        uint32_t c_id;
        char c_last[16];
        uint16_t c_last_len = 0;
        if( dg.flip(0.6) ) /* customer by last name */ {
//            c_last_len = dg.randomCustomerName(c_last, (sizeof c_last));
            c_last_len = dg.randomCustomerName(c_last, dg.uniformInt(0, numCUSTOMERs_per_DISTRICT));
            assert( c_last_len > 0 );
        } else /* customer by C_ID */ {
            c_id = dg.NURand(dg.A_for_C_LAST, 1, numCUSTOMERs_per_DISTRICT, dg.C_for_C_LAST) - 1; // [1..3000] - 1
        }
        
        /*
         * resolve customer ID ...
         */
        Seqnum * pSeqnum = NULL;
        if( c_last_len > 0 ) /* customer specified by last name */ {
            /* get c_id using TblCUSTOMER_INDEX ... */
            TblCUSTOMER_INDEX::Key * pkCX = new TblCUSTOMER_INDEX::Key(theW_ID, theD_ID, (const uint8_t *)(c_last), c_last_len);
            TblCUSTOMER_INDEX::Row * prCX = 0;
            ans = conn.read( pkCX, ((::Value **)(&prCX)), &pSeqnum );  pkCX = 0;
            if( (prCX->isNULL() || *pSeqnum == Const::SEQNUM_NULL) ) /* randomly generated name is not in CUSTOMER table ... */ {
                return (-1);
            }
            assert( !prCX->isNULL(TblCUSTOMER_INDEX::Row::CX_C_NID) );
            uint16_t cx_c = prCX->getCol_uint16((::Key *)prCX, TblCUSTOMER_INDEX::Row::CX_C_NID);
            assert( cx_c >= 1 );
            cx_c = TblCUSTOMER_INDEX::Row::CX_C_ID_0 + (cx_c/2);
            assert( !prCX->isNULL( cx_c ) );
            c_id = prCX->getCol_uint32( cx_c );
        }
        
        /*
         * get CUSTOMER and ORDER_INDEX data ...
         */

        /* CUSTOMER table ... */
        TblCUSTOMER::Key * pkC = new TblCUSTOMER::Key(theW_ID, theD_ID, c_id);
        ans = conn.requestRead(pkC);  pkC = 0;
        /* CUSTOMER_PAYMENT table ... */
        TblCUSTOMER_PAYMENT::Key * pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(theW_ID, theD_ID, c_id);
        ans = conn.requestRead(pkC_PAYMENT);  pkC_PAYMENT = 0;
        
        /* ORDER_INDEX table ... */
        TblORDER_INDEX::Key * pkOX = new TblORDER_INDEX::Key(theW_ID, theD_ID, c_id, theT_ID);
        ans = conn.requestRead(pkOX);  pkOX = 0;
        
        /* CUSTOMER table result ... */
        TblCUSTOMER::Row * prC = 0;
        ans = conn.getReadResult( ((::Value **)(&prC)), &pSeqnum );
        assert( !(prC->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
        assert( !(prC->isNULL(TblCUSTOMER::Row::C_FIRST)) );
        char * c_fst = prC->getCol_cString(TblCUSTOMER::Row::C_FIRST);
        assert( !(prC->isNULL(TblCUSTOMER::Row::C_MIDDLE)) );
        char * c_mdl = prC->getCol_cString(TblCUSTOMER::Row::C_MIDDLE);
        assert( !(prC->isNULL(TblCUSTOMER::Row::C_LAST)) );
        char * c_lst = prC->getCol_cString(TblCUSTOMER::Row::C_LAST);
        /* CUSTOMER_PAYMENT table result ... */
        TblCUSTOMER_PAYMENT::Row * prC_PAYMENT = 0;
        ans = conn.getReadResult( ((::Value **)(&prC_PAYMENT)), &pSeqnum );
        assert( !(prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_BALANCE)) );
        double c_balance = prC_PAYMENT->getCol_double(TblCUSTOMER_PAYMENT::Row::C_BALANCE);
        
        /* ORDER_INDEX table result ... */
        TblORDER_INDEX::Row * prOX = 0;
        ans = conn.getReadResult( ((::Value **)(&prOX)), &pSeqnum);
        if( (prOX->isNULL() || *pSeqnum == Const::SEQNUM_NULL) ) {
            assert(0); // TODO: ??? is this possible?
        }
        assert( !(prOX->isNULL(TblORDER_INDEX::Row::OX_O_ID)) );
        uint32_t ox_o_id = prOX->getCol_uint32(TblORDER_INDEX::Row::OX_O_ID);
        
        /*
         * get the ORDER ...
         */
        TblORDER::Key * pkO = new TblORDER::Key(theW_ID, theD_ID, ox_o_id, theT_ID);
        TblORDER::Row * prO = 0;
        ans = conn.read( pkO, ((::Value **)(&prO)), &pSeqnum );  pkO = 0;
        assert( !(prO->isNULL() || *pSeqnum == Const::SEQNUM_NULL) ); // orders are never deleted
        assert( !(prO->isNULL(TblORDER::Row::O_ENTRY_D)) );
        time_t o_entry_d = prO->getCol_time(TblORDER::Row::O_ENTRY_D);
        uint16_t o_carrier_id = ( prO->isNULL(TblORDER::Row::O_CARRIER_ID) ? ((uint16_t)(-1))
                                 : prO->getCol_uint16((::Key *)pkO, TblORDER::Row::O_CARRIER_ID) );
        assert( !(prO->isNULL(TblORDER::Row::O_OL_CNT)) );
        uint16_t o_ol_cnt = prO->getCol_uint16((::Key *)pkO, TblORDER::Row::O_OL_CNT);
        
        /*
         * get data for ORDER_LINEs ...
         */
        for( uint16_t ol = 0; ol < o_ol_cnt; ol++ ) {
            TblORDER_LINE::Key * pkOL = new TblORDER_LINE::Key( theW_ID, theD_ID, ox_o_id, ol, theT_ID );
            ans = conn.requestRead(pkOL);  pkOL = 0;
        }
        TblORDER_LINE::Row * prOL = 0;
        for( uint16_t ol = 0; ol < o_ol_cnt; ol++ ) {
            ans = conn.getReadResult( ((::Value **)(&prOL)), &pSeqnum);
            assert(ans != -1);
            assert( !(prOL->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
            assert( !(prOL->isNULL(TblORDER_LINE::Row::OL_I_ID)) );
            uint32_t ol_i_id = prOL->getCol_uint32(TblORDER_LINE::Row::OL_I_ID);
            assert( !(prOL->isNULL(TblORDER_LINE::Row::OL_SUPPLY_W_ID)) );
            uint16_t ol_supply_w_id = prOL->getCol_uint16(NULL, TblORDER_LINE::Row::OL_SUPPLY_W_ID);
            time_t ol_delivery_d = 0;
            if( !(prOL->isNULL(TblORDER_LINE::Row::OL_DELIVERY_D)) ) {
                ol_delivery_d = prOL->getCol_time(TblORDER_LINE::Row::OL_DELIVERY_D);
            }
            assert( !(prOL->isNULL(TblORDER_LINE::Row::OL_QUANTITY)) );
            uint16_t ol_quantity = prOL->getCol_uint16(NULL, TblORDER_LINE::Row::OL_QUANTITY);
            assert( !(prOL->isNULL(TblORDER_LINE::Row::OL_AMOUNT)) );
            double ol_amount = prOL->getCol_double(TblORDER_LINE::Row::OL_AMOUNT);
        }

        ans = 0;
        
        /*
         * cleanup ...
         */
        delete [] c_lst;  delete [] c_mdl;  delete [] c_fst;
        return ans;
    }

};
