//
//  DBtxnNewOrder.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 11/1/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

//#include <unordered_set>
#include <set>
#include <tpcc/Tables.h>
#include <tpcc/DB.h>
#include <util/const.h>


namespace TPCC {


    /*
     * New Order Transaction ...
     */
    
    int DB::txnNewOrder() {
        int ans;
        
        uint16_t theW_ID = terminal_w_id;
        uint16_t theT_ID = terminal_id;
        time_t now = time(0);
        
        /*
         * generate the input data ...
         */
        uint16_t theD_ID = dg.uniformInt(0, numDISTRICTs_per_WAREHOUSE);
        uint32_t theC_ID = dg.NURand(dg.A_for_C_ID, 1, numCUSTOMERs_per_DISTRICT, dg.C_for_C_ID) - 1; // in [0..numCUSTOMERs_per_DISTRICT)
        uint16_t theOL_CNT = dg.uniformInt(5, 16);
        /* choose items, quantities and supplying warehouses ... */
        uint32_t i_ids[15];
        uint16_t supply_w_ids[15];
        uint16_t ol_quantities[15];
        uint16_t all_local = 1;
        { /* do not order the same item twice ... */
//            std::unordered_set<uint32_t> i_id_set;
            std::set<uint32_t> i_id_set;
            int ol = 0;
            while( ol < theOL_CNT ) {
                uint32_t tmp_i_id = dg.NURand(dg.A_for_OL_I_ID, 1, numITEMs, dg.C_for_OL_I_ID) - 1;  // in [0..numITEMs)
                if (i_id_set.find(tmp_i_id) != i_id_set.end()) continue;
//                if( i_id_set.count(tmp_i_id) > 0 ) continue;
                i_id_set.insert(tmp_i_id);  i_ids[ol] = tmp_i_id;
                supply_w_ids[ol] = theW_ID;
                if( (numWAREHOUSEs > 1) && dg.flip(0.01) ) /* nonlocal warehouse */ {
                    all_local = 0;
                    do { supply_w_ids[ol] = dg.uniformInt(0, numWAREHOUSEs); } while( supply_w_ids[ol] == theW_ID );
                }
                ol_quantities[ol] = dg.uniformInt(1, 11);
                ol++;
            }
        }
        /* TODO: force rollback 1% of time ... */
//        if( dg.flip(0.01) ) { i_ids[theOL_CNT-1] = (uint32_t)(-1); }
        
        /*
         * issue reads ...
         */
        
        /* WAREHOUSE table ... */
        TblWAREHOUSE::Key * pkW = new TblWAREHOUSE::Key(theW_ID);
        ans = conn.requestRead(pkW);  
        
        /* DISTRICT table ... */
/*        TblDISTRICT::Key * pkD = new TblDISTRICT::Key(theW_ID, theD_ID);
        ans = conn.requestRead(pkD);  pkD = 0;*/
       
        /* DISTRICT_NEXT_O_ID table ...  */
        TblDISTRICT_NEXT_O_ID::Key * pkD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Key(theW_ID, theD_ID, theT_ID);
        ans = conn.requestRead(pkD_NEXT_O_ID);  delete pkD_NEXT_O_ID;
       

        /* CUSTOMER table ... */
        TblCUSTOMER::Key * pkC = new TblCUSTOMER::Key(theW_ID, theD_ID, theC_ID);
        ans = conn.requestRead(pkC);  delete pkC;
        
        /* ITEM and STOCK tables ... */
        for( int ol = 0; ol < theOL_CNT; ol++ ) {
            TblITEM::Key * pkI = new TblITEM::Key(i_ids[ol]);
            ans = conn.requestRead(pkI);  delete pkI;
            TblSTOCK::Key * pkS = new TblSTOCK::Key(supply_w_ids[ol], i_ids[ol]);
            ans = conn.requestRead(pkS);  delete pkS;
        }
        
        /*
         * Get and process read results in the order they were issued ...
         */
        uint32_t o_id;
        double d_tax;
        double c_discount;
        char * c_last = 0;
        char c_credit[2]; // 'BC' or 'GC'
        double c_credit_lim;
        
        /* WAREHOUSE table ... */
        TblWAREHOUSE::Row * prW = 0;
        Seqnum * pSeqnum = NULL;
        ans = conn.getReadResult( ((::Value **)(&prW)), &pSeqnum );
        if (*pSeqnum == Const::SEQNUM_NULL) {
            pkW->print();
        }
        assert( !(prW->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_TAX) );
        double w_tax = prW->getCol_double(TblWAREHOUSE::Row::W_TAX);
        
        /* DISTRICT table ... */
/*        TblDISTRICT::Row * prD = 0;
        ans = conn.getReadResult( ((::Value **)(&prD)), &pSeqnum );
        assert( !(prD->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
        assert( !prD->isNULL(TblDISTRICT::Row::D_TAX) );
        d_tax = prD->getCol_double(TblDISTRICT::Row::D_TAX);
        assert( !prD->isNULL(TblDISTRICT::Row::D_NEXT_O_ID) );*/
        /* DISTRICT_NEXT_O_ID table ... */
        TblDISTRICT::Row * prD_NEXT_O_ID = 0;
        ans = conn.getReadResult( ((::Value **)(&prD_NEXT_O_ID)), &pSeqnum );
        o_id = prD_NEXT_O_ID->getCol_uint32(TblDISTRICT_NEXT_O_ID::Row::D_NEXT_O_ID);
        prD_NEXT_O_ID->putCol_uint32(TblDISTRICT_NEXT_O_ID::Row::D_NEXT_O_ID, o_id+1);
        pkD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Key(theW_ID, theD_ID, theT_ID);
        ans = conn.requestWrite(pkD_NEXT_O_ID, prD_NEXT_O_ID);  delete pkD_NEXT_O_ID;
        
        /* CUSTOMER table ... */
        TblCUSTOMER::Row * prC = 0;
        ans = conn.getReadResult( ((::Value **)(&prC)), &pSeqnum );
        assert( !(prC->isNULL() || *pSeqnum == Const::SEQNUM_NULL));
        assert( !prC->isNULL(TblCUSTOMER::Row::C_DISCOUNT) );
        c_discount = prC->getCol_double(TblCUSTOMER::Row::C_DISCOUNT);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_LAST) );
        c_last = prC->getCol_cString(TblCUSTOMER::Row::C_LAST);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_CREDIT) );
        ans = prC->getCol(TblCUSTOMER::Row::C_CREDIT, ((uint8_t *)(&c_credit[0])), (sizeof c_credit));
        assert( ans == 2 );
        assert( !prC->isNULL(TblCUSTOMER::Row::C_CREDIT_LIM) );
        c_credit_lim = prC->getCol_double(TblCUSTOMER::Row::C_CREDIT_LIM);
        
        /* create and write new rows for ORDER, ORDER_INDEX, NEW_ORDER ... */
        TblORDER::Key * pkO = new TblORDER::Key( theW_ID, theD_ID, o_id, theT_ID );
        TblORDER::Row * prO = new TblORDER::Row;  prO->reset();
        prO->putCol_time(TblORDER::Row::O_ENTRY_D, now);
        prO->putCol_uint16(TblORDER::Row::O_OL_CNT, theOL_CNT);
        prO->putCol_uint16(TblORDER::Row::O_ALL_LOCAL, all_local);
        ans = conn.requestWrite(pkO, prO);  delete pkO;  delete prO;
        
        TblORDER_INDEX::Key * pkOX = new TblORDER_INDEX::Key( theW_ID, theD_ID, theC_ID, theT_ID );
        TblORDER_INDEX::Row * prOX = new TblORDER_INDEX::Row;  prOX->reset();
        prOX->putCol_uint32(TblORDER_INDEX::Row::OX_O_ID, o_id);
        ans = conn.requestWrite(pkOX, prOX);  delete pkOX;  delete prOX;
        
        TblNEW_ORDER::Key * pkNO = new TblNEW_ORDER::Key( theW_ID, theD_ID, o_id, theT_ID );
        TblNEW_ORDER::Row * prNO = new TblNEW_ORDER::Row;  prNO->reset();
        ans = conn.requestWrite(pkNO, prNO);  delete pkNO;  delete prNO;
        
        /* process ordered items, writing ORDER_LINE rows and updating STOCK table as needed ... */
        double total_amount = 0.0;
        for( int ol = 0; ol < theOL_CNT; ol++ ) {
            
            /* retrieve ITEM and STOCK read results ... */
            TblITEM::Row * prI = 0;
            ans = conn.getReadResult( ((::Value **)(&prI)), &pSeqnum );
            
            if( (prI->isNULL() || *pSeqnum == Const::SEQNUM_NULL)) /* request for unused item */ {
                ans = (-1);  goto Out;
            }
            
            assert( !(prI->isNULL(TblITEM::Row::I_PRICE)) );
            double i_price = prI->getCol_double(TblITEM::Row::I_PRICE);
            assert( !(prI->isNULL(TblITEM::Row::I_NAME)) );
            char * i_name = prI->getCol_cString(TblITEM::Row::I_NAME);
            assert( !(prI->isNULL(TblITEM::Row::I_DATA)) );
            char * i_data = prI->getCol_cString(TblITEM::Row::I_DATA);
            
            TblSTOCK::Row * prS = 0;
            ans = conn.getReadResult( ((::Value **)(&prS)), &pSeqnum);
            assert( !(prS->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
            assert( !(prS->isNULL(TblSTOCK::Row::S_DIST_01+supply_w_ids[ol])) );
            char * s_dist_xx = prS->getCol_cString(TblSTOCK::Row::S_DIST_01+supply_w_ids[ol]);
            assert( !(prS->isNULL(TblSTOCK::Row::S_DATA)) );
            char * s_data = prS->getCol_cString(TblSTOCK::Row::S_DATA);
            assert( !(prS->isNULL(TblSTOCK::Row::S_QUANTITY)) );
            uint16_t s_quantity = prS->getCol_uint16(NULL, TblSTOCK::Row::S_QUANTITY);
            assert( !(prS->isNULL(TblSTOCK::Row::S_YTD)) );
            uint32_t s_ytd = prS->getCol_uint32(TblSTOCK::Row::S_YTD);
            assert( !(prS->isNULL(TblSTOCK::Row::S_ORDER_CNT)) );
            uint16_t s_order_cnt = prS->getCol_uint16(NULL, TblSTOCK::Row::S_ORDER_CNT);
            assert( !(prS->isNULL(TblSTOCK::Row::S_REMOTE_CNT)) );
            uint16_t s_remote_cnt = prS->getCol_uint16(NULL, TblSTOCK::Row::S_REMOTE_CNT);
            
            /* update the STOCK row ... */
            if( s_quantity >= (ol_quantities[ol] + 10) ) {
                prS->putCol_uint16( TblSTOCK::Row::S_QUANTITY, s_quantity - ol_quantities[ol] );
            } else {
                prS->putCol_uint16( TblSTOCK::Row::S_QUANTITY, s_quantity + 91 - ol_quantities[ol] );
            }
            prS->putCol_uint32(TblSTOCK::Row::S_YTD, s_ytd + ol_quantities[ol]);
            prS->putCol_uint16(TblSTOCK::Row::S_ORDER_CNT, s_order_cnt+1);
            if( supply_w_ids[ol] != theW_ID ) {
                prS->putCol_uint16( TblSTOCK::Row::S_REMOTE_CNT, s_remote_cnt+1 );
            }
            TblSTOCK::Key * pkS = new TblSTOCK::Key(supply_w_ids[ol], i_ids[ol]);
            ans = conn.requestWrite(pkS, prS);  delete pkS;
            
            /* insert an ORDER_LINE row ... */
            TblORDER_LINE::Key * pkOL = new TblORDER_LINE::Key(theW_ID, theD_ID, o_id, ol, theT_ID);
            TblORDER_LINE::Row * prOL = new TblORDER_LINE::Row;  prOL->reset();
            double ol_amount = ol_quantities[ol] * i_price;
            prOL->putCol_uint32(TblORDER_LINE::Row::OL_I_ID, i_ids[ol]);
            prOL->putCol_uint16(TblORDER_LINE::Row::OL_SUPPLY_W_ID, supply_w_ids[ol]);
            // OL_DELIVERY_D is NULL
            prOL->putCol_uint16(TblORDER_LINE::Row::OL_QUANTITY, ol_quantities[ol]);
            prOL->putCol_double(TblORDER_LINE::Row::OL_AMOUNT, ol_amount);
            prOL->putCol(TblORDER_LINE::Row::OL_DIST_INFO, (uint8_t *)(s_dist_xx), strlen(s_dist_xx));
            ans = conn.requestWrite(pkOL, prOL);  delete pkOL;  delete prOL;
            
            /* accumulate total_amount ... */
            total_amount += ol_amount * (1.0 - c_discount) * (1.0 + w_tax + d_tax);
            
            /* the funky "brand-generic" test ... */
            char brand_generic = 'B';
            if( (strstr(i_data, "ORIGINAL") == 0) || (strstr(i_data, "ORIGINAL") == 0) ) {
                brand_generic = 'G';
            }
            
            /* clean up */
            delete [] i_name;  delete [] i_data;
            delete [] s_dist_xx;  delete [] s_data;
        }
        ans = 0;
        
    Out: ;
        /* clean up */
        delete [] c_last;
        
        /* clean keys */
        delete pkW;

        return ans;
    }
    

};
