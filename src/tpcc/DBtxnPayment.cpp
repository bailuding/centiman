//
//  DBtxnPayment.cpp
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
     * Payment Transaction ...
     */
    int DB::txnPayment() {
        int ans;

        uint16_t theT_ID = terminal_id;
        uint16_t theW_ID = terminal_w_id;

        /*
         * generate the input data ...
         */
        uint16_t theD_ID = dg.uniformInt(0, numDISTRICTs_per_WAREHOUSE);
        uint16_t c_w_id;
        uint16_t c_d_id;
        uint32_t c_id;
        if( (numWAREHOUSEs == 1) || dg.flip(0.85) ) /* local */ {
            c_w_id = theW_ID;  c_d_id = theD_ID;
        } else /* nonlocal */ {
            do { c_w_id = dg.uniformInt(0, numWAREHOUSEs); } while( c_w_id == theW_ID );
            c_d_id = dg.uniformInt(0, numDISTRICTs_per_WAREHOUSE);
        }
        char c_last[16];
        uint16_t c_last_len = 0;
        if( dg.flip(0.6) ) /* customer by last name */ {
//            c_last_len = dg.randomCustomerName(c_last, (sizeof c_last));
            c_last_len = dg.randomCustomerName(c_last, dg.uniformInt(0, numCUSTOMERs_per_DISTRICT));
            assert( c_last_len > 0 );
        } else /* customer by C_ID */ {
            c_id = dg.NURand(dg.A_for_C_LAST, 1, numCUSTOMERs_per_DISTRICT, dg.C_for_C_LAST) - 1; // [1..nCperD] - 1
        }
        double h_amount = dg.uniformInt(100, 500001)/100.00;
        time_t h_date = time(0);
        
        Seqnum * pSeqnum = NULL;
        /*
         * resolve customer ID ...
         */
        if( c_last_len > 0 ) /* customer specified by last name */ {
            /* get c_id using TblCUSTOMER_INDEX ... */
            TblCUSTOMER_INDEX::Key * pkCX = new TblCUSTOMER_INDEX::Key(c_w_id, c_d_id, (const uint8_t *)(c_last), c_last_len);
            TblCUSTOMER_INDEX::Row * prCX = 0;
            ans = conn.read( pkCX, ((::Value **)(&prCX)), &pSeqnum );  delete pkCX;
            if( (prCX->isNULL() || *pSeqnum == Const::SEQNUM_NULL) ) /* randomly generated customer last name does not exist */ {
                return (-1);
            }
            assert( !prCX->isNULL(TblCUSTOMER_INDEX::Row::CX_C_NID) );
            uint16_t cx_c = prCX->getCol_uint16((::Key *)pkCX, TblCUSTOMER_INDEX::Row::CX_C_NID);
            assert( cx_c >= 1 );
            cx_c = TblCUSTOMER_INDEX::Row::CX_C_ID_0 + (cx_c/2);
            assert( !prCX->isNULL( cx_c ) );
            c_id = prCX->getCol_uint32( cx_c );
        }

        /*
         * issue reads  ...
         */
        
        /* WAREHOUSE table ... */
        TblWAREHOUSE::Key * pkW = new TblWAREHOUSE::Key(theW_ID);
        ans = conn.requestRead(pkW);  delete pkW;
        /* WAREHOUSE_YTD table ... */
        TblWAREHOUSE_YTD::Key * pkW_YTD = new TblWAREHOUSE_YTD::Key(theW_ID, theT_ID);
        ans = conn.requestRead(pkW_YTD); delete pkW_YTD;
        
        /* DISTRICT table ... */
        TblDISTRICT::Key * pkD = new TblDISTRICT::Key(theW_ID, theD_ID);
        ans = conn.requestRead(pkD);  delete pkD;
        /* DISTRICT_YTD table ... */
        TblDISTRICT_YTD::Key * pkD_YTD = new TblDISTRICT_YTD::Key(theW_ID, theD_ID, theT_ID);
        ans = conn.requestRead(pkD_YTD);  delete pkD_YTD;
        
        /* CUSTOMER table ... */
        TblCUSTOMER::Key * pkC = new TblCUSTOMER::Key(c_w_id, c_d_id, c_id);
        ans = conn.requestRead(pkC);  delete pkC;
        /* CUSTOMER_PAYMENT table ... */
        TblCUSTOMER_PAYMENT::Key * pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(c_w_id, c_d_id, c_id);
        ans = conn.requestRead(pkC_PAYMENT);  delete pkC_PAYMENT;
        
        /*
         * retrieve data ...
         */
        
        /* WAREHOUSE table ... */
        TblWAREHOUSE::Row * prW = 0;
        ans = conn.getReadResult( ((::Value **)(&prW)), &pSeqnum );
        assert( !(prW->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_NAME) );
        char * w_name = prW->getCol_cString(TblWAREHOUSE::Row::W_NAME);
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_STREET_1) );
        char * w_street_1 = prW->getCol_cString(TblWAREHOUSE::Row::W_STREET_1);
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_STREET_2) );
        char * w_street_2 = prW->getCol_cString(TblWAREHOUSE::Row::W_STREET_2);
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_CITY) );
        char * w_city = prW->getCol_cString(TblWAREHOUSE::Row::W_CITY);
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_STATE) );
        char * w_state = prW->getCol_cString(TblWAREHOUSE::Row::W_STATE);
        assert( !prW->isNULL(TblWAREHOUSE::Row::W_ZIP) );
        char * w_zip = prW->getCol_cString(TblWAREHOUSE::Row::W_ZIP);

        /* WAREHOUSE_YTD table ... */
        TblWAREHOUSE_YTD::Row * prW_YTD = 0;
        ans = conn.getReadResult( ((::Value **)(&prW_YTD)), &pSeqnum);
        assert(!(prW_YTD->isNULL() || *pSeqnum == Const::SEQNUM_NULL));
        assert( !prW_YTD->isNULL(TblWAREHOUSE_YTD::Row::W_YTD) );
        double w_ytd = prW_YTD->getCol_double(TblWAREHOUSE_YTD::Row::W_YTD);
        
        /* DISTRICT table ... */
        TblDISTRICT::Row * prD = 0;
        ans = conn.getReadResult( ((::Value **)(&prD)), &pSeqnum );
        assert(ans != -1);
        assert(*pSeqnum != Const::SEQNUM_NULL);
        assert( !(prD->isNULL()));
        
        assert( !prD->isNULL(TblDISTRICT::Row::D_NAME) );
        char * d_name = prD->getCol_cString(TblDISTRICT::Row::D_NAME);
        assert( !prD->isNULL(TblDISTRICT::Row::D_STREET_1) );
        char * d_street_1 = prD->getCol_cString(TblDISTRICT::Row::D_STREET_1);
        assert( !prD->isNULL(TblDISTRICT::Row::D_STREET_2) );
        char * d_street_2 = prD->getCol_cString(TblDISTRICT::Row::D_STREET_2);
        assert( !prD->isNULL(TblDISTRICT::Row::D_CITY) );
        char * d_city = prD->getCol_cString(TblDISTRICT::Row::D_CITY);
        assert( !prD->isNULL(TblDISTRICT::Row::D_STATE) );
        char * d_state = prD->getCol_cString(TblDISTRICT::Row::D_STATE);
        assert( !prD->isNULL(TblDISTRICT::Row::D_ZIP) );
        char * d_zip = prD->getCol_cString(TblDISTRICT::Row::D_ZIP);

        /* DISTRICT_YTD table ... */
        TblDISTRICT_YTD::Row * prD_YTD = 0;
        ans = conn.getReadResult( ((::Value **)(&prD_YTD)), &pSeqnum );
        assert(ans != -1);
        assert(*pSeqnum != Const::SEQNUM_NULL);
        assert(!(prD_YTD->isNULL()));
        assert( !prD_YTD->isNULL(TblDISTRICT_YTD::Row::D_YTD) );
        double d_ytd = prD_YTD->getCol_double(TblDISTRICT_YTD::Row::D_YTD);
        
        /* CUSTOMER table ... */
        TblCUSTOMER::Row * prC = 0;
        ans = conn.getReadResult( ((::Value **)(&prC)), &pSeqnum );
        assert( !(prC->isNULL() || *pSeqnum == Const::SEQNUM_NULL) );
        assert( !prC->isNULL(TblCUSTOMER::Row::C_FIRST) );
        char * c_fst = prC->getCol_cString(TblCUSTOMER::Row::C_FIRST);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_MIDDLE) );
        char * c_mdl = prC->getCol_cString(TblCUSTOMER::Row::C_MIDDLE);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_LAST) );
        char * c_lst = prC->getCol_cString(TblCUSTOMER::Row::C_LAST);
        
        assert( !prC->isNULL(TblCUSTOMER::Row::C_STREET_1) );
        char * c_street_1 = prC->getCol_cString(TblCUSTOMER::Row::C_STREET_1);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_STREET_2) );
        char * c_street_2 = prC->getCol_cString(TblCUSTOMER::Row::C_STREET_2);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_CITY) );
        char * c_city = prC->getCol_cString(TblCUSTOMER::Row::C_CITY);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_STATE) );
        char * c_state = prC->getCol_cString(TblCUSTOMER::Row::C_STATE);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_ZIP) );
        char * c_zip = prC->getCol_cString(TblCUSTOMER::Row::C_ZIP);
        
        assert( !prC->isNULL(TblCUSTOMER::Row::C_PHONE) );
        char * c_phone = prC->getCol_cString(TblCUSTOMER::Row::C_PHONE);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_SINCE) );
        time_t c_since = prC->getCol_time(TblCUSTOMER::Row::C_SINCE);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_CREDIT) );
        char c_credit[2];  ans = prC->getCol( TblCUSTOMER::Row::C_CREDIT, (uint8_t *)(c_credit), (sizeof c_credit) );
        assert( !prC->isNULL(TblCUSTOMER::Row::C_CREDIT_LIM) );
        double c_credit_lim = prC->getCol_double(TblCUSTOMER::Row::C_CREDIT_LIM);
        assert( !prC->isNULL(TblCUSTOMER::Row::C_DISCOUNT) );
        double c_discount = prC->getCol_double(TblCUSTOMER::Row::C_DISCOUNT);
        
        /* CUSTOMER_PAYMENT table ... */
        TblCUSTOMER_PAYMENT::Row * prC_PAYMENT = 0;
        ans = conn.getReadResult( ((::Value **)(&prC_PAYMENT)), &pSeqnum );
        assert( !prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_BALANCE) );
        double c_balance = prC_PAYMENT->getCol_double(TblCUSTOMER_PAYMENT::Row::C_BALANCE);
        assert( !prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_YTD_PAYMENT) );
        double c_ytd_payment = prC_PAYMENT->getCol_double(TblCUSTOMER_PAYMENT::Row::C_YTD_PAYMENT);
        assert( !prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_DELIVERY_CNT) );
        uint16_t c_delivery_cnt = prC_PAYMENT->getCol_uint16((::Key *)pkC_PAYMENT, TblCUSTOMER_PAYMENT::Row::C_DELIVERY_CNT);
        assert( !prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_PAYMENT_CNT) );
        uint16_t c_payment_cnt = prC_PAYMENT->getCol_uint16((::Key *)pkC_PAYMENT, TblCUSTOMER_PAYMENT::Row::C_PAYMENT_CNT);
        char * c_data = 0;
        if( c_credit[0] == 'B' ) {
            assert( !prC_PAYMENT->isNULL(TblCUSTOMER_PAYMENT::Row::C_DATA) );
            c_data = prC_PAYMENT->getCol_cString(TblCUSTOMER_PAYMENT::Row::C_DATA);
        }
        
        /*
         * write data ...
         */
        
        /* WAREHOUSE_YTD table ... */
        pkW_YTD = new TblWAREHOUSE_YTD::Key( theW_ID, theT_ID );
        prW_YTD->putCol_double( TblWAREHOUSE_YTD::Row::W_YTD, w_ytd + h_amount );
        ans = conn.requestWrite(pkW_YTD, prW_YTD);  delete pkW_YTD;

/*        pkW = new TblWAREHOUSE::Key( theW_ID );
        prW->putCol_double( TblWAREHOUSE::Row::W_YTD, w_ytd + h_amount );
        ans = conn.requestWrite(pkW, prW);  pkW = 0;  prW = 0;*/
        
        /* DISTRICT_YTD table ... */
        pkD_YTD = new TblDISTRICT_YTD::Key( theW_ID, theD_ID, theT_ID );
        prD_YTD->putCol_double( TblDISTRICT_YTD::Row::D_YTD, d_ytd + h_amount );
        ans = conn.requestWrite(pkD_YTD, prD_YTD);  delete pkD_YTD;
/*        pkD = new TblDISTRICT::Key( theW_ID, theD_ID );
        prD->putCol_double( TblDISTRICT::Row::D_YTD, d_ytd + h_amount );
        assert( !prD->isNULL(TblDISTRICT::Row::D_STREET_1) );
        ans = conn.requestWrite(pkD, prD);  pkD = 0;  prD = 0;*/
        
        /* CUSTOMER_PAYMENT table ... */
        pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(c_w_id, c_d_id, c_id);
        prC_PAYMENT->putCol_double( TblCUSTOMER_PAYMENT::Row::C_BALANCE, c_balance - h_amount );
        prC_PAYMENT->putCol_double( TblCUSTOMER_PAYMENT::Row::C_YTD_PAYMENT, c_ytd_payment + h_amount );
        prC_PAYMENT->putCol_uint16( TblCUSTOMER_PAYMENT::Row::C_PAYMENT_CNT, c_payment_cnt + 1 );
        if( c_credit[0] == 'B' ) {
            char new_c_data[501];
            ans = snprintf(new_c_data, (sizeof new_c_data), "%d %d %d %d %d %e %s",
                           c_id, c_d_id, c_w_id, theD_ID, theW_ID, h_amount, c_data);
            prC_PAYMENT->putCol(TblCUSTOMER_PAYMENT::Row::C_DATA, (uint8_t *)(new_c_data), ans);
        }
        ans = conn.requestWrite(pkC_PAYMENT, prC_PAYMENT);  delete pkC_PAYMENT;
 /*       pkC = new TblCUSTOMER::Key(c_w_id, c_d_id, c_id);
        prC->putCol_double( TblCUSTOMER::Row::C_BALANCE, c_balance - h_amount );
        prC->putCol_double( TblCUSTOMER::Row::C_YTD_PAYMENT, c_ytd_payment + h_amount );
        prC->putCol_uint16( TblCUSTOMER::Row::C_PAYMENT_CNT, c_payment_cnt + 1 );
        if( c_credit[0] == 'B' ) {
            char new_c_data[501];
            ans = snprintf(new_c_data, (sizeof new_c_data), "%d %d %d %d %d %e %s",
                           c_id, c_d_id, c_w_id, theD_ID, theW_ID, h_amount, c_data);
            prC->putCol(TblCUSTOMER::Row::C_DATA, (uint8_t *)(new_c_data), ans);
        }
        ans = conn.requestWrite(pkC, prC);  pkC = 0;  prC = 0;*/
        
        /* HISTORY table ... */
        TblHISTORY::Key * pkH = new TblHISTORY::Key(terminal_id, getTxn()); // we add at most one HISTORY row per txn
        TblHISTORY::Row * prH = new TblHISTORY::Row;  prH->reset();
        prH->putCol_uint16(TblHISTORY::Row::H_C_W_ID, c_w_id);
        prH->putCol_uint16(TblHISTORY::Row::H_C_D_ID, c_d_id);
        prH->putCol_uint32(TblHISTORY::Row::H_C_ID, c_id);
        prH->putCol_uint16(TblHISTORY::Row::H_W_ID, theW_ID);
        prH->putCol_uint16(TblHISTORY::Row::H_D_ID, theD_ID);
        prH->putCol_time(TblHISTORY::Row::H_DATE, h_date);
        prH->putCol_double(TblHISTORY::Row::H_AMOUNT, h_amount);
        char h_data[25];
        ans = snprintf(h_data, (sizeof h_data), "%s    %s", w_name, d_name);
        prH->putCol( TblHISTORY::Row::H_DATA, ((uint8_t *)(h_data)), ans );
        ans = conn.requestWrite(pkH, prH);  delete pkH;  delete prH;
        
        /*
         * cleanup ...
         */
        delete [] c_data;
        delete [] c_phone;  delete [] c_zip;  delete [] c_state;  delete [] c_city;
        delete [] c_street_2;  delete [] c_street_1;
        delete [] c_lst;  delete [] c_mdl;  delete [] c_fst;
        
        delete [] d_zip;  delete [] d_state;  delete [] d_city;
        delete [] d_street_2;  delete [] d_street_1;  delete [] d_name;
        
        delete [] w_zip;  delete [] w_state;  delete [] w_city;
        delete [] w_street_2;  delete [] w_street_1;  delete [] w_name;
        
        ans = 0;
        return ans;
    }

};
