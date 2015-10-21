//
//  TblCUSTOMER_PAYMENT.cpp
//
//  Key and row descriptions for the TPC-C CUSTOMER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include "tpcc/TblCUSTOMER_PAYMENT.h"

namespace TPCC {

    void TblCUSTOMER_PAYMENT::Key::init(uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = d_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = c_id;  p += sizeof(uint32_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblCUSTOMER_PAYMENT::Row::populate( DataGen &dg, uint32_t c_id) {
        // reset( NUM_COLUMNS, MAX_LENGTH );
        reset();
        { /* C_BALANCE */
            putCol_double( C_BALANCE, -10.00 );
        }
        { /* C_YTD_PAYMENT */
            putCol_double( C_YTD_PAYMENT, 10.00 );
        }
        { /* C_PAYMENT_CNT */
            putCol_uint16( C_PAYMENT_CNT, 1 );
        }
        { /* C_DELIVERY_CNT */
            putCol_uint16( C_DELIVERY_CNT, 0 );
        }
        { /* C_DATA */
        	char c_data[500];
        	size_t len = dg.randomAlphanumericString(c_data, 300, 501); 
            putCol( C_DATA, (uint8_t *)(c_data), len );
        }
    }
    
    void TblCUSTOMER_PAYMENT::Key::print() {
        FILE * f = stdout;
        fprintf(f, "CUSTOMER::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblCUSTOMER_PAYMENT::TBLID);
            printPart_uint16(pos, "C_W_ID");
            printPart_uint16(pos, "C_D_ID");
            printPart_uint32(pos, "C_ID");
        }
        fprintf(f, "}");
    }

    void TblCUSTOMER_PAYMENT::Row::print() {
        FILE * f = stdout;
        fprintf(f, "CUSTOMER::Row { ");
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_double(C_BALANCE, "C_BALANCE");
            printCol_double(C_YTD_PAYMENT, "C_YTD_PAYMENT");
            printCol_uint16(C_PAYMENT_CNT, "C_PAYMENT_CNT");
            printCol_uint16(C_DELIVERY_CNT, "C_DELIVERY_CNT");
            printCol_string(C_DATA, "C_DATA");
        }
        fprintf(f, "}");
    }

    
}; // TPCC
