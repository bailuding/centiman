//
//  TblCUSTOMER_INDEX.cpp
//
//  Key and row descriptions for the TPC-C CUSTOMER_INDEX table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <assert.h>
#include "tpcc/TblCUSTOMER_INDEX.h"

namespace TPCC {

    void TblCUSTOMER_INDEX::Key::init( uint16_t cx_w_id, uint16_t cx_d_id,
			const uint8_t * cx_last, uint16_t cx_last_len ) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = cx_w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = cx_d_id;  p += sizeof(uint16_t);
        memcpy( p, cx_last, cx_last_len );  p += cx_last_len;
        copyFrom(buf, (p-buf));
    }

        
    void TblCUSTOMER_INDEX::Row::populate( DataGen &dg ) {
        reset();
        putCol_uint16( CX_C_NID, 0 );
    }

    void TblCUSTOMER_INDEX::Key::print() {
        FILE * f = stdout;
        fprintf(f, "CUSTOMER_INDEX::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblCUSTOMER_INDEX::TBLID);
            printPart_uint16(pos, "CX_W_ID");
            printPart_uint16(pos, "CX_D_ID");
            printPart_chars(pos, "CX_LAST");
        }
        fprintf(f, "}");
    }
    
    void TblCUSTOMER_INDEX::Row::print() {
        FILE * f = stdout;
        fprintf(f, "CUSTOMER_INDEX::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint16(CX_C_NID, "CX_C_NID");
            printCol_uint32(CX_C_ID_0, "CX_C_ID_0");
            printCol_uint32(CX_C_ID_1, "CX_C_ID_1");
            printCol_uint32(CX_C_ID_2, "CX_C_ID_2");
            printCol_uint32(CX_C_ID_3, "CX_C_ID_3");
            printCol_uint32(CX_C_ID_4, "CX_C_ID_4");
            printCol_uint32(CX_C_ID_5, "CX_C_ID_5");
            printCol_uint32(CX_C_ID_6, "CX_C_ID_6");
            printCol_uint32(CX_C_ID_7, "CX_C_ID_7");
            printCol_uint32(CX_C_ID_8, "CX_C_ID_8");
            printCol_uint32(CX_C_ID_9, "CX_C_ID_9");
        }
        fprintf(f, "}");
    }
    
}; // TPCC
