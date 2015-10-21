//
//  TblHISTORY.cpp
//
//  Key and row descriptions for the TPC-C HISTORY table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <time.h>
#include "tpcc/TblHISTORY.h"

namespace TPCC {

    void TblHISTORY::Key::init(uint16_t h_terminal, uint32_t h_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = h_terminal;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = h_id;  p += sizeof(uint32_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblHISTORY::Row::populate( DataGen &dg, uint16_t w_id, uint16_t d_id, uint32_t c_id ) {
        reset();
        { /* H_C_W_ID */
            putCol_uint16( H_C_W_ID, w_id );
        }
        { /* H_C_D_ID */
            putCol_uint16( H_C_D_ID, d_id );
        }
        { /* H_C_W_ID */
            putCol_uint32( H_C_ID, c_id );
        }
        { /* H_W_ID */
            putCol_uint16( H_W_ID, w_id );
        }
        { /* H_D_ID */
            putCol_uint16( H_D_ID, d_id );
        }
        { /* H_DATE */
            putCol_time( H_DATE, time(0) );
        }
        { /* H_AMOUNT */
            putCol_double( H_AMOUNT, 10.00 );
        }
        { /* H_DATA */
            char h_data[24];
            size_t len = dg.randomAlphanumericString(h_data, 12, 25);
            putCol( H_DATA, (uint8_t *)(h_data), len );
        }
    }
    
    void TblHISTORY::Key::print() {
        FILE * f = stdout;
        fprintf(f, "HISTORY::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblHISTORY::TBLID);
            printPart_uint16(pos, "H_TERMINAL");
            printPart_uint32(pos, "H_ID");
        }
        fprintf(f, "}");
    }
    
    void TblHISTORY::Row::print() {
        FILE * f = stdout;
        fprintf(f, "HISTORY::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint16(H_C_W_ID, "H_C_W_ID");
            printCol_uint16(H_C_D_ID, "H_C_D_ID");
            printCol_uint32(H_C_ID, "H_C_ID");
            printCol_uint16(H_W_ID, "H_W_ID");
            printCol_uint16(H_D_ID, "H_D_ID");
            printCol_time(H_DATE, "H_DATE");
            printCol_double(H_AMOUNT, "H_AMOUNT");
            printCol_string(H_DATA, "H_DATA");
        }
        fprintf(f, "}");
    }

}; // TPCC
