//
//  TblWAREHOUSE_YTD.cpp
//
//  Key and row descriptions for the TPC-C WAREHOUSE table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <cstdio>

#include "tpcc/TblWAREHOUSE_YTD.h"

namespace TPCC {

    void TblWAREHOUSE_YTD::Key::init(uint16_t w_id, uint16_t t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblWAREHOUSE_YTD::Row::populate( DataGen &dg ) {
        reset();
        { /* W_YTD */
            putCol_double( W_YTD, 300000.00 );
        }
    }

    void TblWAREHOUSE_YTD::Key::print() {
        FILE * f = stdout;
        fprintf(f, "WAREHOUSE_YTD::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblWAREHOUSE_YTD::TBLID);
            printPart_uint16(pos, "W_ID");
            printPart_uint16(pos, "T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblWAREHOUSE_YTD::Row::print() {
        FILE * f = stdout;
        fprintf(f, "WAREHOUSE_YTD::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_double(W_YTD, "W_YTD");
        }
        fprintf(f, "}");
    }
    
}; // TPCC
