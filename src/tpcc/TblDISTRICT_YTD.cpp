//
//  TblDISTRICT_YTD.cpp
//
//  Key and row descriptions for the TPC-C DISTRICT table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include "tpcc/TblDISTRICT_YTD.h"

namespace TPCC {

    void TblDISTRICT_YTD::Key::init(uint16_t w_id, uint16_t d_id, uint16_t t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = d_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblDISTRICT_YTD::Row::populate( DataGen &dg ) {
        reset();
        { /* D_YTD */
            putCol_double( D_YTD, 30000.00 );
        }
    }
    
    void TblDISTRICT_YTD::Key::print() {
        FILE * f = stdout;
        fprintf(f, "DISTRICT::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblDISTRICT_YTD::TBLID);
            printPart_uint16(pos, "D_W_ID");
            printPart_uint16(pos, "D_ID");
            printPart_uint16(pos, "T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblDISTRICT_YTD::Row::print() {
        FILE * f = stdout;
        fprintf(f, "DISTRICT::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_double(D_YTD, "D_YTD");
        }
        fprintf(f, "}");
    }

}; // TPCC
