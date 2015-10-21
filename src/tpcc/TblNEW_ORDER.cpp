//
//  TblNEW_ORDER.cpp
//
//  Key and row descriptions for the TPC-C NEW_ORDER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <time.h>
#include "tpcc/TblNEW_ORDER.h"

namespace TPCC {

    void TblNEW_ORDER::Key::init(uint16_t no_w_id, uint16_t no_d_id, 
            uint32_t no_o_id, uint16_t no_t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = no_w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = no_d_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = no_o_id;  p += sizeof(uint32_t);
        *((uint16_t *)(p)) = no_t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblNEW_ORDER::Row::populate( DataGen &dg ) {
        reset();
        //
        // there are no non-NULL values in any row of this table
        // all information is conveyed by presence or absence of a row with a given key
        //
    }
   
    void TblNEW_ORDER::Key::print() {
        FILE * f = stdout;
        fprintf(f, "NEW_ORDER::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblNEW_ORDER::TBLID);
            printPart_uint16(pos, "NO_W_ID");
            printPart_uint16(pos, "NO_D_ID");
            printPart_uint32(pos, "NO_O_ID");
            printPart_uint16(pos, "NO_T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblNEW_ORDER::Row::print() {
        FILE * f = stdout;
        fprintf(f, "NEW_ORDER::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint32(NO_X, "NO_X");
        }
        fprintf(f, "}");
    }

}; // TPCC
