//
//  TblNEW_ORDER_INDEX.cpp
//
//  Key and row descriptions for the TPC-C NEW_ORDER_INDEX table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <time.h>
#include "tpcc/TblNEW_ORDER_INDEX.h"

namespace TPCC {

    void TblNEW_ORDER_INDEX::Key::init(uint16_t nox_w_id, uint16_t nox_d_id, uint16_t nox_t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = nox_w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = nox_d_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = nox_t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblNEW_ORDER_INDEX::Row::populate( DataGen &dg, uint32_t nox_o_id ) {
        reset();
        { /* NOX_O_ID */
        	putCol_uint32( NOX_O_ID, nox_o_id );
        }
    }
  
    void TblNEW_ORDER_INDEX::Key::print() {
        FILE * f = stdout;
        fprintf(f, "NEW_ORDER_INDEX::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblNEW_ORDER_INDEX::TBLID);
            printPart_uint16(pos, "NOX_W_ID");
            printPart_uint16(pos, "NOX_D_ID");
            printPart_uint16(pos, "NOX_T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblNEW_ORDER_INDEX::Row::print() {
        FILE * f = stdout;
        fprintf(f, "NEW_ORDER_INDEX::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint32(NOX_O_ID, "NOX_O_ID");
        }
        fprintf(f, "}");
    }

}; // TPCC
