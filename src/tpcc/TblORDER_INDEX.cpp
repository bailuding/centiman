//
//  TblORDER_INDEX.cpp
//
//  Key and row descriptions for the TPC-C ORDER_INDEX table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include "tpcc/TblORDER_INDEX.h"

namespace TPCC {

    void TblORDER_INDEX::Key::init(uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id, uint16_t ox_t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = ox_w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = ox_d_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = ox_c_id;  p += sizeof(uint32_t);
        *((uint16_t *)(p)) = ox_t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblORDER_INDEX::Row::populate( DataGen &dg, uint32_t ox_o_id ) {
        reset();
        { /* OX_O_ID */
        	putCol_uint32( OX_O_ID, ox_o_id );
        }
    }
    
    void TblORDER_INDEX::Key::print() {
        FILE * f = stdout;
        fprintf(f, "ORDER_INDEX::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblORDER_INDEX::TBLID);
            printPart_uint16(pos, "OX_W_ID");
            printPart_uint16(pos, "OX_R_ID");
            printPart_uint32(pos, "OX_C_ID");
            printPart_uint16(pos, "OX_T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblORDER_INDEX::Row::print() {
        FILE * f = stdout;
        fprintf(f, "ORDER_INDEX::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint32(OX_O_ID, "OX_O_ID");
        }
        fprintf(f, "}");
    }

}; // TPCC
