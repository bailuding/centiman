//
//  TblITEM.cpp
//
//  Key and row descriptions for the TPC-C ITEM table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include <cstdio>
#include "tpcc/TblITEM.h"

namespace TPCC {

    void TblITEM::Key::init(uint32_t i_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = i_id;  p += sizeof(uint32_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblITEM::Row::populate( DataGen &dg ) {
        reset();
        { /* I_IM_ID */
            putCol_uint32( I_IM_ID, ((uint32_t)(dg.uniformInt(1, 10001))) );
        }
        { /* I_NAME */
            char i_name[24];
            size_t len = dg.randomAlphanumericString(i_name, 14, 25);
            putCol( I_NAME, (uint8_t *)(i_name), (uint16_t)(len) );
        }
        { /* I_PRICE */
            putCol_double( I_PRICE, ((double)(dg.uniformInt(100, 10001))) / 100.0 );
        }
        { /* I_DATA */
            char i_data[50];
            size_t len = dg.randomAlphanumericString(i_data, 26, 51);
            if( dg.flip(0.1) ) /* 10 % of the time */ {
                uint32_t pos = dg.uniformInt(0, ((uint32_t)(len))-8);
                memcpy( i_data + pos, "ORIGINAL", 8 );
            }
            putCol( I_DATA, (uint8_t *)(i_data), (uint16_t)(len) );
        }
    }
    
    void TblITEM::Key::print() {
        FILE * f = stdout;
        fprintf(f, "ITEM::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblITEM::TBLID);
            printPart_uint32(pos, "I_ID");
        }
        fprintf(f, "}");
    }
    
    void TblITEM::Row::print() {
        FILE * f = stdout;
        fprintf(f, "ITEM::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint32(I_IM_ID, "I_IM_ID");
            printCol_string(I_NAME, "I_NAME");
            printCol_double(I_PRICE, "I_PRICE");
            printCol_string(I_DATA, "I_DATA");
        }
        fprintf(f, "}");
    }
    
}; // TPCC
