//
//  TblSTOCK.cpp
//
//  Key and row descriptions for the TPC-C STOCK table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include "tpcc/TblSTOCK.h"

namespace TPCC {

    void TblSTOCK::Key::init(uint16_t w_id, uint32_t i_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = i_id;  p += sizeof(uint32_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblSTOCK::Row::populate( DataGen &dg /*, uint16_t w_id, uint32_t i_id*/ ) {
        reset();
        { /* S_QUANTITY */
            putCol_uint16( S_QUANTITY, (uint16_t)(dg.uniformInt(10, 101)) );
        }
        for( int i = S_DIST_01; i <= S_DIST_10; i++ ) {
            /* S_DIST_(i) */
            char s_dist[24];
            size_t len = dg.randomAlphanumericString( s_dist, (sizeof s_dist), 1+(sizeof s_dist) );
            assert( len == (sizeof s_dist) );
            putCol( i, (uint8_t *)(s_dist), len );
        }
        { /* S_YTD */
            putCol_uint32( S_YTD, 0 );
        }
        { /* S_ORDER_CNT */
            putCol_uint16( S_ORDER_CNT, 0 );
        }
        { /* S_REMOTE_CNT */
            putCol_uint16( S_REMOTE_CNT, 0 );
        }
        { /* S_DATA */
            char s_data[50];
            size_t len = dg.randomAlphanumericString(s_data, 26, 51);
            if( dg.flip(0.1) ) {
                memcpy( (s_data + dg.select(((unsigned)(len)) - 8)), "ORIGINAL", 8 );
            }
            putCol( S_DATA, (uint8_t *)(s_data), (uint16_t)(len) );
        }
    }
    
    void TblSTOCK::Key::print() {
        FILE * f = stdout;
        fprintf(f, "STOCK::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblSTOCK::TBLID);
            printPart_uint16(pos, "S_W_ID");
            printPart_uint32(pos, "S_I_ID");
        }
        fprintf(f, "}");
    }
    
    void TblSTOCK::Row::print() {
        FILE * f = stdout;
        fprintf(f, "STOCK::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint16(S_QUANTITY, "S_QUANTITY");
            printCol_string(S_DIST_01, "S_DIST_01");
            printCol_string(S_DIST_02, "S_DIST_02");
            printCol_string(S_DIST_03, "S_DIST_03");
            printCol_string(S_DIST_04, "S_DIST_04");
            printCol_string(S_DIST_05, "S_DIST_05");
            printCol_string(S_DIST_06, "S_DIST_06");
            printCol_string(S_DIST_07, "S_DIST_07");
            printCol_string(S_DIST_08, "S_DIST_08");
            printCol_string(S_DIST_09, "S_DIST_09");
            printCol_string(S_DIST_10, "S_DIST_10");
            printCol_uint32(S_YTD, "S_YTD");
            printCol_uint16(S_ORDER_CNT, "S_ORDER_CNT");
            printCol_uint16(S_REMOTE_CNT, "S_REMOTE_CNT");
            printCol_string(S_DATA, "S_DATA");
        }
        fprintf(f, "}");
    }
    
}; // TPCC
