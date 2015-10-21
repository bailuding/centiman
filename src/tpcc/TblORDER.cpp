//
//  TblORDER.cpp
//
//  Key and row descriptions for the TPC-C ORDER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <time.h>
#include "tpcc/TblORDER.h"

namespace TPCC {

    void TblORDER::Key::init(uint16_t o_w_id, uint16_t o_d_id, uint32_t o_id, uint16_t o_t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = o_w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = o_d_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = o_id;  p += sizeof(uint32_t);
        *((uint16_t *)(p)) = o_t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblORDER::Row::populate( DataGen &dg, uint32_t o_c_id, time_t entry_d, uint16_t ol_cnt, bool delivered ) {
        reset();
        { /* O_C_ID */
            putCol_uint32( O_C_ID, o_c_id );
        }
        { /* O_ENTRY_D */
            putCol_time( O_ENTRY_D, entry_d );
        }
        { /* O_CARRIER_ID */
        	if( delivered ) {
				putCol_uint16( O_CARRIER_ID, dg.uniformInt(1, 11) );
            } else {
            	// NULL: putCol( O_CARRIER_ID, 0, 0 );
            }
        }
        { /* O_OL_CNT */
            putCol_uint16( O_OL_CNT, ol_cnt  );
        }
        { /* O_ALL_LOCAL */
            putCol_uint16( O_ALL_LOCAL, 1 );
        }
    }
 
    void TblORDER::Key::print() {
        FILE * f = stdout;
        fprintf(f, "ORDER::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblORDER::TBLID);
            printPart_uint16(pos, "O_W_ID");
            printPart_uint16(pos, "O_D_ID");
            printPart_uint32(pos, "O_ID");
            printPart_uint16(pos, "O_T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblORDER::Row::print() {
        FILE * f = stdout;
        fprintf(f, "ORDER::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint32(O_C_ID, "O_C_ID");
            printCol_time(O_ENTRY_D, "O_ENTRY_D");
            printCol_uint16(O_CARRIER_ID, "O_CARRIER_ID");
            printCol_uint16(O_OL_CNT, "O_OL_CNT");
            printCol_uint16(O_ALL_LOCAL, "O_ALL_LOCAL");
        }
        fprintf(f, "}");
    }

}; // TPCC
