//
//  TblORDER_LINE.cpp
//
//  Key and row descriptions for the TPC-C ORDER_LINE table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <time.h>
#include "tpcc/TblORDER_LINE.h"

namespace TPCC {

    void TblORDER_LINE::Key::init(uint16_t ol_w_id, uint16_t ol_d_id, uint32_t ol_o_id, uint16_t ol_number, uint16_t ol_t_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = ol_w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = ol_d_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = ol_o_id;  p += sizeof(uint32_t);
        *((uint16_t *)(p)) = ol_number;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = ol_t_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblORDER_LINE::Row::populate( DataGen &dg, uint32_t ol_o_id,
    		uint16_t ol_w_id, time_t delivery_d ) {
        reset();
        { /* OL_I_ID */
        	uint32_t iid = dg.NURand(8191, 1, 100000, dg.C_for_OL_I_ID );
            putCol_uint32( OL_I_ID, iid );
        }
        { /* OL_SUPPLY_W_ID */
			uint16_t swid = ol_w_id;
        	putCol_uint16( OL_SUPPLY_W_ID, swid );
        }
        { /* OL_DELIVERY_D */
        	if( delivery_d != ((time_t)(0)) ) {
				putCol_time( OL_DELIVERY_D, delivery_d );
            } else {
            	// NULL: putCol( OL_DELIVERY_D, 0, 0 );
            }
        }
        { /* OL_QUANTITY */
			putCol_uint16( OL_QUANTITY, 5 );
        }
        { /* OL_AMOUNT */
        	double olamt = 0.0;
        	if( delivery_d != ((time_t)(0)) ) {
        		olamt = 0.01 * dg.uniformInt(1, 1000000);
			}
			putCol_double( OL_AMOUNT, olamt );
        }
        { /* OL_DIST_INFO */
            char ol_dist_info[24];
            size_t len = dg.randomAlphanumericString(ol_dist_info, 24, 25);
            putCol( OL_DIST_INFO, (uint8_t *)(ol_dist_info), len );
        }
    }
    
    void TblORDER_LINE::Key::print() {
        FILE * f = stdout;
        fprintf(f, "ORDER_LINE::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblORDER_LINE::TBLID);
            printPart_uint16(pos, "OL_W_ID");
            printPart_uint16(pos, "OL_D_ID");
            printPart_uint32(pos, "OL_O_ID");
            printPart_uint16(pos, "OL_NUMBER");
            printPart_uint16(pos, "OL_T_ID");
        }
        fprintf(f, "}");
    }
    
    void TblORDER_LINE::Row::print() {
        FILE * f = stdout;
        fprintf(f, "ORDER_LINE::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_uint32(OL_I_ID, "OL_I_ID");
            printCol_uint16(OL_SUPPLY_W_ID, "OL_SUPPLY_W_ID");
            printCol_time(OL_DELIVERY_D, "OL_DELIVERY_D");
            printCol_uint16(OL_QUANTITY, "OL_QUANTITY");
            printCol_double(OL_AMOUNT, "OL_AMOUNT");
            printCol_string(OL_DIST_INFO, "OL_DIST_INFO");
        }
        fprintf(f, "}");
    }

}; // TPCC
