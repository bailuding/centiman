//
//  TblCUSTOMER.cpp
//
//  Key and row descriptions for the TPC-C CUSTOMER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include "tpcc/TblCUSTOMER.h"

namespace TPCC {

    void TblCUSTOMER::Key::init(uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = d_id;  p += sizeof(uint16_t);
        *((uint32_t *)(p)) = c_id;  p += sizeof(uint32_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblCUSTOMER::Row::populate( DataGen &dg, uint32_t c_id, time_t c_since) {
        // reset( NUM_COLUMNS, MAX_LENGTH );
        reset();
        { /* C_FIRST */
            char c_first[16];
            size_t len = dg.randomAlphanumericString(c_first, 8, 17);
            putCol( C_FIRST, (uint8_t *)(c_first), len );
        }
        { /* C_MIDDLE */
            putCol( C_MIDDLE, (const uint8_t *)("OE"), 2 );
        }
        { /* C_LAST */
            char c_last[16];
            size_t len = dg.randomCustomerName( c_last, c_id );
            putCol( C_LAST, (uint8_t *)(c_last), len );
        }
        { /* C_STREET_1 */
            char c_street_1[20];
            size_t len = dg.randomAlphanumericString(c_street_1, 10, 21);
            putCol( C_STREET_1, (uint8_t *)(c_street_1), len );
        }
        { /* C_STREET_2 */
            char c_street_2[20];
            size_t len = dg.randomAlphanumericString(c_street_2, 10, 21);
            putCol( C_STREET_2, (uint8_t *)(c_street_2), len );
        }
        { /* C_CITY */
            char c_city[20];
            size_t len = dg.randomAlphanumericString(c_city, 10, 21);
            putCol( C_CITY, (uint8_t *)(c_city), len );
        }
        { /* C_STATE */
            char c_state[2];
            (void)dg.randomAlphanumericString(c_state,
            	(sizeof c_state), 1+(sizeof c_state) );
            putCol( C_STATE, (uint8_t *)(c_state), (sizeof c_state) );
        }
        { /* C_ZIP */
            char c_zip[9];
            dg.randomZipCode(c_zip);
            putCol( C_ZIP, (uint8_t *)(c_zip), (sizeof c_zip) );
        }
        { /* C_PHONE */
            char c_phone[16];
            (void)dg.randomAlphanumericString( c_phone,
            	(sizeof c_phone), 1+(sizeof c_phone) );
            putCol( C_PHONE, (uint8_t *)(c_phone), (sizeof c_phone) );
        }
        { /* C_SINCE */
            putCol_uint64( C_SINCE, c_since );
        }
        { /* C_CREDIT */
            putCol( C_CREDIT, (uint8_t *)(dg.flip(0.1) ? "BC" : "GC"), 2 );
        }
        { /* C_CREDIT_LIM */
            putCol_double( C_CREDIT_LIM, 50000.00 );
        }
        { /* C_DISCOUNT */
            putCol_double( C_DISCOUNT, 0.00001 * dg.uniformInt(0, 5001) );
        }
    }
    
    void TblCUSTOMER::Key::print() {
        FILE * f = stdout;
        fprintf(f, "CUSTOMER::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblCUSTOMER::TBLID);
            printPart_uint16(pos, "C_W_ID");
            printPart_uint16(pos, "C_D_ID");
            printPart_uint32(pos, "C_ID");
        }
        fprintf(f, "}");
    }

    void TblCUSTOMER::Row::print() {
        FILE * f = stdout;
        fprintf(f, "CUSTOMER::Row { ");
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_string(C_FIRST, "C_FIRST");
            printCol_string(C_MIDDLE, "C_MIDDLE");
            printCol_string(C_LAST, "C_LAST");
            printCol_string(C_STREET_1, "C_STREET_1");
            printCol_string(C_STREET_2, "C_STREET_2");
            printCol_string(C_CITY, "C_CITY");
            printCol_string(C_STATE, "C_STATE");
            printCol_string(C_ZIP, "C_ZIP");
            printCol_string(C_PHONE, "C_PHONE");
            printCol_time(C_SINCE, "C_SINCE");
            printCol_string(C_CREDIT, "C_CREDIT");
            printCol_double(C_CREDIT_LIM, "C_CREDIT_LIM");
            printCol_double(C_DISCOUNT, "C_DISCOUNT");
        }
        fprintf(f, "}");
    }

    
}; // TPCC
