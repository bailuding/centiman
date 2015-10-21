//
//  TblWAREHOUSE.cpp
//
//  Key and row descriptions for the TPC-C WAREHOUSE table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <cstdio>

#include "tpcc/TblWAREHOUSE.h"

namespace TPCC {

    void TblWAREHOUSE::Key::init(uint16_t w_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblWAREHOUSE::Row::populate( DataGen &dg ) {
        reset();
        { /* W_NAME */
            char w_name[10];
            size_t len = dg.randomAlphanumericString(w_name, 6, 11);
            putCol( W_NAME, (uint8_t *)(w_name), (uint16_t)(len) );
        }
        { /* W_STREET_1 */
            char w_street_1[20];
            size_t len = dg.randomAlphanumericString(w_street_1, 10, 21);
            putCol( W_STREET_1, (uint8_t *)(w_street_1), (uint16_t)(len) );
        }
        { /* W_STREET_2 */
            char w_street_2[20];
            size_t len = dg.randomAlphanumericString(w_street_2, 10, 21);
            putCol( W_STREET_2, (uint8_t *)(w_street_2), (uint16_t)(len) );
        }
        { /* W_CITY */
            char w_city[20];
            size_t len = dg.randomAlphanumericString(w_city, 10, 21);
            putCol( W_CITY, (uint8_t *)(w_city), (uint16_t)(len) );
        }
        { /* W_STATE */
            char w_state[2];
            dg.randomAlphanumericString(w_state, 2, 3);
            putCol( W_STATE, (uint8_t *)(w_state), 2 );
        }
        { /* W_ZIP */
            char w_zip[9];
            dg.randomZipCode(w_zip);
            putCol( W_ZIP, (uint8_t *)(w_zip), (sizeof w_zip) );
        }
        { /* W_TAX */
            putCol_double( W_TAX, ((double)(dg.uniformInt(0, 2001))) / 10000.0 );
        }
    }

    void TblWAREHOUSE::Key::print() {
        FILE * f = stdout;
        fprintf(f, "WAREHOUSE::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblWAREHOUSE::TBLID);
            printPart_uint16(pos, "W_ID");
        }
        fprintf(f, "}");
    }
    
    void TblWAREHOUSE::Row::print() {
        FILE * f = stdout;
        fprintf(f, "WAREHOUSE::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_string(W_NAME, "W_NAME");
            printCol_string(W_STREET_1, "W_STREET_1");
            printCol_string(W_STREET_2, "W_STREET_2");
            printCol_string(W_CITY, "W_CITY");
            printCol_string(W_STATE, "W_STATE");
            printCol_string(W_ZIP, "W_ZIP");
            printCol_double(W_TAX, "W_TAX");
        }
        fprintf(f, "}");
    }
    
}; // TPCC
