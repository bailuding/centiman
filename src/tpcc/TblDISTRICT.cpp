//
//  TblDISTRICT.cpp
//
//  Key and row descriptions for the TPC-C DISTRICT table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include "tpcc/TblDISTRICT.h"

namespace TPCC {

    void TblDISTRICT::Key::init(uint16_t w_id, uint16_t d_id) {
        uint8_t buf[MAX_LENGTH];
        uint8_t * p = buf;
        *((uint16_t *)(p)) = TBLID;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = w_id;  p += sizeof(uint16_t);
        *((uint16_t *)(p)) = d_id;  p += sizeof(uint16_t);
        copyFrom(buf, (p-buf));
    }

        
    void TblDISTRICT::Row::populate( DataGen &dg ) {
        reset();
        { /* D_NAME */
            char d_name[10];
            size_t len = dg.randomAlphanumericString(d_name, 6, 11);
            putCol( D_NAME, (uint8_t *)(d_name), len );
        }
        { /* D_STREET_1 */
            char d_street_1[20];
            size_t len = dg.randomAlphanumericString(d_street_1, 10, 21);
            putCol( D_STREET_1, (uint8_t *)(d_street_1), len );
        }
        { /* D_STREET_2 */
            char d_street_2[20];
            size_t len = dg.randomAlphanumericString(d_street_2, 10, 21);
            putCol( D_STREET_2, (uint8_t *)(d_street_2), len );
        }
        { /* D_CITY */
            char d_city[20];
            size_t len = dg.randomAlphanumericString(d_city, 10, 21);
            putCol( D_CITY, (uint8_t *)(d_city), len );
        }
        { /* D_STATE */
            char d_state[2];
            (void)dg.randomAlphanumericString(d_state, 2, 3);
            putCol( D_STATE, (uint8_t *)(d_state), 2 );
        }
        { /* D_ZIP */
            char d_zip[9];
            dg.randomZipCode(d_zip);
            putCol( D_ZIP, (uint8_t *)(d_zip), (sizeof d_zip) );
        }
        { /* D_TAX */
            putCol_double( D_TAX, ((double)(dg.uniformInt(0, 2001))) / 10000.0 );
        }
    }
    
    void TblDISTRICT::Key::print() {
        FILE * f = stdout;
        fprintf(f, "DISTRICT::Key { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            uint16_t pos = 0;
            printTBLID(pos, TblDISTRICT::TBLID);
            printPart_uint16(pos, "D_W_ID");
            printPart_uint16(pos, "D_ID");
        }
        fprintf(f, "}");
    }
    
    void TblDISTRICT::Row::print() {
        FILE * f = stdout;
        fprintf(f, "DISTRICT::Row { " );
        if( mVal == 0 ) {
            fprintf(f, "(NULL) ");
        } else {
            printHdr();
            printCol_string(D_NAME, "D_NAME");
            printCol_string(D_STREET_1, "D_STREET_1");
            printCol_string(D_STREET_2, "D_STREET_2");
            printCol_string(D_CITY, "D_CITY");
            printCol_string(D_STATE, "D_STATE");
            printCol_string(D_ZIP, "D_ZIP");
            printCol_double(D_TAX, "D_TAX");
        }
        fprintf(f, "}");
    }

}; // TPCC
