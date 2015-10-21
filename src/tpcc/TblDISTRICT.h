//
//  TblDISTRICT.h
//
//  Key and row descriptions for the TPC-C DISTRICT table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblDISTRICT__
#define __TPCC__TblDISTRICT__

#include <stdint.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblDISTRICT {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID	uint16
            // D_W_ID	uint16      [0..W)
            // D_ID		uint16      [0..10)
            static const unsigned MAX_LENGTH	= 6;

            void init(uint16_t w_id, uint16_t d_id);
            
            void print();
            
            Key(uint16_t w_id, uint16_t d_id) : PKey() {
                init( w_id, d_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key


        class Row : public PRow {
        public:
            static const unsigned D_NAME       = 0;    // varchar(10)
            static const unsigned D_STREET_1   = 1;    // varchar(20)
            static const unsigned D_STREET_2   = 2;    // varchar(20)
            static const unsigned D_CITY       = 3;    // varchar(20)
            static const unsigned D_STATE      = 4;    // char(2)
            static const unsigned D_ZIP        = 5;    // char(9)
            static const unsigned D_TAX        = 6;    // double
            
            static const unsigned NUM_COLUMNS  = 7;
            static const unsigned MAX_LENGTH   = 89;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg );
            
            void print();

            Row() : PRow() {}
            ~Row() {}
        }; // Row

        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t w_id, uint16_t d_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( w_id, d_id ), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

}; // TblDISTRICT
    
}; // TPCC

#endif /* __TPCC__TblDISTRICT__ */

