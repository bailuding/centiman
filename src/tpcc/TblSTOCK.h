//
//  TblSTOCK.h
//
//  Key and row descriptions for the TPC-C STOCK table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblSTOCK__
#define __TPCC__TblSTOCK__

#include <stdint.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblSTOCK {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID	uint16
            // W_ID		uint16      [0..W)
            // I_ID		uint32      [0..100000)
            static const unsigned MAX_LENGTH	= 8;

            void init(uint16_t w_id, uint32_t i_id);
            
            void print();
            
            Key(uint16_t w_id, uint32_t i_id) : PKey() {
                init( w_id,  i_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key



        class Row : public PRow {
        public:
            static const unsigned S_QUANTITY   = 0;    // uint16
            static const unsigned S_DIST_01    = 1;    // char(24)
            static const unsigned S_DIST_02    = 2;
            static const unsigned S_DIST_03    = 3;
            static const unsigned S_DIST_04    = 4;
            static const unsigned S_DIST_05    = 5;
            static const unsigned S_DIST_06    = 6;
            static const unsigned S_DIST_07    = 7;
            static const unsigned S_DIST_08    = 8;
            static const unsigned S_DIST_09    = 9;
            static const unsigned S_DIST_10    = 10;
            static const unsigned S_YTD        = 11;   // uint32
            static const unsigned S_ORDER_CNT  = 12;   // uint16
            static const unsigned S_REMOTE_CNT = 13;   // uint16
            static const unsigned S_DATA       = 14;   // varchar(50)
            
            static const unsigned NUM_COLUMNS  = 15;
            static const unsigned MAX_LENGTH   = 300;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg /*, uint16_t w_id, uint32_t i_id */);

            void print();
            
            Row() : PRow() {}
            ~Row() {}
        }; // Row

        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t w_id, uint32_t i_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key(w_id, i_id), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }
        
    }; // TblSTOCK
    
}; // TPCC

#endif /* __TPCC__TblSTOCK__ */

