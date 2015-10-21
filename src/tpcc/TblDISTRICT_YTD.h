//
//  TblDISTRICT_YTD.h
//
//  Key and row descriptions for the TPC-C DISTRICT table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblDISTRICT_YTD__
#define __TPCC__TblDISTRICT_YTD__

#include <stdint.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblDISTRICT_YTD {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID	uint16
            // D_W_ID	uint16      [0..W)
            // D_ID		uint16      [0..10)
            // T_ID     uint16      [0..10)
            static const unsigned MAX_LENGTH	= 8;

            void init(uint16_t w_id, uint16_t d_id, uint16_t t_id);
            
            void print();
            
            Key(uint16_t w_id, uint16_t d_id, uint16_t t_id) : PKey() {
                init( w_id, d_id, t_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key


        class Row : public PRow {
        public:
            static const unsigned D_YTD        = 0;    // double
            
            static const unsigned NUM_COLUMNS  = 1;
            static const unsigned MAX_LENGTH   = 8;
            
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
        
        static Row * readRow( DBConn & conn, uint16_t w_id, uint16_t d_id, uint16_t t_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( w_id, d_id, t_id ), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

}; // TblDISTRICT_YTD
    
}; // TPCC

#endif /* __TPCC__TblDISTRICT_YTD__ */

