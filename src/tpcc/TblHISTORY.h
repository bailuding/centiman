//
//  TblHISTORY.h
//
//  Key and row descriptions for the TPC-C HISTORY table
//  There is no natural key for this -- each terminal must generate
//    its own unique keys.  This is a hack.
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblHISTORY__
#define __TPCC__TblHISTORY__

#include <stdint.h>
#include <time.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblHISTORY {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // H_TERMINAL	uint16  [0..numTerminals)
            // H_ID			uint32  [0...)
            static const unsigned MAX_LENGTH	= 8;

            void init(uint16_t h_terminal, uint32_t h_id);
            
            void print();
            
            Key(uint16_t h_terminal, uint32_t h_id) : PKey() {
                init( h_terminal, h_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key


        class Row : public PRow {
        public:
            static const unsigned H_C_W_ID  = 0;    // uint16
            static const unsigned H_C_D_ID	= 1;    // uint16
            static const unsigned H_C_ID	= 2;    // uint32
            static const unsigned H_W_ID	= 3;	// uint16
            static const unsigned H_D_ID	= 4;    // uint16
            static const unsigned H_DATE	= 5;    // time_t
            static const unsigned H_AMOUNT	= 6;    // double
            static const unsigned H_DATA	= 7;	// varchar(24)
            
            static const unsigned NUM_COLUMNS  = 8;
            static const unsigned MAX_LENGTH   = 52;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg, uint16_t w_id, uint16_t d_id, uint32_t c_id );
            
            void print();

            Row() : PRow() {}
            ~Row() {}
        }; // Row
        
        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t h_terminal, uint32_t h_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( h_terminal, h_id), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }
    }; // TblHISTORY

}; // TPCC

#endif /* __TPCC__TblHISTORY__ */

