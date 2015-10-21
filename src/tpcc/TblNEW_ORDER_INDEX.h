//
//  TblNEW_ORDER_INDEX.h
//
//  Key and row descriptions for the TPC-C NEW_ORDER_INDEX table
//  Map Warehouse + Region to oldest unfilled order:
//    [W_ID, D_ID] -> [O_ID]
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblNEW_ORDER_INDEX__
#define __TPCC__TblNEW_ORDER_INDEX__

#include <stdint.h>
#include <time.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblNEW_ORDER_INDEX {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // NOX_W_ID		uint16  [0..W)
            // NOX_D_ID		uint16  [0..10)
            // NOX_T_ID     uint16  [0..10)
//            static const unsigned MAX_LENGTH	= 6;
            static const unsigned MAX_LENGTH	= 8;

//            void init(uint16_t nox_w_id, uint16_t nox_d_id);
            void init(uint16_t nox_w_id, uint16_t nox_d_id, uint16_t nox_t_id);
            
            void print();
            
//            Key(uint16_t nox_w_id, uint16_t nox_d_id) : PKey() {
//                init( nox_w_id, nox_d_id );
//            }
            Key(uint16_t nox_w_id, uint16_t nox_d_id, uint16_t nox_t_id) : PKey() {
                init( nox_w_id, nox_d_id, nox_t_id );
            }
            Key() : PKey() {}
            ~Key() {}
        }; // Key



        class Row : public PRow {
        public:
            static const unsigned NOX_O_ID		= 0;	// uint32
            
            static const unsigned NUM_COLUMNS	= 1;
            static const unsigned MAX_LENGTH	= 4;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg, uint32_t nox_o_id );

            void print();
            
            Row() : PRow() {}
            ~Row() {}
        }; // Row

        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t nox_w_id, uint16_t nox_d_id, uint16_t nox_t_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( nox_w_id, nox_d_id, nox_t_id ), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

}; // TblNEW_ORDER_INDEX
    
}; // TPCC

#endif /* __TPCC__TblNEW_ORDER_INDEX__ */

