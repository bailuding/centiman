//
//  TblNEW_ORDER.h
//
//  Key and row descriptions for the TPC-C NEW_ORDER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblNEW_ORDER__
#define __TPCC__TblNEW_ORDER__

#include <stdint.h>
#include <time.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblNEW_ORDER {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // NO_W_ID		uint16  [0..W)
            // NO_D_ID		uint16  [0..10)
            // NO_O_ID		uint32  [0..10000000)
            // NO_T_ID      uint16  [0..10)
//            static const unsigned MAX_LENGTH	= 10;
            static const unsigned MAX_LENGTH	= 12;

//            void init(uint16_t no_w_id, uint16_t no_d_id, uint32_t no_o_id);
            void init(uint16_t no_w_id, uint16_t no_d_id, uint32_t no_o_id, uint16_t no_t_id);
            
            void print();
            
 //           Key(uint16_t no_w_id, uint16_t no_d_id, uint32_t no_o_id) : PKey() {
//                init( no_w_id, no_d_id, no_o_id );
//            }
            Key(uint16_t no_w_id, uint16_t no_d_id, uint32_t no_o_id, uint16_t no_t_id) : PKey() {
                init( no_w_id, no_d_id, no_o_id, no_t_id );
            }
            
            Key() : PKey() {}
            ~Key() {}
        }; // Key


        class Row : public PRow {
        public:
            static const unsigned NO_X			= 0;	// always NULL
            
            static const unsigned NUM_COLUMNS	= 1;
            static const unsigned MAX_LENGTH	= 0;
            
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
        
        static Row * readRow( DBConn & conn, uint16_t no_w_id, uint16_t no_d_id, 
                uint32_t no_o_id, uint16_t no_t_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( no_w_id, no_d_id, no_o_id, no_t_id ), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

    }; // TblNEW_ORDER

}; // TPCC

#endif /* __TPCC__TblNEW_ORDER__ */

