//
//  TblORDER.h
//
//  Key and row descriptions for the TPC-C ORDER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblORDER__
#define __TPCC__TblORDER__

#include <stdint.h>
#include <time.h>
#include <tpcc/PKey.h>
#include <tpcc/PRow.h>
#include <tpcc/DataGen.h>
#include <tpcc/DBConn.h>

#include <storage/storage-hash-table.h>
#include <util/const.h>

namespace TPCC {

    
    class TblORDER {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // O_W_ID		uint16      [0..W)
            // O_D_ID		uint16      [0..10)
            // O_ID			uint32      [0..10000000)
            // O_T_ID		uint16      [0..10)
//            static const unsigned MAX_LENGTH	= 10;
            static const unsigned MAX_LENGTH	= 12;


//            void init(uint16_t o_w_id, uint16_t o_d_id, uint32_t o_id);
            void init(uint16_t o_w_id, uint16_t o_d_id, 
                    uint32_t o_id, uint16_t o_t_id);
            
            void print();
            
 /*           Key(uint16_t o_w_id, uint16_t o_d_id, uint32_t o_id) : PKey() {
                init( o_w_id, o_d_id, o_id );
            }*/

           Key(uint16_t o_w_id, uint16_t o_d_id, 
                uint32_t o_id, uint16_t o_t_id) : PKey() {
                init( o_w_id, o_d_id, o_id, o_t_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key



        class Row : public PRow {
        public:
            static const unsigned O_C_ID		= 0;	// uint32
            static const unsigned O_ENTRY_D		= 1;	// time_t
            static const unsigned O_CARRIER_ID	= 2;    // uint16
            static const unsigned O_OL_CNT		= 3;	// uint16
            static const unsigned O_ALL_LOCAL	= 4;    // uint16 as bool
            
            static const unsigned NUM_COLUMNS  = 5;
            static const unsigned MAX_LENGTH   = 18;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg, uint32_t o_c_id, time_t entry_d, uint16_t ol_cnt, bool delivered );

            void print();
            
            Row() : PRow() {}
            ~Row() {}
        }; // Row
        

        static int read( DBConn & conn, Key * pKey, Row ** ppValue, Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t o_w_id, uint16_t o_d_id, uint32_t o_id, uint16_t o_t_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( o_w_id, o_d_id, o_id, o_t_id ), &pr, &pSeqnum );
            return pr;
        }
 
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

}; // TblORDER

}; // TPCC

#endif /* __TPCC__TblORDER__ */

