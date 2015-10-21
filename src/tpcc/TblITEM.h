//
//  TblITEM.h
//
//  Key and row descriptions for the TPC-C ITEM table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblITEM__
#define __TPCC__TblITEM__

#include <stdint.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblITEM {
    public:
        static const unsigned TBLID;
        
        class Key : public PKey {
        public:
            // TBLID	uint16
            // I_ID		uint32      [0..100000)
            static const unsigned MAX_LENGTH	= 6;

            void init(uint32_t i_id);
            
            void print();
            
            Key(uint32_t i_id) : PKey() {
                init(i_id);
            }
            Key() : PKey() {}
            ~Key() {}
        }; // Key


        class Row : public PRow {
        public:
            static const unsigned I_IM_ID  = 0;    // uint32
            static const unsigned I_NAME   = 1;    // varchar(24)
            static const unsigned I_PRICE  = 2;    // double
            static const unsigned I_DATA   = 3;    // varchar(50)
            
            static const unsigned NUM_COLUMNS  = 4;
            static const unsigned MAX_LENGTH   = 86;
            
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

        static Row * readRow( DBConn & conn, uint32_t i_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key(i_id), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }
        
    }; // TblITEM

}; // TPCC

#endif /* __TPCC__TblITEM__ */

