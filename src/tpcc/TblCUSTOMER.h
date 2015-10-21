//
//  TblCUSTOMER.h
//
//  Key and row descriptions for the TPC-C CUSTOMER table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblCUSTOMER__
#define __TPCC__TblCUSTOMER__


#include <stdint.h>
#include <time.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblCUSTOMER {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID	uint16
            // C_W_ID	uint16      [0..W)
            // C_D_ID	uint16      [0..10)
            // C_ID		uint32      [0..96000)
            static const unsigned MAX_LENGTH	= 10;

            void init(uint16_t w_id, uint16_t d_id, uint32_t c_id);
            
            void print();
            
            Key(uint16_t w_id, uint16_t d_id, uint32_t c_id) : PKey() {
                init( w_id, d_id, c_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key



        class Row : public PRow {
        public:
            static const unsigned C_FIRST			= 0;    // varchar(16)
            static const unsigned C_MIDDLE			= 1;    // char(2)
            static const unsigned C_LAST			= 2;    // varchar(16)
            static const unsigned C_STREET_1		= 3;    // varchar(20)
            static const unsigned C_STREET_2		= 4;    // varchar(20)
            static const unsigned C_CITY			= 5;    // varchar(20)
            static const unsigned C_STATE			= 6;    // char(2)
            static const unsigned C_ZIP				= 7;    // char(9)
            static const unsigned C_PHONE			= 8;    // varchar(16)
            static const unsigned C_SINCE			= 9;    // time_t
            static const unsigned C_CREDIT			= 10;	// char(2)
            static const unsigned C_CREDIT_LIM		= 11;	// double
            static const unsigned C_DISCOUNT		= 12;	// double
            
            static const unsigned NUM_COLUMNS  = 13;
            static const unsigned MAX_LENGTH   = 147;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg, uint32_t c_id, time_t c_since );
            
            void print();

            Row() : PRow() { reset(); }
            ~Row() {}


        }; // Row

        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t w_id, uint16_t d_id, uint32_t c_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( w_id, d_id, c_id ), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

    }; // TblCUSTOMER
    
}; // TPCC


#endif /* __TPCC__TblCUSTOMER__ */

