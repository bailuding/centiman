//
//  TblORDER_LINE.h
//
//  Key and row descriptions for the TPC-C ORDER_LINE table
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblORDER_LINE__
#define __TPCC__TblORDER_LINE__

#include <stdint.h>
#include <time.h>
#include "tpcc/PKey.h"
#include "tpcc/PRow.h"
#include "tpcc/DataGen.h"
#include "tpcc/DBConn.h"

namespace TPCC {

    class TblORDER_LINE {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // OL_W_ID		uint16  [0..W) = O_W_ID
            // OL_D_ID		uint16  [0..10) = O_D_ID
            // OL_O_ID		uint32  [0..3000) = O_O_ID
            // OL_NUMBER	uint16  [0..O_OL_CNT)
            // OL_T_ID	    uint16  [0..10)
//            static const unsigned MAX_LENGTH	= 12;
            static const unsigned MAX_LENGTH	= 14;

            void init(uint16_t ol_w_id, uint16_t ol_d_id, 
                    uint32_t ol_o_id, uint16_t ol_number, uint16_t ol_t_id);
            
            void print();
 
/*            Key(uint16_t ol_w_id, uint16_t ol_d_id, uint32_t ol_o_id, uint16_t ol_number) : PKey() {
                init( ol_w_id, ol_d_id, ol_o_id, ol_number );
            }*/

            Key(uint16_t ol_w_id, uint16_t ol_d_id, 
                    uint32_t ol_o_id, uint16_t ol_number, uint16_t ol_t_id) : PKey() {
                init( ol_w_id, ol_d_id, ol_o_id, ol_number, ol_t_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key



        class Row : public PRow {
        public:
            static const unsigned OL_I_ID			= 0;	// uint32
            static const unsigned OL_SUPPLY_W_ID	= 1;	// uint16
            static const unsigned OL_DELIVERY_D		= 2;    // time_t
            static const unsigned OL_QUANTITY		= 3;	// uint16
            static const unsigned OL_AMOUNT			= 4;    // double
            static const unsigned OL_DIST_INFO		= 5;    // char(24)
            
            static const unsigned NUM_COLUMNS  = 6;
            static const unsigned MAX_LENGTH   = 48;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg, uint32_t ol_o_id, uint16_t ol_w_id, time_t delivery_d );

            void print();
            
            Row() : PRow() {}
            ~Row() {}
        }; // Row


        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t ol_w_id, uint16_t ol_d_id, 
                uint32_t ol_o_id, uint16_t ol_number, uint16_t ol_t_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_t_id), &pr, &pSeqnum );
            return pr;
        }
        
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

}; // TblORDER_LINE

}; // TPCC

#endif /* __TPCC__TblORDER_LINE__ */

