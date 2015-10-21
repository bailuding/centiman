//
//  TblORDER_INDEX.h
//
//  Key and row descriptions for the TPC-C ORDER_INDEX table
//  Map customer to her most recent order:
//    [W_ID, R_ID, C_ID] -> [O_ID]
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblORDER_INDEX__
#define __TPCC__TblORDER_INDEX__

#include <stdint.h>
#include <tpcc/PKey.h>
#include <tpcc/PRow.h>
#include <tpcc/DataGen.h>
#include <tpcc/DBConn.h>

#include <storage/storage-hash-table.h>
#include <util/const.h>

namespace TPCC {

    class TblORDER_INDEX {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // OX_W_ID		uint16  [0..W) = O_W_ID
            // OX_R_ID		uint16  [0..10) = O_R_ID
            // OX_C_ID		uint32  [0..3000) = O_C_ID
            // OX_T_ID		uint16  [0..10) = O_R_ID
//            static const unsigned MAX_LENGTH	= 10;
            static const unsigned MAX_LENGTH	= 12;

//            void init(uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id);
            void init(uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id, uint16_t ox_t_id);
            
            void print();
            
/*            Key(uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id) : PKey() {
                init( ox_w_id, ox_d_id, ox_c_id );
            }*/
            Key(uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id, uint16_t ox_t_id) : PKey() {
                init( ox_w_id, ox_d_id, ox_c_id, ox_t_id );
            }

            Key() : PKey() {}
            ~Key() {}
        }; // Key



        class Row : public PRow {
        public:
            static const unsigned OX_O_ID		= 0;	// uint32 [0..10000000) = customer's most recent O_ID
            
            static const unsigned NUM_COLUMNS	= 1;
            static const unsigned MAX_LENGTH	= 4;
            
            void reset() { PRow::reset(NUM_COLUMNS, MAX_LENGTH); }
            void populate( DataGen &dg, uint32_t ox_o_id );

            void print();
            
            Row() : PRow() {}
            ~Row() {}
        }; // Row
        

        static int read( DBConn & conn, Key * pKey, Row ** ppValue , Seqnum ** ppSeqnum) {
            int ans = conn.read( pKey, ((::Value **)(ppValue)), ppSeqnum);
            return ans;
        }
        
        static Row * readRow( DBConn & conn, uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id, uint16_t ox_t_id ) {
            Row * pr = 0;
            Seqnum * pSeqnum = NULL;
            (void)read( conn, new Key( ox_w_id, ox_d_id, ox_c_id, ox_t_id ), &pr, &pSeqnum );
            return pr;
        }
        
        static Row * readRow( StorageHashTable & hashTable, 
                uint16_t ox_w_id, uint16_t ox_d_id, uint32_t ox_c_id, uint16_t ox_t_id ) {
            Row * pr = new Row();
            Seqnum seqnum = Const::SEQNUM_NULL;
            hashTable.get(*(new Key( ox_w_id, ox_d_id, ox_c_id, ox_t_id )), (::Value *)pr, &seqnum );
            return pr;
        } 
 
        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

    }; // TblORDER_INDEX

}; // TPCC

#endif /* __TPCC__TblORDER_INDEX__ */

