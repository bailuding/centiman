//
//  TblCUSTOMER_INDEX.h
//
//  Key and row descriptions for the TPC-C CUSTOMER_INDEX table
//
//  Map a last name to a list of customer IDs:
//    [W_ID, R_ID, C_LAST] -> [C_ID_0 ... C_ID_9]
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__TblCUSTOMER_INDEX__
#define __TPCC__TblCUSTOMER_INDEX__

#include <stdint.h>
#include <tpcc/PKey.h>
#include <tpcc/PRow.h>
#include <tpcc/DataGen.h>
#include <tpcc/DBConn.h>
#include <storage/storage-hash-table.h>
#include <util/const.h>

namespace TPCC {

    class TblCUSTOMER_INDEX {
    public:
        static const unsigned TBLID;

        class Key : public PKey {
        public:
            // TBLID		uint16
            // CX_W_ID		uint16      [0..W)
            // CX_D_ID		uint16      [0..10)
            // CX_LAST		varchar(16) = C_LAST
            static const unsigned MAX_LENGTH	= 22;

            void init( uint16_t cx_w_id, uint16_t cx_d_id,
                      const uint8_t * cx_last, uint16_t cx_last_len );

            void print();
            
            Key( uint16_t cx_w_id, uint16_t cx_d_id,
                              const uint8_t * cx_last, uint16_t cx_last_len) : PKey() {
                init( cx_w_id, cx_d_id, cx_last, cx_last_len );
            }
            Key() : PKey() {}
            ~Key() {}
        }; // Key


        class Row : public PRow {
        public:
            static const unsigned CX_C_ID_0			= 0;    // uint32
            static const unsigned CX_C_ID_1			= 1;    // uint32
            static const unsigned CX_C_ID_2			= 2;    // uint32
            static const unsigned CX_C_ID_3			= 3;    // uint32
            static const unsigned CX_C_ID_4			= 4;    // uint32
            static const unsigned CX_C_ID_5			= 5;    // uint32
            static const unsigned CX_C_ID_6			= 6;    // uint32
            static const unsigned CX_C_ID_7			= 7;    // uint32
            static const unsigned CX_C_ID_8			= 8;    // uint32
            static const unsigned CX_C_ID_9			= 9;    // uint32
            static const unsigned CX_C_NID          = 10;   // uint16
            
            static const unsigned NUM_COLUMNS  = 11;
            static const unsigned MAX_LENGTH   = 42;
            
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
        
        static Row * readRow( DBConn & conn, uint16_t cx_w_id, uint16_t cx_d_id,
                             const uint8_t * cx_last, uint16_t cx_last_len ) {
            Row * pr = 0;
            Key * pKey = new Key( cx_w_id, cx_d_id, cx_last, cx_last_len );
            Seqnum * pSeqnum = NULL;
            (void)read( conn, pKey, &pr, &pSeqnum );
            return pr;
        }
       
        static Row * readRow( StorageHashTable & hashTable, uint16_t cx_w_id, uint16_t cx_d_id,
                             const uint8_t * cx_last, uint16_t cx_last_len ) {
            Row * pr = new Row();
            Key * pKey = new Key( cx_w_id, cx_d_id, cx_last, cx_last_len );
            Seqnum seqnum = Const::SEQNUM_NULL;
            hashTable.get(*pKey, (::Value *)pr, &seqnum);
            return pr;
        }
       

        static int requestWrite( DBConn & conn, Key * pKey, Row * pValue ) {
            int ans = conn.requestWrite( (::Key *)(pKey), (::Value *)(pValue) );
            return ans;
        }

}; // TblCUSTOMER_INDEX

}; // TPCC

#endif /* __TPCC__TblCUSTOMER_INDEX__ */

