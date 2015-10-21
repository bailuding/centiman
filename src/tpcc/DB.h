//
//  DB.h
//  centiman TPCC
//
//  This holds a client connection to a TPCC database
//  It is the view a single TPCC terminal has
//
//  Created by Alan Demers on 10/29/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__DBGen__
#define __TPCC__DBGen__

#include <stdint.h>

#include <storage/storage-hash-table.h>
#include <tpcc/DBConn.h>
#include <tpcc/DataGen.h>
#include <util/packet.h>
#include <util/queue.h>

namespace TPCC {

    class DB {
        
        unsigned which;
        
        DBConn conn;
        
        bool TRACE;

        uint16_t terminal_id;
        uint16_t terminal_w_id;
       
        static unsigned numTERMINALs;
        static unsigned numWAREHOUSEs;
        static unsigned numITEMs;
        static unsigned numSTOCKs;
        static unsigned numDISTRICTs_per_WAREHOUSE;
        static unsigned numCUSTOMERs_per_DISTRICT;
        static unsigned numORDERs_per_DISTRICT;
        
        void genRowITEM(uint32_t i_id);
        void genRowWAREHOUSE(uint16_t w_id);
        void genRowWAREHOUSE_YTD(uint16_t w_id, uint16_t t_id);
        void genRowSTOCK(uint16_t w_id, uint32_t i_id);
        void genRowDISTRICT(uint16_t w_id, uint16_t d_id);
        void genRowDISTRICT_YTD(uint16_t w_id, uint16_t d_id, uint16_t t_id);
        void genRowDISTRICT_NEXT_O_ID(uint16_t w_id, uint16_t d_id, uint16_t t_id, 
                uint32_t d_next_o_id);
        void genRowCUSTOMER(uint16_t w_id, uint16_t d_id, uint32_t c_id);
        void genRowCUSTOMER_PAYMENT(uint16_t w_id, uint16_t d_id, uint32_t c_id);
        void genRowHISTORY(uint16_t t_id, uint32_t h_id,
                        uint16_t w_id, uint16_t d_id, uint32_t c_id);
        void genRowORDER(uint16_t w_id, uint16_t d_id, uint32_t o_id, uint32_t c_id,
                      time_t entry_d, uint16_t ol_cnt, bool isDelivered, uint16_t t_id);
        void genRowORDER_LINE(uint16_t w_id, uint16_t d_id, uint32_t o_id,
                           uint16_t ol_num, time_t delivery_d, uint16_t t_id);
        void genRowNEW_ORDER(uint16_t w_id, uint16_t d_id, uint32_t o_id, uint16_t t_id);
 
        void genRowITEM(StorageHashTable &, int id, uint32_t i_id);
        void genRowWAREHOUSE(StorageHashTable &, int id, uint16_t w_id);
        void genRowWAREHOUSE_YTD(StorageHashTable &, int id, uint16_t w_id, uint16_t t_id);
        void genRowSTOCK(StorageHashTable &, int id, uint16_t w_id, uint32_t i_id);
        void genRowDISTRICT(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id);
        void genRowDISTRICT_YTD(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint16_t t_id);
        void genRowDISTRICT_NEXT_O_ID(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint16_t t_id, 
                uint32_t d_next_o_id);
        void genRowCUSTOMER(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint32_t c_id);
        void genRowCUSTOMER_PAYMENT(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint32_t c_id);
        void genRowHISTORY(StorageHashTable &, int id, uint16_t t_id, uint32_t h_id,
                        uint16_t w_id, uint16_t d_id, uint32_t c_id);
        void genRowORDER(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint32_t o_id, uint32_t c_id,
                        time_t entry_d, uint16_t ol_cnt, bool isDelivered, uint16_t t_id);
        void genRowORDER_LINE(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint32_t o_id,
                           uint16_t ol_num, time_t delivery_d, uint16_t t_id);
        void genRowNEW_ORDER(StorageHashTable &, int id, uint16_t w_id, uint16_t d_id, uint32_t o_id, uint16_t t_id);
        
    public:
        
        DataGen dg;

        void setTRACE(bool true4on) { TRACE = true4on; }
        void setTerminal(uint16_t terminal_id, uint16_t w_id) {
            this->terminal_id = terminal_id;
            this->terminal_w_id = w_id;
//            fprintf(stdout, "DB: t_id %u w_id %u\n", terminal_id, w_id); fflush(stdout);
        }
       
        void setNumWAREHOUSEs(unsigned num)
        {
            numWAREHOUSEs = num;
        }
        
        unsigned getNumWAREHOUSEs()
        {
            return numWAREHOUSEs;
        }

        DBConn & getConn() { return conn; }
        
        void setIsolation(int pid) { conn.setIsolation(pid); }
        int beginTxn() { return conn.beginTxn(); }
        int getTxn() { return conn.getCurrentTxn(); }
        int commitTxn() { return conn.commit(); }
        int abortTxn() { return conn.abort(); }
        
        
        int generate();
        int generate(StorageHashTable &, int id);
        
        int txnStockLevel();
        int txnDelivery();
        int txnOrderStatus();
        int txnPayment();
        int txnNewOrder();
        
        DB(unsigned which = 0, int32_t randomSeed = 17);
        
        DB(unsigned which, int32_t randomSeed, int nW);

        DB(unsigned which, int32_t randomSeed,
            int nW,
            int id, Queue<Packet> * inQueue, Queue<Packet> * outQueue);
        
        DB(unsigned which, int32_t randomSeed,
           int nW, int nI, int nDperW, int nCperD, int nOperD);
        
        DB(DB & db, int32_t randomSeed = 17);

        ~DB();
    };
};

#endif /* defined(__TPCC__DBGen__) */
