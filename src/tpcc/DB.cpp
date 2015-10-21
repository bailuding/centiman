//
//  DB.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 10/29/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include "tpcc/Tables.h"
#include "tpcc/DB.h"

#include <storage/storage-hash-table.h>
#include <util/index.h>

namespace TPCC {
    
    unsigned DB::numTERMINALs;
    unsigned DB::numWAREHOUSEs;
    unsigned DB::numITEMs;
    unsigned DB::numSTOCKs;
    unsigned DB::numDISTRICTs_per_WAREHOUSE;
    unsigned DB::numCUSTOMERs_per_DISTRICT;
    unsigned DB::numORDERs_per_DISTRICT;

    void DB::genRowNEW_ORDER(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint32_t o_id, uint16_t t_id) {
        // this is indexed, but no need to populate the index here b/c empty index == order 0 anyway
        TblNEW_ORDER::Key * pkNO = new TblNEW_ORDER::Key(w_id, d_id, o_id, t_id);
        if (Index::getStorageIdx((::Key *)(pkNO)) == id) {
            TblNEW_ORDER::Row * prNO = new TblNEW_ORDER::Row;
            prNO->populate(dg);
            hashTable.put(*pkNO, *prNO, 0);
            delete prNO;
        }
        delete pkNO;
    }

    void DB::genRowORDER_LINE(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint32_t o_id, uint16_t ol_num, time_t delivery_d, uint16_t t_id) {

        TblORDER_LINE::Key * pkOL = new TblORDER_LINE::Key(w_id, d_id, o_id, ol_num, t_id);
        if (Index::getStorageIdx((::Key *)(pkOL)) == id) {
            TblORDER_LINE::Row * prOL = new TblORDER_LINE::Row;
            prOL->populate(dg, o_id, w_id, delivery_d);
            hashTable.put(*pkOL, *prOL, 0);
            delete prOL;
        }
        delete pkOL;
    }
    
    void DB::genRowORDER(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint32_t o_id, uint32_t c_id,
                          time_t entry_d, uint16_t ol_cnt, bool isDelivered, uint16_t t_id) {
        /* read and update the ORDER_INDEX row ... */
        TblORDER_INDEX::Row * prOI = TblORDER_INDEX::readRow( hashTable, w_id, d_id, c_id, t_id );
        if( prOI->mCnt == 0 ) /* ORDER_INDEX entry nonexistent */ { prOI->reset(); }
        prOI->putCol_uint32( TblORDER_INDEX::Row::OX_O_ID, o_id );
        TblORDER_INDEX::Key * pkOI = new TblORDER_INDEX::Key(w_id, d_id, c_id, terminal_id);
        if (Index::getStorageIdx((::Key *)(pkOI)) == id)
            hashTable.put(*pkOI, *prOI, 0);
        /* insert the ORDER row ... */
        TblORDER::Key * pkO = new TblORDER::Key(w_id, d_id, o_id, t_id);
        TblORDER::Row * prO = new TblORDER::Row;  prO->populate(dg, c_id, entry_d, ol_cnt, isDelivered);
        if (Index::getStorageIdx((::Key *)(pkO)) == id)
            hashTable.put(*pkO, *prO, 0);
        delete pkOI;
        delete pkO;
        delete prO;
    }

    void DB::genRowHISTORY(StorageHashTable & hashTable, int id,
            uint16_t t_id, uint32_t h_id, uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        TblHISTORY::Key * pkH = new TblHISTORY::Key(0, h_id);
        if (Index::getStorageIdx((::Key *)(pkH)) == id) {
            TblHISTORY::Row * prH = new TblHISTORY::Row;
            prH->populate(dg, w_id, d_id, c_id);
            hashTable.put(*pkH, *prH, 0);
            delete prH;
        }
        delete pkH;
    }
    
    void DB::genRowCUSTOMER(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        /* generate the new CUSTOMER row (but do not write it yet) ... */
        TblCUSTOMER::Key * pkC = new TblCUSTOMER::Key(w_id, d_id, c_id);
        TblCUSTOMER::Row * prC = new TblCUSTOMER::Row;
        prC->populate(dg, c_id, time(0));

        /* extract the customer last name ... */
        uint8_t c_last[17];
        uint16_t c_last_len = prC->getCol( TblCUSTOMER::Row::C_LAST, c_last, 16 );
        /* read the CUSTOMER_INDEX row ... */
        TblCUSTOMER_INDEX::Row * prCI = TblCUSTOMER_INDEX::readRow(hashTable , w_id, d_id, c_last, c_last_len );
        /* take care of NULL result (first use of given last name) ... */
        unsigned numIndexEntries;
        if( prCI->mCnt != 0 ) /* CUSTOMER_INDEX entry already exists */ {
            // prCI->print();  fprintf(stdout, "\n");  fflush(stdout); // TODO: XXX <---------------------------------------------------------------
            assert(!(prCI->isNULL(TblCUSTOMER_INDEX::Row::CX_C_NID)));
            numIndexEntries = prCI->getCol_uint16(NULL, TblCUSTOMER_INDEX::Row::CX_C_NID);
        } else /* create new CUSTOMER_INDEX entry */ {
            prCI->reset();
            // prCI->print();  fprintf(stdout, "\n");  fflush(stdout); // TODO: XXX <---------------------------------------------------------------
            numIndexEntries = 0;
        }
        /* message the CUSTOMER_INDEX entry to reflect new customer .... */
        if( numIndexEntries <= (TblCUSTOMER_INDEX::Row::CX_C_ID_9 - TblCUSTOMER_INDEX::Row::CX_C_ID_0) ) {
            prCI->putCol_uint32((TblCUSTOMER_INDEX::Row::CX_C_ID_0 + numIndexEntries), c_id);
            prCI->putCol_uint16( TblCUSTOMER_INDEX::Row::CX_C_NID, numIndexEntries+1 );
            // prCI->print();  fprintf(stdout, "\n");  fflush(stdout); // TODO: XXX <---------------------------------------------------------------
        }
        /* write the CUSTOMER_INDEX row ... */
        TblCUSTOMER_INDEX::Key * pkCI = new TblCUSTOMER_INDEX::Key(w_id, d_id, c_last, c_last_len);
        if (Index::getStorageIdx((::Key *)(pkCI)) == id)
            hashTable.put(*pkCI, *prCI, 0);
        /* write the CUSTOMER row ... */
        if (Index::getStorageIdx((::Key *)(pkC)) == id)
            hashTable.put(*pkC, *prC, 0);
        delete pkC;
        delete prC;
        delete pkCI;
    }
     
    void DB::genRowCUSTOMER_PAYMENT(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        /* generate the new CUSTOMER_PAYMENT row (but do not write it yet) ... */
        TblCUSTOMER_PAYMENT::Key * pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(w_id, d_id, c_id);
        if (Index::getStorageIdx((::Key *)(pkC_PAYMENT)) == id) {
            TblCUSTOMER_PAYMENT::Row * prC_PAYMENT = new TblCUSTOMER_PAYMENT::Row;
            prC_PAYMENT->populate(dg, c_id);
            /* write the CUSTOMER row ... */
            hashTable.put(*pkC_PAYMENT, *prC_PAYMENT, 0);
            delete prC_PAYMENT;
        }
        delete pkC_PAYMENT;
    }

    void DB::genRowDISTRICT(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id) {
        TblDISTRICT::Key * pkD = new TblDISTRICT::Key(w_id, d_id);
        if (Index::getStorageIdx((::Key *)(pkD)) == id) {
            TblDISTRICT::Row * prD = new TblDISTRICT::Row;
            prD->populate(dg);
            assert( !prD->isNULL(TblDISTRICT::Row::D_STREET_1) );
            hashTable.put(*pkD, *prD, 0);
            delete prD;
        }
        delete pkD;
    }

    void DB::genRowDISTRICT_YTD(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint16_t t_id) {
        TblDISTRICT_YTD::Key * pkD_YTD = new TblDISTRICT_YTD::Key(w_id, d_id, t_id);
        if (Index::getStorageIdx((::Key *)(pkD_YTD)) == id) {
            TblDISTRICT_YTD::Row * prD_YTD = new TblDISTRICT_YTD::Row;
            prD_YTD->populate(dg);
            hashTable.put(*pkD_YTD, *prD_YTD, 0);
            delete prD_YTD;
        }
        delete pkD_YTD;
    }
    
    void DB::genRowDISTRICT_NEXT_O_ID(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t d_id, uint16_t t_id, uint32_t d_next_o_id) {
        TblDISTRICT_NEXT_O_ID::Key * pkD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Key(w_id, d_id, t_id);
        if (Index::getStorageIdx((::Key *)(pkD_NEXT_O_ID)) == id) {
            TblDISTRICT_NEXT_O_ID::Row * prD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Row;
            prD_NEXT_O_ID->populate(dg, d_next_o_id);
            hashTable.put(*pkD_NEXT_O_ID, *prD_NEXT_O_ID, 0);
            delete prD_NEXT_O_ID;
        }
        delete pkD_NEXT_O_ID;
    }

    void DB::genRowSTOCK(StorageHashTable & hashTable, int id, 
            uint16_t w_id, uint32_t i_id) {
        TblSTOCK::Key * pkS = new TblSTOCK::Key(w_id, i_id);
        if (Index::getStorageIdx((::Key *)(pkS)) == id) {
            TblSTOCK::Row * prS = new TblSTOCK::Row;
            prS->populate(dg);
            hashTable.put(*pkS, *prS, 0);
            delete prS;
        }
        delete pkS;
    }

    void DB::genRowWAREHOUSE(StorageHashTable & hashTable, int id, 
            uint16_t w_id) {
        TblWAREHOUSE::Key * pkW = new TblWAREHOUSE::Key(w_id);
        if (Index::getStorageIdx((::Key *)(pkW)) == id) {
            TblWAREHOUSE::Row * prW = new TblWAREHOUSE::Row;
            prW->populate(dg);
            hashTable.put(*pkW, *prW, 0);
            delete prW;
        }
        delete pkW;
    }

    void DB::genRowWAREHOUSE_YTD(StorageHashTable & hashTable, int id,
            uint16_t w_id, uint16_t t_id) {
        TblWAREHOUSE_YTD::Key * pkW_YTD = new TblWAREHOUSE_YTD::Key(w_id, t_id);
        if (Index::getStorageIdx((::Key *)(pkW_YTD)) == id) {
            TblWAREHOUSE_YTD::Row * prW_YTD = new TblWAREHOUSE_YTD::Row;
            prW_YTD->populate(dg);
            hashTable.put(*pkW_YTD, *prW_YTD, 0);
            delete prW_YTD;
        }
        delete pkW_YTD;
    }

    void DB::genRowITEM(StorageHashTable & hashTable, int id, 
            uint32_t i_id) {
        TblITEM::Key * pkI = new TblITEM::Key(i_id);
        if (Index::getStorageIdx((::Key *)(pkI)) == id) {
            TblITEM::Row * prI = new TblITEM::Row;
            prI->populate(dg);
            hashTable.put(*pkI, *prI, 0);
            delete prI;
        }
        delete pkI;
    }
   
    /* LocalKVStore */
    void DB::genRowNEW_ORDER(uint16_t w_id, uint16_t d_id, uint32_t o_id, uint16_t t_id) {
        // this is indexed, but no need to populate the index here b/c empty index == order 0 anyway
        TblNEW_ORDER::Key * pkNO = new TblNEW_ORDER::Key(w_id, d_id, o_id, t_id);
        TblNEW_ORDER::Row * prNO = new TblNEW_ORDER::Row;
        prNO->populate(dg);
        conn.requestWrite(pkNO, prNO);
    }
    
    void DB::genRowORDER_LINE(uint16_t w_id, uint16_t d_id, uint32_t o_id, 
            uint16_t ol_num, time_t delivery_d, uint16_t t_id) {
        
        TblORDER_LINE::Key * pkOL = new TblORDER_LINE::Key(w_id, d_id, o_id, ol_num, t_id);
        TblORDER_LINE::Row * prOL = new TblORDER_LINE::Row;
        prOL->populate(dg, o_id, w_id, delivery_d);
        conn.requestWrite(pkOL, prOL);
    }
    
    void DB::genRowORDER(uint16_t w_id, uint16_t d_id, uint32_t o_id, uint32_t c_id,
                          time_t entry_d, uint16_t ol_cnt, bool isDelivered, uint16_t t_id) {
        /* read and update the ORDER_INDEX row ... */
        TblORDER_INDEX::Row * prOI = TblORDER_INDEX::readRow( conn, w_id, d_id, c_id, t_id );
        if( prOI->isNULL() ) /* ORDER_INDEX entry nonexistent */ { prOI->reset(); }
        prOI->putCol_uint32( TblORDER_INDEX::Row::OX_O_ID, o_id );
        TblORDER_INDEX::Key * pkOI = new TblORDER_INDEX::Key(w_id, d_id, c_id, t_id);
        conn.requestWrite(pkOI, prOI);
        /* insert the ORDER row ... */
        TblORDER::Key * pkO = new TblORDER::Key(w_id, d_id, o_id, t_id);
        TblORDER::Row * prO = new TblORDER::Row;  prO->populate(dg, c_id, entry_d, ol_cnt, isDelivered);
        conn.requestWrite(pkO, prO);
    }
    
    void DB::genRowHISTORY(uint16_t t_id, uint32_t h_id, uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        TblHISTORY::Key * pkH = new TblHISTORY::Key(0, h_id);
        TblHISTORY::Row * prH = new TblHISTORY::Row;
        prH->populate(dg, w_id, d_id, c_id);
        conn.requestWrite(pkH, prH);
    }
    
    void DB::genRowCUSTOMER(uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        /* generate the new CUSTOMER row (but do not write it yet) ... */
        TblCUSTOMER::Key * pkC = new TblCUSTOMER::Key(w_id, d_id, c_id);
        TblCUSTOMER::Row * prC = new TblCUSTOMER::Row;
        prC->populate(dg, c_id, time(0));

        /* extract the customer last name ... */
        uint8_t c_last[17];
        uint16_t c_last_len = prC->getCol( TblCUSTOMER::Row::C_LAST, c_last, 16 );
        /* read the CUSTOMER_INDEX row ... */
        TblCUSTOMER_INDEX::Row * prCI = TblCUSTOMER_INDEX::readRow( conn, w_id, d_id, c_last, c_last_len );
        /* take care of NULL result (first use of given last name) ... */
        unsigned numIndexEntries;
        if( !(prCI->isNULL()) ) /* CUSTOMER_INDEX entry already exists */ {
            // prCI->print();  fprintf(stdout, "\n");  fflush(stdout); // TODO: XXX <---------------------------------------------------------------
            assert(!(prCI->isNULL(TblCUSTOMER_INDEX::Row::CX_C_NID)));
            numIndexEntries = prCI->getCol_uint16(NULL, TblCUSTOMER_INDEX::Row::CX_C_NID);
        } else /* create new CUSTOMER_INDEX entry */ {
            prCI->reset();
            // prCI->print();  fprintf(stdout, "\n");  fflush(stdout); // TODO: XXX <---------------------------------------------------------------
            numIndexEntries = 0;
        }
        /* massage the CUSTOMER_INDEX entry to reflect new customer .... */
        if( numIndexEntries <= (TblCUSTOMER_INDEX::Row::CX_C_ID_9 - TblCUSTOMER_INDEX::Row::CX_C_ID_0) ) {
            prCI->putCol_uint32((TblCUSTOMER_INDEX::Row::CX_C_ID_0 + numIndexEntries), c_id);
            prCI->putCol_uint16( TblCUSTOMER_INDEX::Row::CX_C_NID, numIndexEntries+1 );
            // prCI->print();  fprintf(stdout, "\n");  fflush(stdout); // TODO: XXX <---------------------------------------------------------------
        }
        /* write the CUSTOMER_INDEX row ... */
        TblCUSTOMER_INDEX::Key * pkCI = new TblCUSTOMER_INDEX::Key(w_id, d_id, c_last, c_last_len);
        conn.requestWrite(pkCI, prCI);
        /* write the CUSTOMER row ... */
        conn.requestWrite(pkC, prC);
    }
     
    void DB::genRowCUSTOMER_PAYMENT(uint16_t w_id, uint16_t d_id, uint32_t c_id) {
        /* generate the new CUSTOMER_PAYMENT row (but do not write it yet) ... */
        TblCUSTOMER_PAYMENT::Key * pkC_PAYMENT = new TblCUSTOMER_PAYMENT::Key(w_id, d_id, c_id);
        TblCUSTOMER_PAYMENT::Row * prC_PAYMENT = new TblCUSTOMER_PAYMENT::Row;
        prC_PAYMENT->populate(dg, c_id);
        /* write the CUSTOMER_PAYMENT row ... */
        conn.requestWrite(pkC_PAYMENT, prC_PAYMENT);
    }

    void DB::genRowDISTRICT(uint16_t w_id, uint16_t d_id) {
        TblDISTRICT::Key * pkD = new TblDISTRICT::Key(w_id, d_id);
        TblDISTRICT::Row * prD = new TblDISTRICT::Row;
        prD->populate(dg);
        int ans = TblDISTRICT::requestWrite( conn, pkD, prD );
        assert( ans >= 0 );
    }
 
    void DB::genRowDISTRICT_YTD(uint16_t w_id, uint16_t d_id, uint16_t t_id) {
        TblDISTRICT_YTD::Key * pkD_YTD = new TblDISTRICT_YTD::Key(w_id, d_id, t_id);
        TblDISTRICT_YTD::Row * prD_YTD = new TblDISTRICT_YTD::Row;
        prD_YTD->populate(dg);
        int ans = TblDISTRICT_YTD::requestWrite( conn, pkD_YTD, prD_YTD );
        assert( ans >= 0 );
    }
    
    void DB::genRowDISTRICT_NEXT_O_ID(uint16_t w_id, uint16_t d_id, uint16_t t_id, uint32_t d_next_o_id) {
        TblDISTRICT_NEXT_O_ID::Key * pkD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Key(w_id, d_id, t_id);
        TblDISTRICT_NEXT_O_ID::Row * prD_NEXT_O_ID = new TblDISTRICT_NEXT_O_ID::Row;
        prD_NEXT_O_ID->populate(dg, d_next_o_id);
        int ans = TblDISTRICT_NEXT_O_ID::requestWrite( conn, pkD_NEXT_O_ID, prD_NEXT_O_ID );
        assert( ans >= 0 );
    }
    
    void DB::genRowSTOCK(uint16_t w_id, uint32_t i_id) {
        TblSTOCK::Key * pkS = new TblSTOCK::Key(w_id, i_id);
        TblSTOCK::Row * prS = new TblSTOCK::Row;
        prS->populate(dg);
        int ans = TblSTOCK::requestWrite( conn, pkS, prS );
        assert( ans >= 0 );
    }
    
    void DB::genRowWAREHOUSE(uint16_t w_id) {
        TblWAREHOUSE::Key * pkW = new TblWAREHOUSE::Key(w_id);
        TblWAREHOUSE::Row * prW = new TblWAREHOUSE::Row;
        prW->populate(dg);
        int ans = TblWAREHOUSE::requestWrite(conn, pkW, prW);
        assert( ans >= 0 );
    }
   
    void DB::genRowWAREHOUSE_YTD(uint16_t w_id, uint16_t t_id) {
        TblWAREHOUSE_YTD::Key * pkW_YTD = new TblWAREHOUSE_YTD::Key(w_id, t_id);
        TblWAREHOUSE_YTD::Row * prW_YTD = new TblWAREHOUSE_YTD::Row;
        prW_YTD->populate(dg);
        int ans = TblWAREHOUSE_YTD::requestWrite(conn, pkW_YTD, prW_YTD);
        assert( ans >= 0 );
    }
   

    void DB::genRowITEM(uint32_t i_id) {
        TblITEM::Key * pkI = new TblITEM::Key(i_id);
        TblITEM::Row * prI = new TblITEM::Row;
        prI->populate(dg);
        int ans = TblITEM::requestWrite(conn, pkI, prI);
        assert( ans >= 0 );
    }
    

    int DB::generate() {
        
        /* create the ITEM table ... */
        for( uint32_t i_id = 0; i_id < numITEMs; i_id++ ) {
            conn.beginTxn();
            genRowITEM( i_id );
            conn.commit();
            if( TRACE ) {
                conn.beginTxn();
                TblITEM::Row * prI = TblITEM::readRow( conn, i_id );
                assert( prI != 0 );
                fprintf(stdout, "ITEM(%d): ", i_id);  prI->print();  fprintf(stdout, "\n");  fflush(stdout);
                delete prI;
                conn.abort();
            }
        }
        
        /* for each of W warehouses ... */
        for( uint16_t w_id = 0; w_id < numWAREHOUSEs; w_id++ ) {
            
            /* add a row to the WAREHOUSE table ... */
            conn.beginTxn();
            genRowWAREHOUSE( w_id );
            conn.commit();
            
            /* for each of T terminals ... */
            for (uint16_t t_id = 0; t_id < numTERMINALs; t_id++) {
                /* add a row to the WAREHOUSE_YTD table ... */
                conn.beginTxn();
                genRowWAREHOUSE_YTD( w_id, t_id );
                conn.commit();
            }

            if( TRACE ) {
                conn.beginTxn();
                TblWAREHOUSE::Row * prW = TblWAREHOUSE::readRow( conn, w_id );
                assert( prW != 0 );
                fprintf(stdout, "WARENOUSE(%d): ", w_id);  prW->print();  fprintf(stdout, "\n");  fflush(stdout);
                delete prW;
                conn.abort();
            }
            
            /* add rows to the STOCK table ... */
            for( uint32_t i_id = 0; i_id < numSTOCKs; i_id++ ) {
                conn.beginTxn();
                genRowSTOCK( w_id, i_id );
                conn.commit();
                if( TRACE ) {
                    conn.beginTxn();
                    TblSTOCK::Row * prS = TblSTOCK::readRow( conn, w_id, i_id );
                    assert( prS != 0 );
                    fprintf(stdout, "STOCK(%d,%d): ", w_id, i_id);  prS->print();  fprintf(stdout, "\n");  fflush(stdout);
                    delete prS;
                    conn.abort();
                }
            }
            
            /* for each DISTRICT ... */
            for( uint16_t d_id = 0; d_id < numDISTRICTs_per_WAREHOUSE; d_id++ ) {
                
                /* add a row to the DISTRICT table ... */
                conn.beginTxn();
                genRowDISTRICT( w_id, d_id);
                conn.commit();

                /* for each T of the terminals ... */
                for (uint16_t t_id = 0; t_id < numTERMINALs; ++t_id) {
                    /* add a row to the DISTRICT_YTD table ... */
                    conn.beginTxn();
                    genRowDISTRICT_YTD(w_id, d_id, t_id);
                    conn.commit();
                    /* add a row to the DISTRICT_NEXT_O_ID table ... */
                    conn.beginTxn();
                    genRowDISTRICT_NEXT_O_ID(w_id, d_id, t_id, numORDERs_per_DISTRICT);
                    conn.commit();
                }

                if( TRACE ) {
                    conn.beginTxn();
                    TblDISTRICT::Row * prD = TblDISTRICT::readRow( conn, w_id, d_id );
                    assert( prD != 0 );
                    fprintf(stdout, "DISTRICT(%d,%d): ", w_id, d_id);  prD->print();  fprintf(stdout, "\n");  fflush(stdout);
                    delete prD;
                    conn.abort();
                }
                
                /* for each CUSTOMER of this district ... */
                for( uint32_t c_id = 0; c_id < numCUSTOMERs_per_DISTRICT; c_id++ ) {
                    
                    /* add a row to the CUSTOMER table ... */
                    conn.beginTxn();
                    genRowCUSTOMER( w_id, d_id, c_id );
                    conn.commit();

                    /* add a row to the CUSTOMER_PAYMENT table ... */
                    conn.beginTxn();
                    genRowCUSTOMER_PAYMENT( w_id, d_id, c_id );
                    conn.commit();

                    if( TRACE ) {
                        conn.beginTxn();
                        TblCUSTOMER::Row * prC = TblCUSTOMER::readRow( conn, w_id, d_id, c_id );
                        assert( prC != 0 );
                        fprintf(stdout, "CUSTOMER(%d,%d,%d): ", w_id, d_id, c_id);  prC->print();  fprintf(stdout, "\n");  fflush(stdout);
                        delete prC;
                        conn.abort();
                    }
                    
                    /* add a row to the HISTORY table ... */
                    static uint32_t gbl_h_id = 0;
                    conn.beginTxn();
                    genRowHISTORY( 0, gbl_h_id, w_id, d_id, c_id );
                    conn.commit();
                    if( TRACE ) {
                        conn.beginTxn();
                        TblHISTORY::Row * prH = TblHISTORY::readRow( conn, 0, gbl_h_id );
                        assert( prH != 0 );
                        fprintf(stdout, "HISTORY(%d,%d): ", 0, gbl_h_id);  prH->print();  fprintf(stdout, "\n");  fflush(stdout);
                        delete prH;
                        conn.abort();
                    }
                    gbl_h_id += 1;
                }
                

                /* for each ORDER ... */
                uint32_t numNEW_ORDERs = (3*numORDERs_per_DISTRICT + 9)/ 10;
                uint32_t * ordering_c_ids = dg.randomPermutation( numORDERs_per_DISTRICT );
                for( uint32_t o_id = 0; o_id < numORDERs_per_DISTRICT; o_id++ ) {
                    time_t entry_d = time(0);
                    bool isDelivered = ( (o_id + numNEW_ORDERs) < numORDERs_per_DISTRICT );
                    uint16_t nol = (uint16_t)( dg.uniformInt(5, 16) );
                    
                    /* add a row to the ORDER table ... */
                    for (uint16_t t_id = 0; t_id < numTERMINALs; ++t_id) {
                        conn.beginTxn();
                        genRowORDER( w_id, d_id, o_id, ordering_c_ids[o_id], entry_d, 
                            nol, isDelivered, t_id );
                        if( TRACE ) {
                            conn.beginTxn();
                            TblORDER::Row * prO = TblORDER::readRow( conn, w_id, d_id, o_id, t_id );
                            assert( prO != 0 );
                            fprintf(stdout, "ORDER(%d,%d,%d): ", w_id, d_id, o_id);  prO->print();  fprintf(stdout, "\n");  fflush(stdout);
                            delete prO;
                            conn.abort();
                        }
                    }

                    /* add rows to the ORDER_LINE table ... */
                    for (uint16_t t_id = 0; t_id < numTERMINALs; ++t_id) {
                        for( uint16_t ol_number = 0; ol_number < nol; ol_number++ ) {
                            conn.beginTxn();
                            genRowORDER_LINE( w_id, d_id, o_id, ol_number, 
                                    (isDelivered ? entry_d : ((time_t)(0))), t_id );
                            conn.commit();
                            if( TRACE ) {
                                conn.beginTxn();
                                TblORDER_LINE::Row * prOL = TblORDER_LINE::readRow( conn, w_id, d_id, o_id, ol_number, t_id );
                                assert( prOL != 0 );
                                fprintf(stdout, "ORDER_LINE(%d,%d,%d,%d): ", w_id, d_id, o_id, ol_number);  prOL->print();  fprintf(stdout, "\n");  fflush(stdout);
                                delete prOL;
                                conn.abort();
                            }
                        }
                        /* possibly add a row to tne NEW_ORDER table ... */
                        if( !isDelivered ) {
                            conn.beginTxn();
                            genRowNEW_ORDER( w_id, d_id, o_id, t_id );
                            conn.commit();
                            if( TRACE ) {
                                conn.beginTxn();
                                TblNEW_ORDER::Row * prNL = TblNEW_ORDER::readRow( conn, w_id, d_id, o_id, t_id );
                                assert( prNL != 0 );
                                fprintf(stdout, "NEW_ORDER(%d,%d,%d): ", w_id, d_id, o_id);  prNL->print();  fprintf(stdout, "\n");  fflush(stdout);
                                delete prNL;
                                conn.abort();
                            }
                        }
                    }
                }
                delete [] ordering_c_ids;
            }
            
        }
        
        return 0;
    }
    
    int DB::generate(StorageHashTable & hashTable, int id) {
        
        /* create the ITEM table ... */
        fprintf(stdout, "generate ITEM ...\n"); fflush(stdout);
        for( uint32_t i_id = 0; i_id < numITEMs; i_id++ ) {
            genRowITEM( hashTable, id, i_id );
        }
        
        /* for each of W warehouses ... */
        for( uint16_t w_id = 0; w_id < numWAREHOUSEs; w_id++ ) {
            
            fprintf(stdout, "generate WAREHOUSE %u ...\n", w_id); fflush(stdout);
            /* add a row to the WAREHOUSE table ... */
            genRowWAREHOUSE( hashTable, id, w_id );
            
            /* for each of T terminals ... */
            for (uint16_t t_id = 0; t_id < numTERMINALs; t_id++) {
                /* add a row to the WAREHOUSE_YTD table ... */
                genRowWAREHOUSE_YTD( hashTable, id, w_id, t_id);
            }
            
            /* add rows to the STOCK table ... */
            fprintf(stdout, "generate STOCK ...\n"); fflush(stdout);
            for( uint32_t i_id = 0; i_id < numSTOCKs; i_id++ ) {
                genRowSTOCK( hashTable, id, w_id, i_id );
            }
            
            /* for each DISTRICT ... */
            for( uint16_t d_id = 0; d_id < numDISTRICTs_per_WAREHOUSE; d_id++ ) {
                /* add a row to the DISTRICT table ... */
                genRowDISTRICT( hashTable, id, w_id, d_id );
                
                /* for each of T terminals ... */
                for (uint16_t t_id = 0; t_id < numTERMINALs; ++t_id) {
                    /* add a row to the DISTRICT_YTD table ... */
                    genRowDISTRICT_YTD( hashTable, id, w_id, d_id, t_id );
                    /* add a row to the DISTRICT_NEXT_O_ID table ... */
                    genRowDISTRICT_NEXT_O_ID( hashTable, id, w_id, d_id, t_id, numORDERs_per_DISTRICT );
                }

                /* for each CUSTOMER of this district ... */
                for( uint32_t c_id = 0; c_id < numCUSTOMERs_per_DISTRICT; c_id++ ) {
                     
                    /* add a row to the CUSTOMER table ... */
                    genRowCUSTOMER( hashTable, id, w_id, d_id, c_id );
 
                    /* add a row to the CUSTOMER_PAYMENT table ... */
                    genRowCUSTOMER_PAYMENT( hashTable, id, w_id, d_id, c_id );
                    
                    /* add a row to the HISTORY table ... */
                    static uint32_t gbl_h_id = 0;
                    genRowHISTORY( hashTable, id, 0, gbl_h_id, w_id, d_id, c_id );
                    gbl_h_id += 1;
                }


                /* for each ORDER ... */
                uint32_t numNEW_ORDERs = (3*numORDERs_per_DISTRICT + 9)/ 10;
                uint32_t * ordering_c_ids = dg.randomPermutation( numORDERs_per_DISTRICT );
                for (uint16_t t_id = 0; t_id < numTERMINALs; ++t_id) {
                    for( uint32_t o_id = 0; o_id < numORDERs_per_DISTRICT; o_id++ ) {
                        time_t entry_d = time(0);
                        bool isDelivered = ( (o_id + numNEW_ORDERs) < numORDERs_per_DISTRICT );
                        uint16_t nol = (uint16_t)( dg.uniformInt(5, 16) );

                        /* add a row to the ORDER table ... */
                        genRowORDER( hashTable, id, w_id, d_id, o_id, ordering_c_ids[o_id], entry_d, nol, isDelivered, t_id );

                        /* add rows to the ORDER_LINE table ... */
                        for( uint16_t ol_number = 0; ol_number < nol; ol_number++ ) {
                            genRowORDER_LINE( hashTable, id, w_id, d_id, o_id, ol_number, (isDelivered ? entry_d : ((time_t)(0))), t_id );
                        }

                        /* possibly add a row to tne NEW_ORDER table ... */
                        if( !isDelivered ) {
                            genRowNEW_ORDER( hashTable, id, w_id, d_id, o_id, t_id );
                        }
                    }
                }
                delete [] ordering_c_ids;
            }

        }

        return 0;
    }

    DB::DB(unsigned awhich, int32_t randomSeed) : which(awhich), conn(awhich), dg(randomSeed) {
        TRACE = false;
        terminal_id = 0;  terminal_w_id = 0;
        numTERMINALs = 10;
        numWAREHOUSEs = 1;
        numITEMs = 100000;
        numSTOCKs = numITEMs;
        numDISTRICTs_per_WAREHOUSE = 10;
        numCUSTOMERs_per_DISTRICT = 3000;
        numORDERs_per_DISTRICT = 3000;
    }
    
    DB::DB(unsigned awhich, int32_t randomSeed, int nW) : which(awhich), conn(awhich), dg(randomSeed) {
        TRACE = false;
        terminal_id = 0;  terminal_w_id = 0;
        numTERMINALs = 10;
        numWAREHOUSEs = nW;
        numITEMs = 100000;
        numSTOCKs = numITEMs;
        numDISTRICTs_per_WAREHOUSE = 10;
        numCUSTOMERs_per_DISTRICT = 3000;
        numORDERs_per_DISTRICT = 3000;
    }

    DB::DB( unsigned awhich, int32_t randomSeed, 
        int nW,
        int id, Queue<Packet> * inQueue, Queue<Packet> * outQueue):
        which(awhich), conn(awhich, id, inQueue, outQueue), dg(randomSeed) 
    {
        TRACE = false;
        terminal_id = 0;  terminal_w_id = 0;
        numTERMINALs = 10;
        numWAREHOUSEs = nW;
        numITEMs = 100000;
        numSTOCKs = numITEMs;
        numDISTRICTs_per_WAREHOUSE = 10;
        numCUSTOMERs_per_DISTRICT = 3000;
        numORDERs_per_DISTRICT = 3000;
    }

    DB::DB( unsigned awhich, int32_t randomSeed,
       int nW, int nI, int nDperW, int nCperD, int nOperD
           ) : which(awhich), conn(awhich), dg(randomSeed) {
        TRACE = false;
        terminal_id = 0;  terminal_w_id = 0;
        numTERMINALs = 10;
        numWAREHOUSEs = nW;
        numITEMs = nI;
        numSTOCKs = numITEMs;
        numDISTRICTs_per_WAREHOUSE = nDperW;
        numCUSTOMERs_per_DISTRICT = nCperD;
        numORDERs_per_DISTRICT = nOperD;
    }
    
    DB::DB( DB & db, int32_t randomSeed) : which(db.which), conn(db.which), dg(randomSeed) {
        TRACE = db.TRACE;
        terminal_id = 0;  terminal_w_id = 0;
        numTERMINALs = db.numTERMINALs;
        numWAREHOUSEs = db.numWAREHOUSEs;
        numITEMs = db.numITEMs;
        numSTOCKs = db.numSTOCKs;
        numDISTRICTs_per_WAREHOUSE = db.numDISTRICTs_per_WAREHOUSE;
        numCUSTOMERs_per_DISTRICT = db.numCUSTOMERs_per_DISTRICT;
        numORDERs_per_DISTRICT = db.numORDERs_per_DISTRICT;
    }

    DB::~DB() {}

};
