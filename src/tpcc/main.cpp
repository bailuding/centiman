//
//  main.cpp
//  centiman
//
//  Created by Alan Demers on 10/18/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <stdint.h>
#include <cstdio>
#include <vector>
#include <time.h>
#include <sys/time.h>

#include <unordered_set> // ?

#include <util/type.h>
#include <tpcc/DataGen.h>
#include <tpcc/DBConn.h>

#include <tpcc/Tables.h>
#include <tpcc/DB.h>


using namespace TPCC;


void runTransactions(DB & db, int n, FILE * file) {
    for(int i = 0; i < n; i++ ) {
        /* set terminal_id and terminal_w_id */
        db.setTerminal(db.dg.uniformInt(0, 10), db.dg.uniformInt(0, db.getNumWAREHOUSEs()));

        db.beginTxn();
        double r = db.dg.rand();
        /* print transaction type */
        if( r <= 0.04 ) {
            fprintf(file, "0 ");
            db.txnStockLevel();
        } else if( r <= 0.08 ) {
            fprintf(file, "1 ");
            db.txnDelivery();
        } else if( r <= 0.12 ) {
            fprintf(file, "2 ");
            db.txnOrderStatus();
        } else if( r <= 0.55 ) {
            fprintf(file, "3 ");
            db.txnPayment();
        } else {
            fprintf(file, "4 ");
            db.txnNewOrder();
        }
        int ans = db.commitTxn();
        if( ans < 0 ) {
            fprintf(stdout, "abort\n"); fflush(stdout);
            break;
        }
        if( (i > 0) && ((i % 1000) == 0) ) {
            fprintf(stdout, "%d\n", i);  fflush(stdout);
        }
    }
    fprintf(stdout, "%d\n", n);  fflush(stdout);
}

int main(int argc, const char * argv[])
{
    /*
     * nTxns
     * numWAREHOUSEs
     */
    int ans;
    struct timeval tv;
    double t0, t1;
    int nTxns = atoi(argv[1]);
    
    fprintf(stdout, "Hello, World!\n");

//    DBGen dbg( dg, conn, true, /*nW=*/1, /*nI=*/10, /*nDperW=*/10, /*nCperD=*/20, /*nOperD=*/20 );
    DB db(0, 17);
    int numWAREHOUSEs = atoi(argv[2]);
    char dump[64] = "/home/blding/occ/data/db-dump-";
    strcat(dump, argv[2]);
    char trace[64] = "/home/blding/occ/data/db-trace-";
    strcat(trace, argv[2]);
    FILE * file = fopen(dump, "w");

    db.getConn().setTRACE(true);
    /* generate db dump */
    db.getConn().setFile(file);
    db.setNumWAREHOUSEs(numWAREHOUSEs);
    
    fprintf(stdout, "Generating ... ");  fflush(stdout);
    (void)gettimeofday(&tv, 0);  t0 = (double)(tv.tv_sec) + ((double)(tv.tv_usec))/1.0e6;
    ans = db.generate();
    (void)gettimeofday(&tv, 0);  t1 = (double)(tv.tv_sec) + ((double)(tv.tv_usec))/1.0e6;
    fprintf(stdout, "ans = %d  time = %e\n", ans, t1 - t0);  fflush(stdout);
    if( ans < 0 ) return ans;
    
    fclose(file);

    fprintf(stdout, "Running %d txns ... \n", nTxns);  fflush(stdout);
#   ifdef NET_STATS
    {
        db.getConn().clearStats();
    }
#   endif
    db.getConn().mStat.reset();
    (void)gettimeofday(&tv, 0);  t0 = (double)(tv.tv_sec) + ((double)(tv.tv_usec))/1.0e6;

    /* generate db trace */
    file = fopen(trace, "w");
    db.getConn().setFile(file);
    runTransactions( db, nTxns, file );
    fclose(file);

    (void)gettimeofday(&tv, 0);  t1 = (double)(tv.tv_sec) + ((double)(tv.tv_usec))/1.0e6;
    fprintf(stdout, "time = %e\n", t1 - t0);  fflush(stdout);
#   ifdef NET_STATS
    {
        DBConn::Stats s;
        db.getConn().getStats(&s);
        fprintf(stdout, "recvd packets %d payload %lld\n", s.recvdPackets, s.recvdPayload);
        fprintf(stdout, "sent packets %d payload %lld\n", s.sentPackets, s.sentPayload);
        fflush(stdout);
    }
#   endif
    db.getConn().mStat.publish();
    return 0;
}


