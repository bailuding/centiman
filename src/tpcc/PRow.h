//
//  Row.h
//  centiman TPCC
//
//  Generic Rows for TPC-C tables
//
//  Created by Alan Demers on 10/14/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__PRow__
#define __TPCC__PRow__

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include <util/type.h>

namespace TPCC {


class PRow : public Value {

public:
    
    struct ColHdr {
        uint16_t pos;   // position of col value (0 for NULL value)
        uint16_t len;   // length of col value (0 for NULL value)
    };
    
    struct RowHdr {
        uint16_t numCols;       // number of cols
        uint16_t numInnerCols;  // number of cols with a non-NULL somewhere strictly to the right
                                //   For an all NULL row, numInnerCols == 0
                                //   Otherwise numInnerCols is index of rightmost non-NULL
        ColHdr colHdr[1 /*maxCols*/];
#       define RowHdr__SIZEOF(n) (sizeof(RowHdr) - sizeof(ColHdr) + (n)*sizeof(ColHdr))
    };
    
    inline RowHdr * getRowHdr() { return ((RowHdr *)(mVal)); }
    inline ColHdr * getColHdr(uint16_t c) { return &( ((RowHdr *)(mVal))->colHdr[c] ); }
    
    void reset( uint16_t nCols, uint16_t rowLen );


    void putCol( uint16_t col, const uint8_t * b, uint16_t bLen );
    
    void putCol_uint16( uint16_t col, uint16_t val ) {
        putCol( col, ((uint8_t *)(&val)), (sizeof val) );
    }
    
    void putCol_uint32( uint16_t col, uint32_t val ) {
        putCol( col, ((uint8_t *)(&val)), (sizeof val) );
    }
    
    void putCol_uint64( uint16_t col, uint64_t val ) {
        putCol( col, ((uint8_t *)(&val)), (sizeof val) );
    }
    
    void putCol_time( uint16_t col, time_t val ) {
        putCol( col, ((uint8_t *)(&val)), (sizeof val) );
    }
    
    void putCol_double( uint16_t col, double val ) {
        putCol( col, ((uint8_t *)(&val)), (sizeof val) );
    }
    
    void putCol_cString( uint16_t col, const char * s ) {
        uint16_t slen = ( (s) ? (uint16_t)(strlen(s)) : 0 );
        putCol( col, (const uint8_t *)(s), slen );
    }

    
    uint16_t getCol( uint16_t col, uint8_t * b, uint16_t bLen) {
        /*
         * Fetch value from specified col of this row
         * Put it in buffer b (max length bLen)
         * Return actual length of column (which may exceed bLen)
         * Return 0 for NULL column
         */
        RowHdr * pRH = getRowHdr();
        assert( (pRH != 0) & (col < pRH->numCols) );
        ColHdr * pCH = getColHdr(col);
        if( pCH->pos == 0 ) return 0;
        size_t len = pCH->len;  if( len > bLen ) len = bLen;
        if( len > 0 ) memcpy(b, mVal+pCH->pos, len);
        return (pCH->len);
    }
    

    uint16_t getCol_uint16(Key * pk, uint16_t c) {
        uint16_t val;
        uint16_t len = getCol(c, (uint8_t *)(&val), (sizeof val));
        if (len != (sizeof val)) {
            if (pk != NULL)
                pk->print();
            else
                fprintf(stdout, "NULL\n");
        }
        assert( len == (sizeof val) );
        return val;
    }
    
    uint32_t getCol_uint32(uint16_t c) {
        uint32_t val;
        uint16_t len = getCol(c, (uint8_t *)(&val), (sizeof val));
        assert( len == (sizeof val) );
        return val;
    }
    
    uint64_t getCol_uint64(uint16_t c) {
        uint64_t val;
        uint16_t len = getCol(c, (uint8_t *)(&val), (sizeof val));
        assert( len == (sizeof val) );
        return val;
    }
    
    time_t getCol_time(uint16_t c) {
        time_t val;
        uint16_t len = getCol(c, (uint8_t *)(&val), (sizeof val));
        assert( len == (sizeof val) );
        return val;
    }

    double getCol_double(uint16_t c) {
        double val;
        uint16_t len = getCol(c, (uint8_t *)(&val), (sizeof val));
        assert( len == (sizeof val) );
        return val;
    }
    
    char * getCol_cString( uint16_t col ) {
        if( isNULL(col) ) return 0;
        uint16_t slen = getCol( col, 0, 0);
        char * ans = new char[1+slen];
        uint16_t slen2 = getCol(col, ((uint8_t *)(ans)), slen);
        assert( slen2 == slen );
        ans[slen] = 0;
        return ans;
    }
    
    bool isNULL(uint16_t c) {
        return (getColHdr(c)->pos == 0);
    }

	bool isNULL() {
		return (mVal == 0);
	}

    void printHdr();
    void printCol_uint16(uint16_t c, const char * cName);
    void printCol_uint32(uint16_t c, const char * cName);
    void printCol_uint64(uint16_t c, const char * cName);
    void printCol_time(uint16_t c, const char * cName);
    void printCol_double(uint16_t c, const char * cName);
    void printCol_string(uint16_t c, const char * cName);
    
    PRow() : Value() {}
    PRow( uint16_t nCols, uint16_t rowLen ) : Value() { reset(nCols, rowLen); }
    ~PRow() {}

}; // PRow

}; // TPCC

#endif /* __TPCC__PRow__ */
