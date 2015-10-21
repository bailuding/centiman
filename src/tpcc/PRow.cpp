//
//  PRow.cpp
//
//  Generic Rows for TPC-C tables
//
//  Created by Alan Demers on 10/14/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <cstdio>

#include <util/type.h>
#include "tpcc/PRow.h"

namespace TPCC {
    
    void PRow::reset( uint16_t nCols, uint16_t rowLen ) {
        /*
         * Reset the row to all NULL
         *   with specified nCols and specified space for row data
         */
        size_t hdrSize = RowHdr__SIZEOF(nCols);
        uint16_t sz = ((uint16_t)(hdrSize + rowLen));
        if( mSize < sz ) {
            if( mVal ) delete [] mVal;
            mVal = new uint8_t[sz];
            mSize = sz;
        }
        mCnt = hdrSize;
        RowHdr * pRH = getRowHdr();
        memset(pRH, 0, hdrSize);
        pRH->numCols = nCols;
    }
    
    void PRow::putCol( uint16_t col, const uint8_t * b, uint16_t bLen ) {
        /*
         * Store a value in given col of this row.
         * This may increase numInnerCols but not numCols.
         */
        ColHdr * pCH = getColHdr(col);
        RowHdr * pRH = getRowHdr();
        if( b != 0 ) /* setting col to non-NULL value */ {
            if( pCH->len == bLen ) /* previous value was same length as new one */ {
                if( bLen > 0 ) memcpy( (mVal + pCH->pos), b, bLen );
                if( pRH->numInnerCols < col ) pRH->numInnerCols = col;
                return;
            }
            if( (col >= pRH->numInnerCols) /* appending to the Row */
               && ((mCnt + bLen) <= mSize) /* new value will fit in current row buffer */ ) {
                pRH->numInnerCols = col;    // col is becoming the rightmost non-NULL value
                pCH->pos = mCnt;            // we are apppending
                pCH->len = bLen;            // this is the new length
                if( bLen > 0 ) {
                    memcpy( (mVal + mCnt), b, bLen );
                    mCnt += bLen;
                }
                return;
            }
            // fall thru to general case
        } else /* (b == 0) ==> setting column to NULL */ {
            assert( bLen == 0 ); // this is a caller error
            if( pCH->len == 0 ) /* previous value was 0-length */ {
                if( pCH->pos != 0 ) /* previous value was not NULL */ {
                    pCH->pos = 0;
                    /* fix up numInnerCols ... */
                    while( (getColHdr(pRH->numInnerCols)->pos == 0) /* it is a NULL */
                          && ( pRH->numInnerCols > 0 ) ) {
                        pRH->numInnerCols--;
                    }
                }
                return;
            }
            // fall thru to the general case
        }
        /*
         * We are changing the length of a column and need to shift the remainder of the row ...
         */
    TheGeneralCase: ;
        {
        	uint16_t nCols = pRH->numCols;
            
            /* allocate new space to copy into ... */
            assert( mSize >= RowHdr__SIZEOF(nCols) );
            uint16_t newSize = mSize;
            int delta = ((int)(bLen)) - ((int)(pCH->len)); // may be negative!
            if( (mCnt + delta) > mSize ) {
                newSize = mSize + delta;  newSize += (newSize/2); // TODO: improve this heuristic?
            }
            PRow tmpRow( nCols, newSize - RowHdr__SIZEOF(nCols) );
            assert( tmpRow.mCnt == RowHdr__SIZEOF(nCols) );
            
            /* copy the columns ... */
            ColHdr * pCHFrom = getColHdr(0);
            ColHdr * pCHTo = tmpRow.getColHdr(0);
            for( uint16_t c = 0; c < nCols; (c++, pCHFrom++, pCHTo++) ) {
                if( c == col ) {
                    tmpRow.putCol( c, b, bLen );
                } else {
                    uint16_t cLen = pCHFrom->len;
                    if( pCHFrom->pos == 0 ) /* NULL */ {
                        pCHTo->pos = 0;
                    } else /* not NULL */ {
                        pCHTo->pos = tmpRow.mCnt;
                        if( cLen > 0 ) {
                            memcpy( tmpRow.mVal + pCHTo->pos, mVal + pCHFrom->pos, cLen );
                            tmpRow.mCnt += cLen;
                        }
                        tmpRow.getRowHdr()->numInnerCols = col;
                    }
                    pCHTo->len = cLen;
                }
            }
            
            /* exchange self and tmpRow ... */
            { uint8_t * tmp = mVal;  mVal = tmpRow.mVal;  tmpRow.mVal = tmp; }
            { uint16_t tmp = mCnt;  mCnt = tmpRow.mCnt;  tmpRow.mCnt = tmp; }
            { uint16_t tmp = mSize;  mSize = tmpRow.mSize;  tmpRow.mSize = tmp; }
            /* now tmpRow destructor will be called and the space will be freed */
        }
    } // putCol

    void PRow::printHdr() {
        FILE * f = stdout;
        fprintf(f, "Hdr (");
        if( mVal == 0 ) {
            fprintf(stdout, "empty");
        } else {
            assert( mCnt >= RowHdr__SIZEOF(0) );
            RowHdr * pRH = getRowHdr();
            fprintf(stdout, "%d,%d", pRH->numCols, pRH->numInnerCols);
            for( int i = 0; i < pRH->numCols; i++ ) {
                ColHdr * pCH = &(pRH->colHdr[i]);
                fprintf(stdout, "(%d,%d)", pCH->pos, pCH->len);
            }
        }
        fprintf(stdout, ") ");
    }
    
    void PRow::printCol_uint16(uint16_t c, const char * cName) {
        fprintf(stdout, "%s ", cName);
        if( isNULL(c) ) {
            fprintf(stdout, "(NULL) ");
        } else {
//            fprintf(stdout, "%d ", getCol_uint16(c));
        }
    }

    void PRow::printCol_uint32(uint16_t c, const char * cName) {
        fprintf(stdout, "%s ", cName);
        if( isNULL(c) ) {
            fprintf(stdout, "(NULL) ");
        } else {
            fprintf(stdout, "%d ", getCol_uint32(c));
        }
    }

    void PRow::printCol_uint64(uint16_t c, const char * cName) {
        fprintf(stdout, "%s ", cName);
        if( isNULL(c) ) {
            fprintf(stdout, "(NULL) ");
        } else {
            fprintf(stdout, "%lld ", getCol_uint64(c));
        }
    }

    void PRow::printCol_time(uint16_t c, const char * cName) {
        fprintf(stdout, "%s ", cName);
        if( isNULL(c) ) {
            fprintf(stdout, "(NULL) ");
        } else {
            fprintf(stdout, "%ld ", getCol_time(c));
        }
    }
    
    void PRow::printCol_double(uint16_t c, const char * cName) {
        fprintf(stdout, "%s ", cName);
        if( isNULL(c) ) {
            fprintf(stdout, "(NULL) ");
        } else {
            fprintf(stdout, "%e ", getCol_double(c));
        }
    }
    
    void PRow::printCol_string(uint16_t c, const char * cName) {
        fprintf(stdout, "%s ", cName);
        if( isNULL(c) ) {
            fprintf(stdout, "(NULL) ");
        } else {
            uint8_t buf[100];
            uint8_t * p = buf;
            uint16_t len = getCol(c, 0, 0);
            if( len >= (sizeof buf) ) p = new uint8_t [1+len];
            (void)getCol(c, p, len);  p[len] = 0;
            fprintf(stdout, "\"%s\" ", (char *)(p));
            if( len >= (sizeof buf) ) delete [] p;
        }
    }

};

