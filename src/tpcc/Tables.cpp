//
//  Tables.cpp
//  centiman TPCC
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//


#include "tpcc/Tables.h"

namespace TPCC {
    
    const unsigned TblCUSTOMER::TBLID           = 1;
    const unsigned TblCUSTOMER_INDEX::TBLID     = 2;
    const unsigned TblDISTRICT::TBLID           = 3;
    const unsigned TblHISTORY::TBLID            = 4;
    const unsigned TblITEM::TBLID               = 5;
    const unsigned TblNEW_ORDER::TBLID          = 6;
    const unsigned TblNEW_ORDER_INDEX::TBLID    = 7;
    const unsigned TblORDER::TBLID              = 8;
    const unsigned TblORDER_INDEX::TBLID        = 9;
    const unsigned TblORDER_LINE::TBLID         = 10;
    const unsigned TblSTOCK::TBLID              = 11;
    const unsigned TblWAREHOUSE::TBLID          = 12;
    const unsigned TblDISTRICT_YTD::TBLID       = 13;
    const unsigned TblWAREHOUSE_YTD::TBLID      = 14;
    const unsigned TblDISTRICT_NEXT_O_ID::TBLID = 15;
    const unsigned TblCUSTOMER_PAYMENT::TBLID   = 16;
}; // TPCC
