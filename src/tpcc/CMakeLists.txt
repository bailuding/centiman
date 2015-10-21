## DBUtil ##
add_library (DBUtil PKey.cpp PRow.cpp RNG2.cpp DataGen.cpp)
target_link_libraries (DBUtil Util)

## LocalKVStore ##
add_library (LocalKVStore LocalKVStore.cpp)
target_link_libraries (LocalKVStore DBUtil)

## Tbl ##
add_library (Tbl Tables.cpp TblCUSTOMER.cpp TblCUSTOMER_INDEX.cpp TblDISTRICT.cpp
TblHISTORY.cpp TblITEM.cpp TblNEW_ORDER.cpp TblNEW_ORDER_INDEX.cpp TblORDER.cpp
TblORDER_INDEX.cpp TblORDER_LINE.cpp TblSTOCK.cpp TblWAREHOUSE.cpp
TblDISTRICT_YTD.cpp TblWAREHOUSE_YTD.cpp TblDISTRICT_NEXT_O_ID.cpp
TblCUSTOMER_PAYMENT.cpp)
target_link_libraries (Tbl DBUtil)

## DBConn ##
add_library (DBConn DBConn.cpp)
target_link_libraries (DBConn LocalKVStore)

## DBtxn ##
add_library (DBtxn DBtxnDelivery.cpp DBtxnNewOrder.cpp DBtxnOrderStatus.cpp
DBtxnPayment.cpp DBtxnStockLevel.cpp)
target_link_libraries (DBtxn Tbl DBConn)

## DB ##
add_library (DB DB.cpp)
target_link_libraries (DB LocalKVStore Tbl DBtxn DBConn)

## main ##
add_executable (Main main.cpp)
target_link_libraries (Main DB)

## split ##
add_executable (Split split.cpp)
add_executable (SplitDb split-db.cpp)
target_link_libraries (SplitDb Util)

## TPCC worker
add_library (TpccWorker tpcc-worker.cpp)
target_link_libraries(TpccWorker DB)
