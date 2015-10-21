## XctionWorker ##
add_library (XctionWorker xction-worker.cpp)
target_link_libraries (XctionWorker Util)

## ProcessorWorker ##
add_library (ProcessorWorker processor-worker.cpp)
target_link_libraries (ProcessorWorker Util)

## Processor ##
add_library (Processor processor.cpp)
target_link_libraries (Processor Client ProcessorWorker XctionWorker TpccWorker
    Util)

## ProcessorMain ##
add_executable (ProcessorMain processor-main.cpp)
target_link_libraries (ProcessorMain Processor ${CMAKE_THREAD_LIBS_INIT}) 
