## SeqnumProcessor ##
add_library (SeqnumProcessor seqnum-processor.cpp)
target_link_libraries (SeqnumProcessor Util)

## ValidatorWorker ##
add_library (ValidatorWorker validator-worker.cpp)
target_link_libraries (ValidatorWorker SeqnumProcessor)

## Validator ##
add_library (Validator validator.cpp)
target_link_libraries (Validator Server ValidatorWorker SeqnumProcessor)

## ValidatorMain ##
add_executable (ValidatorMain validator-main.cpp)
target_link_libraries (ValidatorMain Util Validator ${CMAKE_THREAD_LIBS_INIT})
