# storage/CMakeLists.txt #

## StorageWorker ##
add_library (StorageWorker storage-worker.cpp)
target_link_libraries (StorageWorker DB Util)

## Storage ##
add_library (Storage storage.cpp)
target_link_libraries (Storage Server StorageWorker)

## StorageMain ##
add_executable (StorageMain storage-main.cpp)
target_link_libraries (StorageMain Storage ${CMAKE_THREAD_LIBS_INIT})
