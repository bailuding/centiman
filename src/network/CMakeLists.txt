# network/CMakeLists.txt #

## Socket ##
add_library (Socket socket.cpp)
target_link_libraries (Socket Util)

## Client ##
add_library (Client client.cpp)
target_link_libraries (Client Socket Sender Util)

## Server ##
add_library (Server server.cpp)
target_link_libraries (Server Socket Sender Util)

## Sender ##
add_library (Sender sender.cpp)
target_link_libraries (Sender Processor Util)
