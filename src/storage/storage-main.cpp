/* storage/storage-main.cpp */
#include <storage/storage.h>

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include <util/code-handler.h>
#include <util/const.h>
#include <util/logger.h>
#include "util/config.h"

int main(int argc, char ** argv)
{
    /*
     * id
     * config
     * port
     */
    Config config(argv[2]);
    config.load();

    Storage * storage = new Storage(argv[3], atoi(argv[1]), NULL, &config);

    Logger::out(0, "StorageMain[%s]: Run!\n", argv[3]);
    storage->run();
    int sec = 1;
    for (int i = 0; i < Const::SERVER_RUNTIME; i += sec) {
        sleep(sec);
        storage->publishCounter();
    } 
    Logger::out(1, "StorageMain[%s]: Shut down!\n", argv[3]);
    storage->publish();
    storage->cancel();
    delete storage;

    Logger::out(0, "StorageMain[%s]: Exit!\n", argv[3]);
    return 0;
}

