/* processor/processor-main.cpp */
#include <processor/processor.h>

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include <util/code-handler.h>
#include <util/const.h>
#include <util/generator.h>
#include <util/logger.h>

int main(int argc, char ** argv)
{
    /*
     * id
     * config
     * trace file
     */
    Config config(argv[2]);
    config.load();

    Processor * processor = new Processor(atoi(argv[1]), &config, NULL);
   
    Logger::out(0, "ProcessorMain: Run!\n");
    processor->run();
    int sec = 1;
    for (int i = 0; i < Const::CLIENT_RUNTIME; i += sec) {
        sleep(sec);
        processor->publishCounter();
    }
   
    processor->publishStat();

    Logger::out(0, "ProcessorMain: Shut down!\n");
    processor->cancel();
    delete processor; 
    Logger::out(0, "ProcessorMain: Exit!\n");
    return 0;
}

