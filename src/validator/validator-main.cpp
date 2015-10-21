/* validator/validator-main.cpp */
#include <validator/validator.h>

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include <util/code-handler.h>
#include <util/const.h>
#include <util/logger.h>

int main(int argc, char ** argv)
{
    // usage: id config port
    Config config(argv[2]);
    config.load();

    Validator * validator = new Validator(argv[3], atoi(argv[1]), &config);

    Logger::out(0, "ValidatorMain[%s]: Run!\n", argv[3]);
    validator->run();
    int sec = 1;
    for (int i = 0; i < Const::SERVER_RUNTIME; i += sec) {
        sleep(sec);
        validator->publishCounter();
    } 
    Logger::out(1, "ValidatorMain[%s]: Shut down!\n", argv[3]);
    validator->publish();
    validator->cancel();
    delete validator;

    Logger::out(0, "ValidatorMain[%s]: Exit!\n", argv[3]);
    return 0;
}

