#include "validator/validator.h"

Validator::Validator(const char * port, int id, Config * config):
    mId(id),

    mSeqnumExpect(0)
{
    mPort = new char[6];
    strcpy(mPort, port);

    mpInQueue = new Queue<Packet>(Const::VALIDATOR_IN_QUEUE_SIZE);
    mpOutQueues = new std::map<int, Queue<Packet> *>();
    mPacketQueue = new std::atomic<Packet *> [Const::VALIDATOR_IN_QUEUE_SIZE] ();

    mpServer = new Server(mPort, mpInQueue, mpOutQueues, Const::VALIDATOR_OUT_QUEUE_SIZE);
    mpSeqnumProcessor = new SeqnumProcessor(mId, mpInQueue, mPacketQueue, &mSeqnumExpect);
    mpWorker = new ValidatorWorker(mId, config, mpInQueue, mpOutQueues, mPacketQueue, &mSeqnumExpect);
}

Validator::~Validator()
{
    delete mpServer;
    delete mpSeqnumProcessor;
    delete mpWorker;
   
    delete[] mPort;
    
    delete mpInQueue;
    delete[] mPacketQueue;

    for (std::map<int, Queue<Packet> *>::iterator it = mpOutQueues->begin(); it != mpOutQueues->end(); ++it) {
        delete it->second;
    }
    delete mpOutQueues;

    Logger::out(0, "Validator: Destroyed\n");
}

int Validator::cancel()
{
    mpServer->cancel();
    sleep(Const::SHUTDOWN_TIME);
    if (CodeHandler::print("pthreadCancel", pthread_cancel(mServerThread)))
        return -1;
    sleep(Const::SHUTDOWN_TIME);
    if (CodeHandler::print("pthreadCancel", pthread_cancel(mSeqnumProcessorThread)))
        return -1;
    sleep(Const::SHUTDOWN_TIME);
        if (CodeHandler::print("pthreadCancel", pthread_cancel(mWorkerThread)))
            return -1;
    sleep(Const::SHUTDOWN_TIME);
    Logger::out(0, "Validator: Threads cancelled!\n");
    return 0;

}

void Validator::publish()
{
    mpServer->publishStat("Validator");
    mpWorker->publishStat();
}

void Validator::publishProfile()
{
    mpInQueue->publish("ValidatorIn");
    mpSeqnumProcessor->publishProfile();
    mpWorker->publishProfile();
    for (auto it = mpOutQueues->begin(); it != mpOutQueues->end(); ++it) {
        it->second->publish("ValidatorOut");
        break;
    }
}

void Validator::publishCounter()
{
    Sender * tmp = mpServer->mpSender;
    Logger::out(0, "Counter[V%s]: %s %s %s\n",
            mPort,
            Util::now().c_str(),
            mpWorker->counter.format().c_str(),
            tmp == NULL ? "ph" : tmp->counter.format().c_str());
    mpWorker->counter.reset();
    if (tmp != NULL)
        tmp->counter.reset();
}

void Validator::run()
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    int rc;
    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (CodeHandler::print("pthreadAttr", rc))
        exit(-1);

    rc = pthread_create(&mServerThread, &attr, Server::runHelper, (void *) mpServer);
    if (CodeHandler::print("pthreadCreate", rc))
        exit(-1);
    
    sleep(5);
    
    rc = pthread_create(&mSeqnumProcessorThread, &attr, SeqnumProcessor::runHelper, (void *) mpSeqnumProcessor);
    if (CodeHandler::print("pthreadCreate", rc))
        exit(-1);

    rc = pthread_create(&mWorkerThread, &attr, ValidatorWorker::runHelper, (void *) mpWorker);
    if (CodeHandler::print("pthreadCreate", rc))
        exit(-1);

    pthread_attr_destroy(&attr);
}

