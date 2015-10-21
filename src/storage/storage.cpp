#include "storage/storage.h"

Storage::Storage(const char * port, int id, char * trace, Config * config):
    mId(id)

{
    mPort = new char[6];
    strcpy(mPort, port);

    mpInQueue = new Queue<Packet>(Const::STORAGE_IN_QUEUE_SIZE);
    mpOutQueues = new std::map<int, Queue<Packet> *> ();

    mpServer =  new Server(mPort, mpInQueue, mpOutQueues, Const::STORAGE_OUT_QUEUE_SIZE);
    mpWorker = new StorageWorker(mId, config, mpInQueue, mpOutQueues);
    mpWorker->setTrace(trace);
}

Storage::~Storage()
{
    delete mpServer;
    delete mpWorker; 
    delete[] mPort;
    delete mpInQueue;
    for (std::map<int, Queue<Packet> *>::iterator it = mpOutQueues->begin(); it != mpOutQueues->end(); ++it) {
        delete it->second;
    }
    delete mpOutQueues;
    Logger::out(1, "Storage: Destroyed\n");
}

void Storage::publish()
{
    mpServer->publishStat("Storage");
}

void Storage::publishProfile()
{
    mpInQueue->publish("StorageIn");
    mpWorker->publishProfile();
    for (auto it = mpOutQueues->begin(); it != mpOutQueues->end(); ++it) {
        it->second->publish("StorageOut");
        break;
    }
}

void Storage::publishCounter()
{
    Sender * tmp = mpServer->mpSender;
    Logger::out(0, "Counter[S%s]: %s %s %s\n",
            mPort,
            Util::now().c_str(),
            mpWorker->counter.format().c_str(),
            tmp == NULL ? "ph" : tmp->counter.format().c_str());
    mpWorker->counter.reset();
    if (tmp != NULL)
        tmp->counter.reset();
}

int Storage::cancel()
{
    mpServer->cancel();
    sleep(Const::SHUTDOWN_TIME);

    if (CodeHandler::print("pthreadCancel", pthread_cancel(mServerThread)))
        return -1;
    sleep(Const::SHUTDOWN_TIME);
    if (CodeHandler::print("pthreadCancel", pthread_cancel(mWorkerThread)))
        return -1;
    sleep(Const::SHUTDOWN_TIME);
    Logger::out(1, "Storage: Threads cancelled!\n");
    return 0;
}

void Storage::run()
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

    rc = pthread_create(&mWorkerThread, &attr, StorageWorker::runHelper, (void *) mpWorker);
    if (CodeHandler::print("pthreadCreate", rc))
        exit(-1);

    pthread_attr_destroy(&attr);

}


