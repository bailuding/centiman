#include "processor/processor.h"

Processor::Processor(int id, Config * config, char * trace):
        mId(id), mRand()
{
    this->config = config;

    mpInQueue = new Queue<Packet> (Const::PROCESSOR_IN_QUEUE_SIZE);
    mpXctionOutQueue = new Queue<Packet> (Const::PROCESSOR_XCTION_QUEUE_SIZE);

    int serverNum = Const::STORAGE_NUM + Const::VALIDATOR_NUM;
    mpOutQueues = new Queue<Packet> * [serverNum];
    for (int i = 0; i < serverNum; ++i) {
        if (Const::MODE_TPCC) {
            mpOutQueues[i] = new Queue<Packet> (Const::TPCC_WORKER_NUM);
        } else {
            mpOutQueues[i] = new Queue<Packet> (Const::PROCESSOR_XCTION_QUEUE_SIZE);
        }
    }

    mpSfds = new std::map<int, int> ();
  
    mpXctionWorker = new XctionWorker(mpXctionOutQueue, mpInQueue);
    mpXctionWorker->setTrace(trace);

    if (Const::MODE_TPCC) {
        mpTpccWorkers = new TpccWorker * [Const::TPCC_WORKER_NUM];
        mpXctionOutQueues = new Queue<Packet> * [Const::TPCC_WORKER_NUM];
        for (int i = 0; i < Const::TPCC_WORKER_NUM; ++i) {
            mpXctionOutQueues[i] = new Queue<Packet> (1);
            int gid = mId * Const::TPCC_WORKER_NUM + i;
            mpTpccWorkers[i] = new TpccWorker(mRand.rand(), i, gid % 10, gid / 10, 
                                        mpXctionOutQueues[i], mpInQueue);
        }
        mTpccWorkerThreads = new pthread_t[Const::TPCC_WORKER_NUM];
    }
    mpClient = new Client(mId, config, mpInQueue, mpOutQueues, serverNum, mpSfds);
    mpWorker = new ProcessorWorker(mId, config, mpInQueue, mpXctionOutQueue, mpXctionOutQueues, mpOutQueues, mpSfds);
} 

Processor::~Processor()
{
    delete config;
    delete mpXctionWorker;
    delete mpWorker;
    delete mpClient;
    if (Const::MODE_TPCC) {
        for (int i = 0; i < Const::TPCC_WORKER_NUM; ++i) {
            delete mpTpccWorkers[i];
            delete mpXctionOutQueues[i];
        }
        delete[]  mpTpccWorkers;
        delete[] mpXctionOutQueues;
        delete[] mTpccWorkerThreads;
    }

    delete mpInQueue;
    delete mpXctionOutQueue;
    for (int i = 0; i < Const::STORAGE_NUM + Const::VALIDATOR_NUM; ++i) {
        delete mpOutQueues[i];
    }
    delete[] mpOutQueues;

    delete mpSfds;
    Logger::out(1, "Processor: Destroyed\n");
}

int Processor::cancel()
{
    mpClient->cancel();
    sleep(Const::SHUTDOWN_TIME);

    if (CodeHandler::print("pthreadCancel", pthread_cancel(mXctionThread)))
        return -1;
    sleep(Const::SHUTDOWN_TIME);

    if (Const::MODE_TPCC) {
        for (int i = 0; i < Const::TPCC_WORKER_NUM; ++i) {
            if (CodeHandler::print("pthreadCancel", pthread_cancel(mTpccWorkerThreads[i])))
                return -1;  
        }
    } else {
        if (CodeHandler::print("pthreadCancel", pthread_cancel(mWorkerThread)))
            return -1;  
    }
    sleep(Const::SHUTDOWN_TIME);

    if (CodeHandler::print("pthreadCancel", pthread_cancel(mClientThread)))
        return -1;
    sleep(Const::SHUTDOWN_TIME);

    Logger::out(1, "Processor: Threads cancelled!\n");
    return 0;
}

void Processor::publishStat() 
{
    if (Const::MODE_TPCC) {
        Stat stat;
        Stat stats[5];
        for (int i = 0; i < Const::TPCC_WORKER_NUM; ++i) {
            stat.merge(mpTpccWorkers[i]->getStat());
            Stat * details = mpTpccWorkers[i]->getStatDetail();
            for (int j = 0; j < 5; ++j) {
                stats[j].merge(details[j]);
            }
        }
        stat.publish();
        std::string msg = "";
        for (int i = 0; i < 5; ++i) {
            msg = "Detail(";  msg += i + '0';  msg += ")";
            stats[i].publish(msg.c_str());
        }
    } else {
        mpXctionWorker->publishStat();
    }
    mpClient->publishStat("Server");
    mpWorker->publishStat();
} 
 
void Processor::publishProfile()
{
    mpInQueue->publish("ProcessorIn");
    mpXctionOutQueue->publish("ProcessorXctionOut");
    mpOutQueues[0]->publish("ProcessorValidatorOut");
    mpOutQueues[Const::VALIDATOR_NUM]->publish("ProcessorStorageOut");
}

void Processor::publishCounter()
{
    Logger::out(0, "Counter[P%d]: %s %s %s %s\n",
            mId,
            Util::now().c_str(),
            mpWorker->counter.format().c_str(),
            mpXctionWorker->counter.format().c_str(),
            mpClient->mpSender->counter.format().c_str());
    mpWorker->counter.reset();
    mpXctionWorker->counter.reset();
    mpClient->mpSender->counter.reset();
}

void Processor::run()
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    int rc;
    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (CodeHandler::print("pthreadAttr", rc))
        exit(-1);

    rc = pthread_create(&mClientThread, &attr, Client::runHelper, (void *)mpClient);
    if (CodeHandler::print("pthreadCreate", rc)) {
        exit(-1);
    }

    sleep(5); 

    if (Const::MODE_TPCC) {
        for (int i = 0; i < Const::TPCC_WORKER_NUM; ++i) {
            rc = pthread_create(&mTpccWorkerThreads[i], &attr, TpccWorker::runHelper, (void *)mpTpccWorkers[i]);
            if (CodeHandler::print("pthreadCreate", rc))
                exit(-1);
        }
    } else {
        rc = pthread_create(&mXctionThread, &attr, XctionWorker::runHelper, (void *)mpXctionWorker);
        if (CodeHandler::print("pthreadCreate", rc))
            exit(-1);
    }

    rc = pthread_create(&mWorkerThread, &attr, ProcessorWorker::runHelper, (void *)mpWorker);
    if (CodeHandler::print("pthreadCreate", rc))
        exit(-1);
    pthread_attr_destroy(&attr);
}


