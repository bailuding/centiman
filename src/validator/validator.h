#ifndef __VALIDATOR_VALIDATOR_H__
#define __VALIDATOR_VALIDATOR_H__

#include <atomic>
#include <map>
#include <unistd.h>
#include <pthread.h>

#include <network/server.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/type.h>
#include <validator/seqnum-processor.h>
#include <validator/validator-worker.h>
#include <util/code-handler.h>
#include <util/const.h>
#include <util/logger.h>
#include <util/util.h>


class Validator
{
    public:
        Validator(const char * port, int id, Config * config);
        ~Validator();
        void run();
        int cancel();
        void publish();
        void publishProfile();
        void publishCounter();
        
    private:
        int mId;
        char * mPort;

        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;
        std::atomic<Packet *> * mPacketQueue;
        
        std::atomic<Seqnum> mSeqnumExpect;

        Server * mpServer;
        SeqnumProcessor * mpSeqnumProcessor;
        ValidatorWorker * mpWorker;

        pthread_t mServerThread;
        pthread_t mSeqnumProcessorThread;
        pthread_t mWorkerThread;
};

#endif // __VALIDATOR_VALIDATOR_H__
