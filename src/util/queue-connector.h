#ifndef __UTIL_QUEUE_CONNECTOR_H_
#define __UTIL_QUEUE_CONNECTOR_H_

#include <pthread.h>

#include <util/const.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <util/stat.h>

template <class Type>
class QueueConnector
{
    public:
        QueueConnector(Queue<Type> * pInQueue, Queue<Type> * pOutQueue);
        Stat * getStat();
        void run();
        static void * runHelper(void *);
    private:
        Queue<Type> * mpInQueue;
        Queue<Type> * mpOutQueue;
        Stat * mpStat;
};

template <class Type>
QueueConnector<Type>::QueueConnector(Queue<Type> * pInQueue, Queue<Type> * pOutQueue)
{
    mpInQueue = pInQueue;
    mpOutQueue = pOutQueue;
    mpStat = new Stat();
}

template <class Type>
Stat * QueueConnector<Type>::getStat()
{
    return mpStat;
}

template <class Type>
void QueueConnector<Type>::run()
{
    Type * in = NULL;
    Type * out = NULL;
    QueueBatcher<Type> inBatcher(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE);
    QueueBatcher<Type> outBatcher(mpOutQueue, Const::UTIL_QUEUE_BATCH_SIZE);
    mpStat->setStart();
    while (1) {
        in = inBatcher.getReadSlot();
        out = outBatcher.getWriteSlot();
        inBatcher.putReadSlot(out);
        outBatcher.putWriteSlot(in);
        mpStat->commit();
    }
}

template <class Type>
void * QueueConnector<Type>::runHelper(void * connector)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    ((QueueConnector<Type> *)connector)->run();
    pthread_exit(NULL);
}

#endif // __UTIL_QUEUE_CONNECTOR_H__

