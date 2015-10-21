#ifndef __UTIL_QUEUE_PRODUCER_H_
#define __UTIL_QUEUE_PRODUCER_H_

#include <pthread.h>

#include <util/const.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <util/stat.h>

template <class Type>
class QueueProducer
{
    public:
        QueueProducer(Queue<Type> * pQueue);
        Stat * getStat();
        void run();
        static void * runHelper(void *);
    private:
        Queue<Type> * mpQueue;
        Stat * mpStat;
};

template <class Type>
QueueProducer<Type>::QueueProducer(Queue<Type> * pQueue)
{
    mpQueue = pQueue;
    mpStat = new Stat();
}

template <class Type>
Stat * QueueProducer<Type>::getStat()
{
    return mpStat;
}

template <class Type>
void QueueProducer<Type>::run()
{
    Type * in = NULL;
    QueueBatcher<Type> batcher(mpQueue, Const::UTIL_QUEUE_BATCH_SIZE);
    mpStat->setStart();
    while (1) {
        in = batcher.getWriteSlot();
        mpStat->commit();
        batcher.putWriteSlot(in);
    }
}

template <class Type>
void * QueueProducer<Type>::runHelper(void * producer)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    ((QueueProducer<Type> *)producer)->run();
    pthread_exit(NULL);
}

#endif // __UTIL_QUEUE_PRODUCER_H__

