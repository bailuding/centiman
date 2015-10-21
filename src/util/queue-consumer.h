#ifndef __UTIL_QUEUE_CONSUMER_H__
#define __UTIL_QUEUE_CONSUMER_H__

#include <pthread.h>

#include <util/const.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <util/stat.h>

template <class Type>
class QueueConsumer
{
    public:
        QueueConsumer(Queue<Type> * pQueue);
        Stat * getStat();
        void run();
        static void * runHelper(void *);
    private:
        Queue<Type> * mpQueue;
        Stat * mpStat;
};

template <class Type>
QueueConsumer<Type>::QueueConsumer(Queue<Type> * pQueue)
{
    mpQueue = pQueue;
    mpStat = new Stat();
}

template <class Type>
Stat * QueueConsumer<Type>::getStat()
{
    return mpStat;
}

template <class Type>
void QueueConsumer<Type>::run()
{
    Type * out = NULL;
    QueueBatcher<Type> batcher(mpQueue, Const::UTIL_QUEUE_BATCH_SIZE);
    mpStat->setStart();
    size_t tmp = 0;
    while (1) {
        out = batcher.getReadSlot();
        out->clean();
        mpStat->commit();
        batcher.putReadSlot(out);
        if (++tmp == 1000000) {
            Logger::out(0, ".");
            tmp = 0;
        }
    }
}

template <class Type>
void * QueueConsumer<Type>::runHelper(void * consumer)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    ((QueueConsumer<Type> *)consumer)->run();
    pthread_exit(NULL);
}

#endif // __UTIL_QUEUE_CONSUMER_H__

