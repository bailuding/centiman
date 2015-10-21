config=../config/config_local
valgrind="valgrind --leak-check=yes"

# start storage
../build/storage/StorageMain 0 $config 4000 &
../build/storage/StorageMain 1 $config 4001 &

# start validator
../build/validator/ValidatorMain 0 $config 5000 &
../build/validator/ValidatorMain 1 $config 5001 &
#../build/validator/ValidatorMain 2 $config 5002 &
#../build/validator/ValidatorMain 3 $config 5003 &

sleep 10

# start processor
../build/processor/ProcessorMain 0 $config &
../build/processor/ProcessorMain 1 $config &
