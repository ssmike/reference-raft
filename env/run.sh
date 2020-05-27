for pid in `cat pids`; do
    kill $pid
done

rm pids
#rm -rf *.dir
python gen_conf.py

for i in `seq 0 $(($QUORUM - 1))`; do
    mkdir -p storage/$i.dir
    ./main $i.json &
    echo $! >> pids
    echo running $i PID $!
done
