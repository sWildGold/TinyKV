ITERS=1
failed=0
num=0

rm outputfailed*
rm outputtest*

mkfifo numfifo
exec 6<>numfifo
rm -f numfifo
count=0
while (($count<1)); do # 10 processes
  echo >&6
  ((count++))
done;

mkfifo failedfifo
exec 7<>failedfifo
rm -f failedfifo
{
    while ((num<ITERS));
    do
        read -u7 i
        ((num++))
        if ((${i:0:1}));then
            echo "Pass $num-th iteration"
            rm outputtest${i:1}
        else
            ((failed++))
            echo "Failed $num-th iteration"
            mv outputtest${i:1} outputfailed$failed
        fi
        awk 'BEGIN{printf "iters:%d ratio:%.2f%%\n",'$num','100*\($num-$failed\)/$num'}'
        echo >&6 #start a new process
    done;
}&

iter=1;
while ((iter<=ITERS));
do
    read -u6
    {
        if time go test -run 2B >outputtest$iter;
        then
            echo 1$iter >&7
        else
            echo 0$iter >&7
        fi
    }&
    ((iter++));
done;
wait;