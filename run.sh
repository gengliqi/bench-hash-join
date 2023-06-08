#!/bin/sh

oneRun()
{
    numactl --cpubind=0 --membind=0 ./build/bench-hash-join $1 $2 100000000 $3 0
}

onePerfRun()
{
    perf stat -e L1-dcache-load-misses,L1-dcache-loads,L1-dcache-stores,L1-icache-load-misses,LLC-load-misses,LLC-loads,LLC-store-misses,LLC-stores,branch-load-misses,branch-loads,dTLB-load-misses,dTLB-loads,dTLB-store-misses,dTLB-stores numactl --cpubind=0 --membind=0 ./bench-hash-join $1 $2 100000000 $3 0
}

multipleRun()
{
    local i
    local j
    for ((i=0; i<=100; i+=25))
    do
        for ((j=0; j<=3; j+=1))
        do
            #echo $1,$j,$2,$i
            $1 "$j" "$2" "$i"
        done
    done
}

start=10000
end=100000000
run()
{
    local i
    for ((i=start; i<=end; i*=10))
    do
        multipleRun "$1" "$i" > result_"${i}"/"${2}".log 2>&1
    done
}

rm -rf result*

for ((i=start; i<=end; i*=10))
do
    mkdir result_"${i}"
done

run oneRun 0
run oneRun 1
run oneRun 2
run oneRun 3
run onePerfRun perf0
run onePerfRun perf1
run onePerfRun perf2
run onePerfRun perf3