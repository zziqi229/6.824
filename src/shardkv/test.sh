#!/bin/bash
for i in {0..500}
do
    go test
done

# for i in {0..500}
# do
#     go test -run TestUnreliable1 > errTestUnreliable1.md
#     if [ $? -ne 0 ];then
#         echo "fail"
#         break
#     else
#         echo "success"
#     fi
# done
