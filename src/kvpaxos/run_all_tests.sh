# python3 dstest --race --iter 100 --workers 10 --timeout 60 --output out.log TestBasic TestDone TestPartition TestUnreliable TestHole TestManyPartition
# python3 dstest --race --iter 100 --workers 10 --timeout 60 --output out.log TestDone TestManyPartition
# python3 dstest --race --iter 100 --workers 10 --timeout 60 --output out.log TestDone
python3 dstest --race --iter 100 --workers 10 --timeout 60 --output out.log TestManyPartition