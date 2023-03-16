# python3 dstest --iter 100 --workers 10 --timeout 60 --output out.log TestBasic TestMove TestLimp TestConcurrent1 TestConcurrentUnreliable
# python3 dstest --iter 100 --workers 10 --timeout 60 --output out.log TestBasic TestMove TestLimp 
# python3 dstest --iter 500 --workers 10 --timeout 60 --output out.log TestConcurrent1 TestConcurrentUnreliable
python3 dstest --iter 500 --workers 10 --timeout 60 --output out.log TestConcurrentUnreliable