1 MapReduce

Jeffrey Dean readings  
https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf  

Lab   
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html  

Notes  
https://pdos.csail.mit.edu/6.824/notes/l01.txt  


실행 

1. 
$ go build -buildmode=plugin ../mrapps/wc.go

2. 
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt

3. 
// 다른 윈도우에서 
$ go run mrworker.go wc.so

테스트케이스
bash test-mr.sh