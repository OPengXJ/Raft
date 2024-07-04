# Raft测试方法：

## 环境

这个作业可以用Go module，不用切GOPATH

## 执行测试

```bash
cd src/raft
##依次执行测试
go test -run 2A

go test -run 2B

go test -run 2C

go test -run 2D

##接着会输出各种测试结果
```

主要完成代码在src/raft/raft.go中

实验要求：https://pdos.csail.mit.edu/6.824/labs/lab-raft.html