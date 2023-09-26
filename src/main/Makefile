# Makefile

# The go command to use for our operations
GO=go

# The main Go file to run
MAIN=mrcoordinator.go

# All the txt files starting with 'pg'
FILES=pg*.txt

.PHONY: run

coordinator:
	go run mrcoordinator.go pg-*.txt

worker:
	go build -buildmode=plugin -o wc.so ../mrapps/wc.go && go run mrworker.go wc.so

