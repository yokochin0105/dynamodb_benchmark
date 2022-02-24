# dynamodb go-benchmark

## Prerequistes

- aws cli + credentials
- go
- docker (only for running dynamodb-local)

## DynamoDB local

```bash
# start
docker run -d --name dynamodb -p 8000:8000 amazon/dynamodb-local
# stop
docker stop dynamodb
docker rm dynamodb
```
Now dynamodb local is accessible with `http://localhost:8000`

## Benchmark

Clone this repository

```bash
git clone https://github.com/yokawasa/dynamodb_benchmark.git
cd dynamodb_benchmark
```

Prepare test table and record using a helper tool

```bash
cd helper
go run main.go -h
 
# Read AWS Credentials and check if it's an intended one
echo $AWS_PROFILE
 
# Create a test table 
go run main.go -a create-table -table yoichi-test001
# Create a test item
go run main.go -a create-item -table yoichi-test001 -id foo
# Get the test item
go run main.go -a get-item -table yoichi-test001 -id foo 
# expected output
# Found item: id=foo, age=1
```

Now you're ready to work on dynamodb benchmarking

```bash
cd benchmark
go run main.go -h
 
# Read AWS Credentials and check if it's an intended one
echo $AWS_PROFILE
 
# Execute None-conditional update（increment) - concurrency 1 num 1 (total: 1x1)
go run main.go -a write -table yoichi-test001 -id foo
 
# Execute None-conditional update（increment) - concurrency 10 num 10 (total: 10x10)
go run main.go -a write -table yoichi-test001 -id foo -c 10 -n 10
go run main.go -a write -table yoichi-test001 -id foo -c 10 -n 10 -verbose
 
# Execute Conditional update（increment) with checking age is less than 510 in updating - concurrency 1 num 1 (total: 1)
go run main.go -a write -table yoichi-test001 -id foo -c 1 -n 1 -condition  510 -verbose   
# If age hits to 510, you'll see the following exeception
# Error: after 1 attempts, last error: ConditionalCheckFailedException: The conditional request failed
 
# Execute read - concurrency 10 num 10000
go run main.go -a read -table yoichi-test001 -id foo -c 10 -n 10000 -verbose
```
