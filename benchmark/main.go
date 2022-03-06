package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func usage() {
	fmt.Println(usageText)
	os.Exit(0)
}

var usageText = `auto_increment [options...]

Options:
-a <action>          (Required) An action to execute
                     Defaults to "read"; Must be either "read" or "write"
-table <table>       (Required) DynamoDB table name
-id <id>             (Required) id field value in the table
-condition <max-age> Conditinal check value of max age on updating "age" field in the table
                     Defaults to 0 (No Conditional Check); Must be more than 0
-c connections       Number of parallel simultaneous DynamoDB session
                     Defaults to 1; Must be more than 0
-n num-calls         Run for exactly this number of calls by each DynamoDB session
                     Defaults to 1; Must be more than 0
-r retry-num         Number fo Retry in each message send
                     Default to 1; Must be more than 0
-endpoint-url <url>  DynamoDB Endpoint URL to send the API request to.
                     Defaults to "", which mean the AWS SDK automatically determines the URL
                     For example, give "http://localhost:8000" if it's local dynamodb with exposed port 8000
-verbose             Verbose option
-h                   help message
`

type DynamoDBBenchmark struct {
	Action      string
	TableName   string
	Id          string
	Condition   int
	EndpointUrl string
	Connections int
	NumCalls    int
	RetryNum    int
	Verbose     bool
}

type Item struct {
	Id  string `json:"id"`
	Age int64  `json:"age"`
	Ver int64  `json:"ver"`
}

func retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)
		fmt.Printf("retrying after error:%s\n", err)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

func getDynamoDBClient(endpointUrl string) *dynamodb.DynamoDB {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	if endpointUrl != "" {
		return dynamodb.New(sess, &aws.Config{Endpoint: aws.String(endpointUrl)})
	} else {
		return dynamodb.New(sess)
	}
}

func (c *DynamoDBBenchmark) Run() {
	successCount := uint32(0)
	errorCount := uint32(0)
	successGetCount := uint32(0)
	errorGetCount := uint32(0)
	startTime := time.Now()

	var wg sync.WaitGroup
	for i := 1; i <= c.Connections; i++ {
		wg.Add(1)
		if c.Action == "read" {
			go c.startReadWorker(i, &wg, &successCount, &errorCount, &successGetCount, &errorGetCount)
		} else {
			go c.startWriteWorker(i, &wg, &successCount, &errorCount, &successGetCount, &errorGetCount)
		}
	}
	wg.Wait()

	duration := time.Since(startTime).Seconds()
	duration_ms := time.Since(startTime).Milliseconds()
	average_ms := duration_ms / (int64(successCount) + int64(errorCount) + int64(successGetCount) + int64(errorGetCount))

	fmt.Println("-----------------------")
	fmt.Printf("DynamoDB Benchmark Summary - %s\n", c.Action)
	fmt.Println("-----------------------")
	fmt.Printf("Sent messages: %v\n", successCount)
	fmt.Printf("Errors: %v\n", errorCount)
	fmt.Printf("(GET)Sent messages: %v\n", successGetCount)
	fmt.Printf("(GET)Errors: %v\n", errorGetCount)
	fmt.Printf("Duration (sec): %v\n", duration)
	fmt.Printf("Average (ms): %v\n", average_ms)
}

func (c *DynamoDBBenchmark) startWriteWorker(id int, wg *sync.WaitGroup, successCount *uint32, errorCount *uint32, successGetCount *uint32, errorGetCount *uint32) {
	defer wg.Done()

	db := getDynamoDBClient(c.EndpointUrl)

	param := &dynamodb.UpdateItemInput{
		TableName: &c.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(c.Id),
			},
		},
		UpdateExpression: aws.String("set age = age - :age_decrement_value, ver = ver + :ver_increment_value"),
		ReturnValues:     aws.String("ALL_NEW"),
	}
	param2 := &dynamodb.GetItemInput{
		TableName: &c.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(c.Id),
			},
		},
	}
	for i := 1; i <= c.NumCalls; i++ {
		err2 := retry(c.RetryNum, 2*time.Second, func() (err2 error) {
			dresp, derr := db.GetItem(param2)
			item := Item{}
			derr = dynamodbattribute.UnmarshalMap(dresp.Item, &item)
			// fmt.Printf("[Verbose] DynamoDB GetImte Response: id %s age %d ver %d\n", item.Id, item.Age, item.Ver)
			param.ConditionExpression = aws.String("ver = :ver_value AND age >= :age_minimum_value")
			param.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
				":age_decrement_value": {
					N: aws.String("1"),
				},
				":ver_increment_value": {
					N: aws.String("1"),
				},
				":ver_value": {
					N: aws.String(strconv.FormatInt(item.Ver, 10)),
				},
				":age_minimum_value": {
					N: aws.String(strconv.Itoa(c.Condition)),
				},
			}
			if c.Verbose {
				if derr != nil {
					fmt.Printf("Got error unmarshalling: %s", derr)
					return derr
				}
				// fmt.Printf("[Verbose] DynamoDB GetItem Response: id %s age %d ver %d\n", item.Id, item.Age, item.Ver)
			}
			return derr
		})

		if err2 != nil {
			// fmt.Printf("Error: %v\n", err2)
			atomic.AddUint32(errorGetCount, 1)
			continue
		}

		atomic.AddUint32(successGetCount, 1)


		err := retry(c.RetryNum, 2*time.Second, func() (err error) {
			dresp, derr := db.UpdateItem(param)
			if c.Verbose {
				item := Item{}
				derr := dynamodbattribute.UnmarshalMap(dresp.Attributes, &item)
				if derr != nil {
					fmt.Printf("Got error unmarshalling: %s", derr)
					return derr
				}
				nowTime := time.Now()
				const MilliFormat = "2006/01/02 15:04:05.000"
				fmt.Printf( "[timestamp] %s [Verbose] DynamoDB UpdateItem Response: id %s age %d ver %d\n", nowTime.Format(MilliFormat), item.Id, item.Age, item.Ver)

			}
			return derr
		})

		if err != nil {
			// fmt.Printf("Error: %v\n", err)
			atomic.AddUint32(errorCount, 1)
			continue
		}

		atomic.AddUint32(successCount, 1)
	}
}

func (c *DynamoDBBenchmark) startReadWorker(id int, wg *sync.WaitGroup, successCount *uint32, errorCount *uint32, successGetCount *uint32, errorGetCount *uint32) {
	defer wg.Done()

	db := getDynamoDBClient(c.EndpointUrl)

	param := &dynamodb.GetItemInput{
		TableName: &c.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(c.Id),
			},
		},
	}
	for i := 1; i <= c.NumCalls; i++ {
		err := retry(c.RetryNum, 2*time.Second, func() (err error) {
			dresp, derr := db.GetItem(param)
			if c.Verbose {
				item := Item{}
				derr := dynamodbattribute.UnmarshalMap(dresp.Item, &item)
				if derr != nil {
					fmt.Printf("Got error unmarshalling: %s", derr)
					return derr
				}
				fmt.Printf("[Verbose] DynamoDB GetImte Response: id %s age %d\n", item.Id, item.Age)
			}
			return derr
		})

		if err != nil {
			fmt.Printf("Error: %v\n", err)
			atomic.AddUint32(errorCount, 1)
			continue
		}

		atomic.AddUint32(successCount, 1)
	}
}

func main() {

	var (
		action      string
		tableName   string
		id          string
		condition   int
		endpointUrl string
		connections int
		numCalls    int
		retryNum    int
		verbose     bool
	)

	flag.StringVar(&action, "a", "read", "(Required) read or write")
	flag.StringVar(&tableName, "table", "", "(Required) DynamoDB table name")
	flag.StringVar(&endpointUrl, "endpoint-url", "", "The URL to send the API request to")
	flag.StringVar(&id, "id", "", "(Required) id field value in the table")
	flag.IntVar(&condition, "condition", 0, "Conditinal check value of max age on updating age field")
	flag.IntVar(&connections, "c", 1, "Number of parallel simultaneous Kinesis session")
	flag.IntVar(&numCalls, "n", 1, "Run for exactly this number of calls by each Kinesis session")
	flag.IntVar(&retryNum, "r", 1, "Number fo Retry in each message send")
	flag.BoolVar(&verbose, "verbose", false, "Verbose option")
	flag.Usage = usage
	flag.Parse()

	if action != "read" && action != "write" {
		fmt.Println("[ERROR] Invalid Command Options (-a)! action value must either read or write")
	}
	if tableName == "" || id == "" {
		fmt.Println("[ERROR] Invalid Command Options! Minimum required options are \"-table\" and \"-id\"")
		usage()
	}

	s := DynamoDBBenchmark{
		Action:      action,
		TableName:   tableName,
		Id:          id,
		Condition:   condition,
		EndpointUrl: endpointUrl,
		Connections: connections,
		NumCalls:    numCalls,
		RetryNum:    retryNum,
		Verbose:     verbose,
	}

	s.Run()
}
