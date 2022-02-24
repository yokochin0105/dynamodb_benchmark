package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

func usage() {
	fmt.Println(usageText)
	os.Exit(0)
}

var usageText = `auto_increment [options...]

Options:
-a <action>          (Required) An action to execute
                     Defaults to "create-table"; must be one of: create-table, create-item, delete-item, get-item
-table <table>       (Required) DynamoDB table name
-id <id>             (Required for create-item, delete-item) id field value in the table
-endpoint-url <url>  DynamoDB Endpoint URL to send the API request to.
                     Defaults to "", which mean the AWS SDK automatically determines the URL
                     For example, give "http://localhost:8000" if it's local dynamodb with exposed port 8000
-verbose             Verbose option
-h                   help message
`

type Item struct {
	Id  string `json:"id"`
	Age int64  `json:"age"`
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

func CreateTable(db dynamodbiface.DynamoDBAPI, tableName *string) error {

	attributeDefinitions := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("id"),
			AttributeType: aws.String("S"),
		},
	}

	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String("id"),
			KeyType:       aws.String("HASH"),
		},
	}

	provisionedThroughput := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(10),
		WriteCapacityUnits: aws.Int64(10),
	}

	_, err := db.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions:  attributeDefinitions,
		KeySchema:             keySchema,
		ProvisionedThroughput: provisionedThroughput,
		TableName:             tableName,
	})
	return err
}

func CreateItem(db dynamodbiface.DynamoDBAPI, tableName *string, id *string) error {

	item := Item{
		Id:  *id,
		Age: 1,
	}
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("Got error marshalling map:")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// Create item in table
	param := &dynamodb.PutItemInput{
		TableName: tableName,
		Item:      av,
	}

	_, err = db.PutItem(param)
	return err
}

func DeleteItem(db dynamodbiface.DynamoDBAPI, tableName *string, id *string) error {
	param := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(*id),
			},
		},
		TableName: tableName,
	}
	_, err := db.DeleteItem(param)
	return err
}

func GetItem(db dynamodbiface.DynamoDBAPI, tableName *string, id *string) error {
	result, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: tableName,
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(*id),
			},
		},
	})
	if result.Item == nil {
		msg := "Could not find '" + *id + "'"
		return errors.New(msg)
	}
	item := Item{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}
	fmt.Printf("Found item: id=%s, age=%d\n", item.Id, item.Age)
	return err
}

func main() {

	var (
		action      string
		tableName   string
		id          string
		endpointUrl string
		verbose     bool
	)

	flag.StringVar(&action, "a", "read", "(Required) read or write")
	flag.StringVar(&tableName, "table", "", "(Required) DynamoDB table name")
	flag.StringVar(&endpointUrl, "endpoint-url", "", "The URL to send the API request to")
	flag.StringVar(&id, "id", "", "(Required) id field value in the table")
	flag.BoolVar(&verbose, "verbose", false, "Verbose option")
	flag.Usage = usage
	flag.Parse()

	if action != "create-table" &&
		action != "create-item" &&
		action != "delete-item" &&
		action != "get-item" {
		fmt.Println("[ERROR] Invalid Command Options (-a)! action value must: create-table, create-item or delete-item")
	}
	if tableName == "" ||
		(action == "create-item" && id == "") ||
		(action == "delete-item" && id == "") ||
		(action == "get-item" && id == "") {
		fmt.Println("[ERROR] Invalid Command Options! Minimum required options are \"-table\" and \"-id\"")
		usage()
	}

	db := getDynamoDBClient(endpointUrl)

	var err error
	switch action {
	case "create-table":
		err = CreateTable(db, &tableName)
	case "create-item":
		err = CreateItem(db, &tableName, &id)
	case "delete-item":
		err = DeleteItem(db, &tableName, &id)
	case "get-item":
		err = GetItem(db, &tableName, &id)
	}
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}
