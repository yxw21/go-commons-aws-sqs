package go_commons_aws_sqs_1111

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSQS(t *testing.T) {
	queueName := "test.fifo"
	messageGroupId := "default"

	// init
	sqs, err := NewClient("us-east-2", "{key}", "{secret}", "{session}")
	if err != nil {
		t.Fatal(err)
	}

	// list queue
	listQueuesOutput, err := sqs.GetQueues(nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(listQueuesOutput.QueueUrls)

	// set queue
	if err = sqs.SetQueue(queueName); err != nil {
		t.Fatal(err)
	}

	// send message
	sendResp, err := sqs.SendMessageWithFIFO(messageGroupId, "t1", strconv.FormatInt(time.Now().Unix(), 10))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Sent message with ID: " + *sendResp.MessageId)

	// get message
	getResp, err := sqs.GetMessages(10, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range getResp.Messages {
		fmt.Println("Message IDs:    ", *msg.MessageId)
		fmt.Println("Message Attributes:    ", msg.MessageAttributes)
		fmt.Println("Message Body:    ", *msg.Body)
	}

	// delete all message
	for _, msg := range getResp.Messages {
		fmt.Println(*msg.ReceiptHandle)
		_, err = sqs.DeleteMessage(*msg.ReceiptHandle)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("Deleted message success")
	}
}
