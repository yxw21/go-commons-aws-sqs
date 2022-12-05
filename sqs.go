package go_commons_aws_sqs_1111

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client struct {
	sqs               *sqs.Client
	queueName         string
	getQueueUrlOutput *sqs.GetQueueUrlOutput
}

func (ctx *Client) SetQueue(queueName string) error {
	ctx.queueName = queueName
	qInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	getQueueUrlOutput, err := ctx.sqs.GetQueueUrl(context.TODO(), qInput)
	if err != nil {
		return err
	}
	ctx.getQueueUrlOutput = getQueueUrlOutput
	return nil
}

func (ctx *Client) SendMessageWithFIFO(messageGroupId, message, messageDeduplicationId string) (*sqs.SendMessageOutput, error) {
	sendMessageInput := &sqs.SendMessageInput{
		MessageGroupId:         aws.String(messageGroupId),
		MessageBody:            aws.String(message),
		MessageDeduplicationId: aws.String(messageDeduplicationId),
		QueueUrl:               ctx.getQueueUrlOutput.QueueUrl,
	}
	return ctx.sqs.SendMessage(context.TODO(), sendMessageInput)
}

func (ctx *Client) SendMessage(message, messageDeduplicationId string) (*sqs.SendMessageOutput, error) {
	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:            aws.String(message),
		MessageDeduplicationId: aws.String(messageDeduplicationId),
		QueueUrl:               ctx.getQueueUrlOutput.QueueUrl,
	}
	return ctx.sqs.SendMessage(context.TODO(), sendMessageInput)
}

func (ctx *Client) DeleteMessage(receiptHandle string) (*sqs.DeleteMessageOutput, error) {
	deleteMessageInput := &sqs.DeleteMessageInput{
		ReceiptHandle: &receiptHandle,
		QueueUrl:      ctx.getQueueUrlOutput.QueueUrl,
	}
	return ctx.sqs.DeleteMessage(context.TODO(), deleteMessageInput)
}

func (ctx *Client) GetQueues(queueNamePrefix *string) (*sqs.ListQueuesOutput, error) {
	listQueuesInput := &sqs.ListQueuesInput{QueueNamePrefix: queueNamePrefix}
	return ctx.sqs.ListQueues(context.TODO(), listQueuesInput)
}

func (ctx *Client) GetMessages(maxNumberOfMessages int32, visibilityTimeout, waitTimeSeconds int32) (*sqs.ReceiveMessageOutput, error) {
	receiveMessageInput := &sqs.ReceiveMessageInput{
		AttributeNames: []types.QueueAttributeName{
			"All",
		},
		MaxNumberOfMessages: maxNumberOfMessages,
		MessageAttributeNames: []string{
			"All",
		},
		VisibilityTimeout: visibilityTimeout,
		WaitTimeSeconds:   waitTimeSeconds,
		QueueUrl:          ctx.getQueueUrlOutput.QueueUrl,
	}
	return ctx.sqs.ReceiveMessage(context.TODO(), receiveMessageInput)
}

func (ctx *Client) GetSQS() *sqs.Client {
	return ctx.sqs
}

func (ctx *Client) GetQueue() string {
	return ctx.queueName
}

func (ctx *Client) GetQueueUrlOutput() *sqs.GetQueueUrlOutput {
	return ctx.getQueueUrlOutput
}

func NewClient(region, key, secret, session string) (*Client, error) {
	if region == "" {
		return nil, errors.New("region cannot be empty")
	}
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}
	if secret == "" {
		return nil, errors.New("secret cannot be empty")
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, session)),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		sqs: sqs.NewFromConfig(cfg),
	}, nil
}
