package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/tvanomr/kafkatool/kafkaadmin"
	"os"
	"os/signal"
)

type readCmdType struct {
	client                   sarama.Client
	partition                int32
	startOffset              int64
	shouldStartFromBeginning bool
	shouldStartFromEnd       bool
	shouldWait               bool
}

func (r *readCmdType) printMessage(message *sarama.ConsumerMessage) {
	fmt.Println("Time: ", message.Timestamp)
	fmt.Println("Key: ", string(message.Key))
	if message.Value != nil {
		fmt.Println("Value: ", string(message.Value))
	} else {
		fmt.Println("Tombstone")
	}
	fmt.Println("")
}

func (r *readCmdType) Run(cmd *cobra.Command, args []string) error {
	var offset = r.startOffset
	var topic = args[0]
	if r.shouldStartFromBeginning {
		offset = sarama.OffsetOldest
	}
	if r.shouldStartFromEnd {
		offset = sarama.OffsetNewest
		r.shouldWait = true
	}
	client, err := kafkaadmin.NewDefaultClient([]string{hostPort.String()})
	if err != nil {
		return err
	}
	defer client.Close()
	if !r.shouldWait {
		min, err := client.GetOffset(topic, r.partition, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		max, err := client.GetOffset(topic, r.partition, sarama.OffsetNewest)
		if offset >= sarama.OffsetNewest {
			fmt.Println("offset is in the future")
			return nil
		}
		if max == min {
			fmt.Println("nothing to read")
			return nil
		}
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, r.partition, offset)
	if err != nil {
		return err
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		select {
		case message, ok := <-partitionConsumer.Messages():
			if !ok {
				return nil
			}
			r.printMessage(message)
			if !r.shouldWait && (message.Offset+1) == partitionConsumer.HighWaterMarkOffset() {
				partitionConsumer.Close()
				return nil
			}
		case err, ok := <-partitionConsumer.Errors():
			if !ok {
				return nil
			}
			partitionConsumer.Close()
			return err
		case <-interrupt:
			fmt.Println("force quit")
			partitionConsumer.Close()
			return nil
		}
	}
}

var readCmd = &cobra.Command{
	Use:     "read",
	Aliases: []string{"r"},
	Short:   "read partition",
	Args:    cobra.ExactArgs(1)}

func init() {
	var runner readCmdType
	readCmd.RunE = runner.Run
	flags := readCmd.Flags()
	flags.Int32VarP(&runner.partition, "partition", "p", 0, "partition number")
	flags.Int64VarP(&runner.startOffset, "offset", "o", 0, "starting offset (use this or flags)")
	flags.BoolVar(&runner.shouldStartFromBeginning, "from-start", false, "start from the beginning")
	flags.BoolVar(&runner.shouldStartFromEnd, "from-end", false, "start from end")
	flags.BoolVarP(&runner.shouldWait, "wait", "w", false, "wait for data")
}
