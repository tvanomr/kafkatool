package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var topicListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"l"},
	Short:   "list topics",
	Args:    cobra.ExactArgs(0),
	RunE:    runListTopics}

func runListTopics(cmd *cobra.Command, args []string) error {
	conf := sarama.NewConfig()
	client, err := sarama.NewClient([]string{hostPort.String()}, conf)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.RefreshMetadata()
	if err != nil {
		return err
	}
	topics, err := client.Topics()
	if err != nil {
		return err
	}
	fmt.Println(len(topics), "topics:")
	for _, topic := range topics {
		fmt.Println(topic)
		partitions, err := client.Partitions(topic)
		if err == nil {
			writables, err := client.WritablePartitions(topic)
			if err != nil {
				writables = nil
			}
			fmt.Println("\tPartitions:")
			for _, partition := range partitions {
				fmt.Printf("\t\t%d ", partition)
				for _, writable := range writables {
					if partition == writable {
						fmt.Printf("(writable)")
						break
					}
				}
				fmt.Println("")
			}
		}
	}
	return nil
}
