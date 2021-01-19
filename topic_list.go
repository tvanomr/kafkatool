package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"strings"
)

type topicListCmdType struct {
	client                  sarama.Client
	shouldPrintDefault      bool
	shouldShowCompactedOnly bool
	shouldShowInfiniteOnly  bool
}

func (t *topicListCmdType) getTopicConfigs(topics ...string) (map[string][]*sarama.ConfigEntry, error) {
	request := sarama.DescribeConfigsRequest{Version: 2}
	for _, topic := range topics {
		request.Resources = append(request.Resources, &sarama.ConfigResource{Type: sarama.TopicResource, Name: topic})
	}
	broker, err := t.client.Controller()
	if err != nil {
		return nil, err
	}
	response, err := broker.DescribeConfigs(&request)
	result := make(map[string][]*sarama.ConfigEntry)
	for _, resource := range response.Resources {
		if resource.ErrorCode != int16(sarama.ErrNoError) {
			return nil, sarama.KError(resource.ErrorCode)
		}
		result[resource.Name] = resource.Configs
	}
	return result, nil
}

func inArray(target string, array []string) bool {
	for _, str := range array {
		if target == str {
			return true
		}
	}
	return false
}

func filterTopics(topics []string, predicate func(string) bool) []string {
	var result []string
	for _, topic := range topics {
		if predicate(topic) {
			result = append(result, topic)
		}
	}
	return result
}

func isCompacted(configs []*sarama.ConfigEntry) bool {
	for _, config := range configs {
		if config.Name == "cleanup.policy" && strings.Contains(config.Value, "compact") {
			return true
		}
	}
	return false
}

func isInfinite(configs []*sarama.ConfigEntry) bool {
	var isDelete, isInfiniteTime, isInfiniteSize bool
	for _, config := range configs {
		switch config.Name {
		case "cleanup.policy":
			if config.Value != "delete" {
				return false
			}
			isDelete = true
		case "retention.ms":
			if config.Value != "-1" {
				return false
			}
			isInfiniteTime = true
		case "retention.bytes":
			if config.Value != "-1" {
				return false
			}
			isInfiniteSize = true
		}
	}
	return isDelete && isInfiniteTime && isInfiniteSize
}

func (t *topicListCmdType) runListTopics(cmd *cobra.Command, args []string) error {
	var err error
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_4_0_0
	t.client, err = sarama.NewClient([]string{hostPort.String()}, conf)
	if err != nil {
		return err
	}
	defer t.client.Close()
	err = t.client.RefreshMetadata()
	if err != nil {
		return err
	}
	topics, err := t.client.Topics()
	if err != nil {
		return err
	}
	if len(args) > 0 {
		topics = filterTopics(topics, func(topic string) bool {
			return inArray(topic, args)
		})
	}
	configs, err := t.getTopicConfigs(topics...)
	if err != nil {
		return err
	}
	if t.shouldShowCompactedOnly {
		topics = filterTopics(topics, func(topic string) bool {
			return isCompacted(configs[topic])
		})
	}
	if t.shouldShowInfiniteOnly {
		topics = filterTopics(topics, func(topic string) bool {
			return isInfinite(configs[topic])
		})
	}
	fmt.Println(len(topics), "topics:")
	for _, topic := range topics {
		fmt.Println(topic)
		partitions, err := t.client.Partitions(topic)
		if err == nil {
			writables, err := t.client.WritablePartitions(topic)
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
			fmt.Println("\t Configs")
			for _, config := range configs[topic] {
				if !t.shouldPrintDefault && config.Default {
					continue
				}
				fmt.Printf("\t\t %s=%s", config.Name, config.Value)
				if config.Default && !config.ReadOnly {
					fmt.Printf(" (default) ")
				}
				if config.ReadOnly && !config.Default {
					fmt.Println(" (readonly) ")
				}
				if config.Default && config.ReadOnly {
					fmt.Println(" (readonly,default) ")
				}
				fmt.Println("")
			}
		}
	}
	return nil
}

var topicListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"l"},
	Short:   "list topics"}

func init() {
	var runner topicListCmdType
	topicListCmd.RunE = runner.runListTopics
	flags := topicListCmd.Flags()
	flags.BoolVarP(&runner.shouldPrintDefault, "default", "d", false, "print unchanged (default) config parameters")
	flags.BoolVarP(&runner.shouldShowCompactedOnly, "compact", "c", false, "show compacted topics only")
	flags.BoolVarP(&runner.shouldShowInfiniteOnly, "infinite", "i", false, "show infinite topics only")
}
