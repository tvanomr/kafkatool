package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/tvanomr/kafkatool/kafkaadmin"
	"strings"
)

type topicListCmdType struct {
	client                  sarama.Client
	shouldPrintDefault      bool
	shouldShowCompactedOnly bool
	shouldShowInfiniteOnly  bool
	verbose                 bool
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

func isCompacted(configs kafkaadmin.RawConfig) bool {
	return strings.Contains(configs["cleanup.policy"].Value, "compact")
}

func isInfinite(configs kafkaadmin.RawConfig) bool {
	return configs["cleanup.policy"].Value == "delete" &&
		configs["retention.ms"].Value == "-1" &&
		configs["retention.bytes"].Value == "-1"
}

func (t *topicListCmdType) runListTopics(cmd *cobra.Command, args []string) error {
	var err error
	t.client, err = kafkaadmin.NewDefaultClient([]string{hostPort.String()})
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
	configs, err := kafkaadmin.GetTopicConfigs(t.client, topics...)
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
						fmt.Printf("(writable) ")
						break
					}
				}
				min, max, err := kafkaadmin.GetTopicRange(t.client, topic, partition)
				if err == nil {
					fmt.Printf("range %d-%d", min, max)
				}
				fmt.Println("")
			}
			if !t.verbose {
				continue
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
	flags.BoolVarP(&runner.verbose, "verbose", "v", false, "print more info (configs)")
}
