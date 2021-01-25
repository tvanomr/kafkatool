package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/tvanomr/kafkatool/kafkaadmin"
	"strconv"
)

const (
	cleanupPolicyName  = "cleanup.policy"
	retentionMsName    = "retention.ms"
	retentionBytesName = "retention.bytes"
	segmentBytesName   = "segment.bytes"
	segmentMsName      = "segment.ms"
)

var topicConfigPresets = map[string]map[string]string{
	"infinite": {
		cleanupPolicyName:  "delete",
		retentionMsName:    "-1",
		retentionBytesName: "-1"},
	"compact": {
		cleanupPolicyName: "compact"}}

type topicModCmdType struct {
	client            sarama.Client
	partitions        int32
	replicationFactor int16
	shouldDelete      bool
	configName        string
	configKey         string
	configValue       string
	segmentSize       string
	segmentDuration   string
	retentionSize     string
	retentionDuration string
}

func (t *topicModCmdType) makeConfig() map[string]string {
	result := make(map[string]string)
	for key, value := range topicConfigPresets[t.configName] {
		result[key] = value
	}
	if len(t.configKey) > 0 {
		result[t.configKey] = t.configValue
	}
	if len(t.segmentSize) > 0 {
		value, err := parseBinarySize(t.segmentSize)
		if err == nil && value > 0 {
			result[segmentBytesName] = strconv.FormatInt(value, 10)
		}
	}
	if len(t.segmentDuration) > 0 {
		value, err := parseDuration(t.segmentDuration)
		if err == nil {
			result[segmentMsName] = strconv.FormatInt(value.Milliseconds(), 10)
		} else {
			fmt.Println("segment duration conversion error:", err)
		}
	}
	if len(t.retentionSize) > 0 {
		value, err := parseBinarySize(t.retentionSize)
		if err == nil {
			if value < 0 {
				value = -1
			}
			result[retentionBytesName] = strconv.FormatInt(value, 10)
		}
	}
	if len(t.retentionDuration) > 0 {
		value, err := parseDuration(t.retentionDuration)
		if err == nil {
			result[retentionMsName] = strconv.FormatInt(value.Milliseconds(), 10)
		} else {
			fmt.Println("retention duration conversion error:", err)
		}
	}
	return result
}

func (t *topicModCmdType) Run(cmd *cobra.Command, topics []string) error {
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
	if t.shouldDelete {
		return kafkaadmin.DeleteTopics(t.client, topics...)
	}

	config := t.makeConfig()
	err = kafkaadmin.CreateTopics(t.client, t.partitions, t.replicationFactor, config, topics...)
	if err == nil {
		return nil
	}
	var topicErrors kafkaadmin.TopicErrors
	if !errors.As(err, &topicErrors) {
		return err
	}
	var existingTopics []string
	for topic, err := range topicErrors {
		if err != sarama.ErrTopicAlreadyExists {
			return err
		}
		existingTopics = append(existingTopics, topic)
	}
	err = kafkaadmin.CreatePartitions(t.client, t.partitions, existingTopics...)
	if err != nil {
		if !errors.As(err, &topicErrors) {
			return err
		}
		fmt.Println("Errors while updating the number of partitions")
		fmt.Println(topicErrors)
	}
	if len(config) == 0 {
		return nil
	}
	err = kafkaadmin.ModifyTopicConfig(t.client, config, existingTopics...)
	if err != nil {
		if !errors.As(err, &topicErrors) {
			return err
		}
		fmt.Println("Errors while updating topic configs")
		fmt.Println(topicErrors)
	}
	return nil
}

var topicModCmd = &cobra.Command{
	Use:     "mod",
	Aliases: []string{"m"},
	Short:   "modyfy or create topics"}

func init() {
	var runner topicModCmdType
	topicModCmd.RunE = runner.Run
	flags := topicModCmd.Flags()
	flags.Int32VarP(&runner.partitions, "partitions", "p", 1, "the number of partitions for topics")
	flags.Int16VarP(&runner.replicationFactor, "replicas", "r", 1, "replication factor")
	flags.BoolVarP(&runner.shouldDelete, "delete", "d", false, "delete specified partitions (other parameters have no effect if this option is selected)")
	flags.StringVarP(&runner.configName, "config", "c", "", "configuration profile name (compact and infinite hardcoded)")
	flags.StringVarP(&runner.configKey, "key", "k", "", "topic config key to modify")
	flags.StringVarP(&runner.configValue, "value", "v", "", "topic config value to modify")
	flags.StringVar(&runner.segmentSize, "segment-size", "", "maximum segment size in bytes, suffixes K,M,G,T supported")
	flags.StringVar(&runner.segmentDuration, "segment-duration", "", "maximum segment duration in format like 1w3d2h4m5s3ms (one week, 3 days,2 hours, 4 minutes, 5 seconds and 3 milliseconds, fractional numbers are not supported)")
	flags.StringVar(&runner.retentionSize, "retention-size", "", "retention size in bytes, suffixes K,M,G,T supported")
	flags.StringVar(&runner.retentionDuration, "retention-duration", "", "maximum retention duration, see segment-duration for format")
}
