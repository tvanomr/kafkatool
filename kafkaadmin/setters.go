package kafkaadmin

import (
	"github.com/Shopify/sarama"
	"strconv"
	"time"
)

const DefaultDuration = time.Hour

type TopicErrors map[string]sarama.KError

func (t TopicErrors) Error() string {
	result := ""
	for name, value := range t {
		if len(result) != 0 {
			result += "\n"
		}
		result += "Error at " + name + ":" + value.Error() + " (" + strconv.Itoa(int(value)) + ")"
	}
	return result
}

func DeleteTopics(client sarama.Client, topics ...string) error {
	broker, err := client.Controller()
	if err != nil {
		return err
	}
	result, err := broker.DeleteTopics(&sarama.DeleteTopicsRequest{
		Version: 3,
		Topics:  topics,
		Timeout: DefaultDuration})
	if err != nil {
		return err
	}
	errors := make(TopicErrors)
	for name, value := range result.TopicErrorCodes {
		if value != sarama.ErrNoError {
			errors[name] = value
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

func CreateTopics(client sarama.Client, partitions int32, replicationFactor int16, configs map[string]string, topics ...string) error {
	broker, err := client.Controller()
	if err != nil {
		return err
	}
	var request sarama.CreateTopicsRequest
	request.Version = 4
	request.Timeout = DefaultDuration
	request.TopicDetails = make(map[string]*sarama.TopicDetail)
	for _, topic := range topics {
		detail := &sarama.TopicDetail{
			ReplicationFactor: replicationFactor,
			NumPartitions:     partitions}
		detail.ConfigEntries = make(map[string]*string)
		for name, value := range configs {
			pvalue := new(string)
			*pvalue = value
			detail.ConfigEntries[name] = pvalue
		}
		detail.ReplicaAssignment = make(map[int32][]int32)
		request.TopicDetails[topic] = detail
	}
	response, err := broker.CreateTopics(&request)
	if err != nil {
		return err
	}
	errors := make(TopicErrors)
	for topic, value := range response.TopicErrors {
		if value != nil && value.Err != sarama.ErrNoError {
			errors[topic] = value.Err
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

func CreatePartitions(client sarama.Client, partitions int32, topics ...string) error {
	broker, err := client.Controller()
	if err != nil {
		return err
	}
	var request sarama.CreatePartitionsRequest
	request.Timeout = DefaultDuration
	request.TopicPartitions = make(map[string]*sarama.TopicPartition)
	for _, topic := range topics {
		request.TopicPartitions[topic] = &sarama.TopicPartition{Count: partitions}
	}
	response, err := broker.CreatePartitions(&request)
	if err != nil {
		return err
	}
	errors := make(TopicErrors)
	for topic, value := range response.TopicPartitionErrors {
		if value != nil && value.Err != sarama.ErrNoError {
			errors[topic] = value.Err
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

func ModifyTopicConfig(client sarama.Client, config map[string]string, topics ...string) error {
	broker, err := client.Controller()
	if err != nil {
		return err
	}
	var request sarama.AlterConfigsRequest
	for _, topic := range topics {
		resource := &sarama.AlterConfigsResource{
			Type: sarama.TopicResource,
			Name: topic}
		resource.ConfigEntries = make(map[string]*string)
		for name, value := range config {
			pvalue := new(string)
			*pvalue = value
			resource.ConfigEntries[name] = pvalue
		}
		request.Resources = append(request.Resources, resource)
	}
	response, err := broker.AlterConfigs(&request)
	if err != nil {
		return err
	}
	errors := make(TopicErrors)
	for _, resource := range response.Resources {
		if resource != nil && sarama.KError(resource.ErrorCode) != sarama.ErrNoError {
			errors[resource.Name] = sarama.KError(resource.ErrorCode)
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}
