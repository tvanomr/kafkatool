package kafkaadmin

import "github.com/Shopify/sarama"

func NewConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_4_0_0
	return conf
}

func NewDefaultClient(addrs []string) (sarama.Client, error) {
	return sarama.NewClient(addrs, NewConfig())
}

type RawConfig = map[string]*sarama.ConfigEntry

type TopicsConfigs = map[string]RawConfig

func GetTopicConfigs(client sarama.Client, topics ...string) (TopicsConfigs, error) {
	request := sarama.DescribeConfigsRequest{Version: 2}
	for _, topic := range topics {
		request.Resources = append(request.Resources, &sarama.ConfigResource{Type: sarama.TopicResource, Name: topic})
	}
	broker, err := client.Controller()
	if err != nil {
		return nil, err
	}
	response, err := broker.DescribeConfigs(&request)
	result := make(TopicsConfigs)
	for _, resource := range response.Resources {
		if resource.ErrorCode != int16(sarama.ErrNoError) {
			return nil, sarama.KError(resource.ErrorCode)
		}
		configs := make(RawConfig)
		for _, config := range resource.Configs {
			configs[config.Name] = config
		}
		if len(configs) > 0 {
			result[resource.Name] = configs
		}
	}
	return result, nil
}

func GetTopicRange(client sarama.Client, topic string, partition int32) (min int64, max int64, err error) {
	min, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return 0, 0, err
	}
	max, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, err
	}
	return min, max, err
}
