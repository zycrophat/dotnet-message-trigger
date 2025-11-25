using Confluent.Kafka;

namespace MessageTrigger.Kafka
{
    internal record class KafkaMessage<TKey, TValue>(TKey Key, TValue Value, TopicPartitionOffset TopicPartitionOffset) : IKafkaMessage<TKey, TValue>;
}
