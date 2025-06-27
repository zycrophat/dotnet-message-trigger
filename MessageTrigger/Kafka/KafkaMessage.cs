using Confluent.Kafka;

namespace MessageTrigger.Kafka
{
    public record class KafkaMessage<TKey, TValue>(TKey Key, TValue Value, TopicPartitionOffset TopicPartitionOffset) : IKafkaMessage<TKey, TValue>;
}
