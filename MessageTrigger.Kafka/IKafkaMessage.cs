using Confluent.Kafka;

namespace MessageTrigger.Kafka
{
    public interface IKafkaMessage<out TKey, out TValue>
    {
        public TKey Key { get; }
        public TValue Value { get; }
        public TopicPartitionOffset TopicPartitionOffset { get; }
    }
}