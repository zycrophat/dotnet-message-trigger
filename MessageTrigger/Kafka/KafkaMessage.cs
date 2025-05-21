namespace MessageTrigger.Kafka
{
    public record class KafkaMessage<TKey, TValue>(TKey Key, TValue Value) : IKafkaMessage<TKey, TValue>;
}
