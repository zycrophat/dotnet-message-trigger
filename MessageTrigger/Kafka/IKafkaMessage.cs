using System.Security.Cryptography.X509Certificates;

namespace MessageTrigger.Kafka
{
    public interface IKafkaMessage<out TKey, out TValue>
    {
        public TKey Key { get; }
        public TValue Value { get; }
    }
}