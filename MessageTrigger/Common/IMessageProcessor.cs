using Azure.Storage.Queues;

namespace MessageTrigger.Common
{
    public interface IMessageProcessor<in T>
    {
        public Task ProcessAsync(
            T message,
            CancellationToken cancellationToken = default
        );
    }
}
