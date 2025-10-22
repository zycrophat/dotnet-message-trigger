namespace MessageTrigger.Core.Processing
{
    public interface IMessageProcessor<in T>
    {
        public Task ProcessAsync(
            T message,
            CancellationToken cancellationToken = default
        );
    }
}
