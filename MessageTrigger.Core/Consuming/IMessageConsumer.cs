namespace MessageTrigger.Core.Consuming
{
    public interface IMessageConsumer
    {
        public Task ConsumeAsync(CancellationToken cancellationToken = default);
    }
}
