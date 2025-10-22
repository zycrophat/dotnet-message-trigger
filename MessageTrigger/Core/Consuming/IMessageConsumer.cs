namespace MessageTrigger.Core.Consuming
{
    internal interface IMessageConsumer
    {
        public Task ConsumeAsync(CancellationToken cancellationToken = default);
    }
}
