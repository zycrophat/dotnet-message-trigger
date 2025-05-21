namespace MessageTrigger.Common
{
    public class SequentialMessageBatchProcessor<T> : IMessageProcessor<IEnumerable<T>>
    {
        private readonly IMessageProcessor<T> messageProcessor;

        public SequentialMessageBatchProcessor(
            IMessageProcessor<T> messageProcessor
        )
        {
            ArgumentNullException.ThrowIfNull(messageProcessor);
            this.messageProcessor = messageProcessor;
        }

        public async Task ProcessAsync(IEnumerable<T> batch, CancellationToken cancellationToken = default)
        {
            foreach (var message in batch)
            {
                await messageProcessor.ProcessAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
