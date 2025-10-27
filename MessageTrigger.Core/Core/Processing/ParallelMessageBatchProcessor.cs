namespace MessageTrigger.Core.Processing
{
    public class ParallelMessageBatchProcessor<T> : IMessageProcessor<IEnumerable<T>>
    {
        private readonly IMessageProcessor<T> messageProcessor;
        private readonly int maxDegreeOfParallelism;

        public ParallelMessageBatchProcessor(
            IMessageProcessor<T> messageProcessor,
            int maxDegreeOfParallelism
        )
        {
            ArgumentNullException.ThrowIfNull(messageProcessor);
            this.messageProcessor = messageProcessor;
            this.maxDegreeOfParallelism = maxDegreeOfParallelism;
        }

        public async Task ProcessAsync(IEnumerable<T> batch, CancellationToken cancellationToken = default)
        {
            var parallelOptions = new ParallelOptions()
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            };
            
            await Parallel.ForEachAsync(
                batch,
                parallelOptions,
                async (message, cnclToken) =>
                {
                    await messageProcessor.ProcessAsync(message, cnclToken).ConfigureAwait(false);
                }
            ).ConfigureAwait(false);
        }
    }
}
