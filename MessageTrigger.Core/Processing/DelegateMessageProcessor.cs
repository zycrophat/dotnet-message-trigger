using Microsoft.Extensions.Logging;

namespace MessageTrigger.Core.Processing
{
    public class DelegateMessageProcessor<T> : IMessageProcessor<T>
    {
        private readonly ILogger<DelegateMessageProcessor<T>> logger;
        private readonly Func<T, CancellationToken, Task> messageHandler;

        public DelegateMessageProcessor(
            ILogger<DelegateMessageProcessor<T>> logger,
            Func<T, CancellationToken, Task> messageHandler)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(messageHandler);
            this.logger = logger;
            this.messageHandler = messageHandler;
        }

        public async Task ProcessAsync(
            T queueMessage,
            CancellationToken cancellationToken
        )
        {
            logger.LogDebug("Invoking delegate message handler");
            await messageHandler(queueMessage, cancellationToken).ConfigureAwait(false);
        }
    }

}
