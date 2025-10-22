using Microsoft.Extensions.Logging;

namespace MessageTrigger.Core.Processing
{
    public class MessageProcessor<T> : IMessageProcessor<T>
    {
        private readonly ILogger<MessageProcessor<T>> logger;
        private readonly Func<T, CancellationToken, Task> messageHandler;

        public MessageProcessor(
            ILogger<MessageProcessor<T>> logger,
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
            await messageHandler(queueMessage, cancellationToken).ConfigureAwait(false);
        }
    }

}
