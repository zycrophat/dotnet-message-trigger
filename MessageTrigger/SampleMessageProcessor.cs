using Azure.Storage.Queues.Models;
using MessageTrigger.Core.Processing;
using Microsoft.Extensions.Logging;

namespace MessageTrigger
{
    internal partial class SampleMessageProcessor : IMessageProcessor<QueueMessage>
    {
        private readonly ILogger<SampleMessageProcessor> logger;

        public SampleMessageProcessor(
            ILogger<SampleMessageProcessor> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);
            this.logger = logger;
        }

        public async Task ProcessAsync(QueueMessage message, CancellationToken cancellationToken = default)
        {
            LogProcessingMessage(message.MessageId);
            await Task.Delay(TimeSpan.FromSeconds(60), cancellationToken).ConfigureAwait(false); // Simulate some processing delay
            LogFinishedProcessingMessage(message.MessageId);
        }

        [LoggerMessage(
            LogLevel.Information,
            Message = "Processing message: {messageId}"
        )]
        private partial void LogProcessingMessage(string messageId);

        [LoggerMessage(
            LogLevel.Information,
            Message = "Finished processing message: {messageId}"
        )]
        private partial void LogFinishedProcessingMessage(string messageId);
    }
}
