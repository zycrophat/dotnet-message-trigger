using MessageTrigger.Core.Consuming;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageTrigger.Hosting
{
    internal class MessageConsumerBackgroundService : BackgroundService
    {
        private readonly ILogger<MessageConsumerBackgroundService> logger;
        private readonly IMessageConsumer messageConsumer;

        public MessageConsumerBackgroundService(
            ILogger<MessageConsumerBackgroundService> logger,
            IMessageConsumer messageConsumer)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(messageConsumer);
            this.logger = logger;
            this.messageConsumer = messageConsumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    logger.LogInformation("Starting message consumption");
                    await messageConsumer.ConsumeAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Graceful shutdown
                    logger.LogInformation("Message consumption cancelled");
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Error in message consumption");
                }
            }
        }
    }
}
