using MessageTrigger.Core.Consuming;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageTrigger.Hosting
{
    public partial class MessageConsumerBackgroundService : BackgroundService
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
                    LogStartingMessageConsumption();
                    await messageConsumer.ConsumeAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Graceful shutdown
                    LogConsumptionCancelled();
                }
                catch (Exception ex)
                {
                    LogErrorInMessageConsumption(ex);
                }
            }
        }

        [LoggerMessage(
            LogLevel.Information,
            Message = "Starting message consumption"
        )]
        private partial void LogStartingMessageConsumption();

        [LoggerMessage(
            LogLevel.Information,
            Message = "Message consumption cancelled"
        )]
        private partial void LogConsumptionCancelled();

        [LoggerMessage(
            LogLevel.Warning,
            Message = "Error in message consumption"
        )]
        private partial void LogErrorInMessageConsumption(Exception exception);
    }
}
