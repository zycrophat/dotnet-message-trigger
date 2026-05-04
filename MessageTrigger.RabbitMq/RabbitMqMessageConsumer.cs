using MessageTrigger.Core.Consuming;
using MessageTrigger.Core.Processing;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessageTrigger.RabbitMq
{
    internal partial class RabbitMqMessageConsumer : IMessageConsumer
    {
        private readonly ILogger<RabbitMqMessageConsumer> logger;
        private readonly ConnectionFactory connectionFactory;
        private readonly IMessageProcessor<BasicDeliverEventArgs> messageProcessor;
        private readonly string queueName;
        private readonly Func<Exception, bool> isTransient;

        public RabbitMqMessageConsumer(
            ILogger<RabbitMqMessageConsumer> logger,
            ConnectionFactory connectionFactory,
            IMessageProcessor<BasicDeliverEventArgs> messageProcessor,
            string queueName,
            Func<Exception, bool> isTransient
        )
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(connectionFactory);
            ArgumentNullException.ThrowIfNull(messageProcessor);
            ArgumentException.ThrowIfNullOrEmpty(queueName);
            ArgumentNullException.ThrowIfNull(isTransient);
            this.logger = logger;
            this.connectionFactory = connectionFactory;
            this.messageProcessor = messageProcessor;
            this.queueName = queueName;
            this.isTransient = isTransient;
        }

        public RabbitMqMessageConsumer(
            ILogger<RabbitMqMessageConsumer> logger,
            ConnectionFactory connectionFactory,
            IMessageProcessor<BasicDeliverEventArgs> messageProcessor,
            string queueName
        ) : this(
            logger,
            connectionFactory,
            messageProcessor,
            queueName,
            True
        )
        {
            // NOP
        }

        private static bool True(Exception _) => true;

        public async Task ConsumeAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await using var connection =
                    await connectionFactory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);

                await using var channel =
                    await connection.CreateChannelAsync(null!, cancellationToken).ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (_, ea) =>
                {
                    await ProcessEventArgs(ea, channel);
                };

                LogStartingConsumption();
                _ = await channel.BasicConsumeAsync(
                    queue: queueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken
                ).ConfigureAwait(false);
            } catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogExceptionDuringConsumption(ex);
            }            
        }

        private async Task ProcessEventArgs(BasicDeliverEventArgs ea, IChannel channel)
        {
            try
            {
                await messageProcessor.ProcessAsync(
                    ea,
                    ea.CancellationToken
                ).ConfigureAwait(false);

                await channel.BasicAckAsync(
                    ea.DeliveryTag,
                    multiple: false,
                    ea.CancellationToken
                ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (isTransient(ex))
                {
                    await channel.BasicRejectAsync(
                        ea.DeliveryTag,
                        requeue: true,
                        ea.CancellationToken
                    ).ConfigureAwait(false);
                }
                else
                {
                    await channel.BasicRejectAsync(
                        ea.DeliveryTag,
                        requeue: false,
                        ea.CancellationToken
                    ).ConfigureAwait(false);
                }
            }
        }

        [LoggerMessage(
            Level = LogLevel.Warning,
            Message = "Message consumption failed with an exception"
        )]
        private partial void LogExceptionDuringConsumption(Exception ex);

        [LoggerMessage(
            Level = LogLevel.Debug,
            Message = "Starting to consume messages"
        )]
        private partial void LogStartingConsumption();
    }
}
