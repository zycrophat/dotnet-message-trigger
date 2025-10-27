using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace MessageTrigger.Kafka
{
    internal abstract class BufferedKafkaMessageConsumerBase<TKey, TValue>
    {
        private const int DefaultChannelSize = 256;
        private readonly ILogger<BufferedKafkaMessageConsumerBase<TKey, TValue>> logger;
        private readonly int channelSize;
        private readonly Func<IConsumer<TKey, TValue>> consumerFactory;
        private readonly string topic;

        protected BufferedKafkaMessageConsumerBase(
            ILogger<BufferedKafkaMessageConsumerBase<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            int channelSize = DefaultChannelSize)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(consumerFactory);
            ArgumentException.ThrowIfNullOrWhiteSpace(topic);
            this.logger = logger;
            this.consumerFactory = consumerFactory;
            this.topic = topic;
            this.channelSize = channelSize;
        }

        public async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var consumer = CreateAndConfigureConsumer();
            var channel = CreateChannel();
            try
            {
                var cnclToken = cts.Token;
                var writeMessagesToChannelTask =
                    WriteMessagesToChannel(consumer, channel.Writer, cnclToken);
                var dispatchMessageProcessingTask =
                    DispatchMessageProcessing(consumer, channel.Reader, cnclToken);

                var completedTask = await Task.WhenAny(
                    writeMessagesToChannelTask,
                    dispatchMessageProcessingTask
                ).ConfigureAwait(false);

                if (completedTask.IsFaulted)
                {
                    throw completedTask.Exception!;
                }
                cnclToken.ThrowIfCancellationRequested();
            }
            catch (Exception)
            {
                consumer.Unsubscribe();
                await cts.CancelAsync().ConfigureAwait(false);
                throw;
            }
        }

        private IConsumer<TKey, TValue> CreateAndConfigureConsumer()
        {
            var consumer = consumerFactory();
            consumer.Subscribe(topic);
            return consumer;
        }

        private Channel<ConsumeResult<TKey, TValue>> CreateChannel()
        {
            var options = new BoundedChannelOptions(channelSize)
            {
                FullMode = BoundedChannelFullMode.Wait
            };
            return Channel.CreateBounded<ConsumeResult<TKey, TValue>>(options);
        }

        private async Task WriteMessagesToChannel(
            IConsumer<TKey, TValue> consumer,
            ChannelWriter<ConsumeResult<TKey, TValue>> writer,
            CancellationToken cancellationToken
        )
        {
            while (true)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var consumeResult = consumer.Consume(cancellationToken);
                    await writer.WriteAsync(consumeResult, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "An exception has been caught while writing messages to the channel.");
                    throw;
                }
            }
        }

        protected abstract Task<long> DispatchMessageProcessing(
            IConsumer<TKey, TValue> consumer,
            ChannelReader<ConsumeResult<TKey, TValue>> channelReader,
            CancellationToken cancellationToken
        );
    }
}
