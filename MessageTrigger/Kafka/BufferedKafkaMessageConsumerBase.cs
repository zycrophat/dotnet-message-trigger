using Confluent.Kafka;
using MessageTrigger.Common;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;
using System.Threading.Channels;

namespace MessageTrigger.Kafka
{
    internal abstract class BufferedKafkaMessageConsumerBatchMessageProcessorBase<TKey, TValue>
    {
        private const int DefaultBatchSize = 64;
        private const int DefaultChannelSize = 256;
        private static readonly TimeSpan DefaultBatchTimeout = TimeSpan.FromMilliseconds(250);
        private readonly ILogger<BufferedKafkaMessageConsumerBatchMessageProcessorBase<TKey, TValue>> logger;
        private readonly int batchSize;
        private readonly TimeSpan batchTimeout;
        private readonly int channelSize;
        private readonly Func<IConsumer<TKey, TValue>> consumerFactory;
        private readonly string topic;

        protected BufferedKafkaMessageConsumerBatchMessageProcessorBase(
            ILogger<BufferedKafkaMessageConsumerBatchMessageProcessorBase<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            int channelSize = DefaultChannelSize,
            int batchSize = DefaultBatchSize,
            TimeSpan? batchTimeout = null!)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(consumerFactory);
            ArgumentException.ThrowIfNullOrWhiteSpace(topic);
            this.logger = logger;
            this.consumerFactory = consumerFactory;
            this.topic = topic;
            this.channelSize = channelSize;
            this.batchSize = batchSize;
            this.batchTimeout = batchTimeout ?? DefaultBatchTimeout;
        }

        private static void StoreOffsetsForBatch(
            IConsumer<TKey, TValue> consumer,
            TopicPartitionOffset[] topicPartitionOffsets
        )
        {
            foreach (var topicPartitionOffset in topicPartitionOffsets)
            {
                consumer.StoreOffset(topicPartitionOffset);
            }
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

        private async Task<long> DispatchMessageProcessing(
            IConsumer<TKey, TValue> consumer,
            ChannelReader<ConsumeResult<TKey, TValue>> channelReader,
            CancellationToken cancellationToken
        )
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await channelReader
                    .Batch(
                        batchSize,
                        singleReader: true
                    )
                    .WithTimeout(batchTimeout)
                    .ReadAllAsync(
                        async (batch) =>
                        {
                            await ProcessBatch(
                                consumer,
                                batch,
                                cancellationToken
                            ).ConfigureAwait(false);
                        },
                        cancellationToken
                    ).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An exception has been caught while processing a batch of messages.");
                throw;
            }
        }

        private async Task ProcessBatch(
            IConsumer<TKey, TValue> consumer,
            List<ConsumeResult<TKey, TValue>> batch,
            CancellationToken cancellationToken
        )
        {
            var partitionOffsets = await ProcessMessages(batch, cancellationToken).ConfigureAwait(false);
            StoreOffsetsForBatch(consumer, partitionOffsets);
        }

        abstract protected Task<TopicPartitionOffset[]> ProcessMessages(
            List<ConsumeResult<TKey, TValue>> batch,
            CancellationToken cancellationToken
        );
    }
}