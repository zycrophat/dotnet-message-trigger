using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;
using System.Collections.ObjectModel;
using System.Threading.Channels;

namespace MessageTrigger.Kafka
{
    public abstract partial class BufferedKafkaMessageBatchConsumerBase<TKey, TValue> : BufferedKafkaMessageConsumerBase<TKey, TValue>
    {
        private const int DefaultBatchSize = 64;
        private const int DefaultChannelSize = 256;
        private static readonly TimeSpan DefaultBatchTimeout = TimeSpan.FromMilliseconds(250);
        private readonly ILogger<BufferedKafkaMessageBatchConsumerBase<TKey, TValue>> logger;
        private readonly int batchSize;
        private readonly TimeSpan batchTimeout;

        private protected BufferedKafkaMessageBatchConsumerBase(
            ILogger<BufferedKafkaMessageBatchConsumerBase<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            int channelSize = DefaultChannelSize,
            int batchSize = DefaultBatchSize,
            TimeSpan? batchTimeout = null!) : base(
                logger,
                consumerFactory,
                topic,
                channelSize)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(consumerFactory);
            ArgumentException.ThrowIfNullOrWhiteSpace(topic);
            this.logger = logger;
            this.batchSize = batchSize;
            this.batchTimeout = batchTimeout ?? DefaultBatchTimeout;
        }

        protected override async Task<long> DispatchMessageProcessing(
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
                LogExceptionWhileProcessingBatch(ex);
                throw;
            }
        }

        [LoggerMessage(
            LogLevel.Error,
            Message = "An exception has been caught while processing a batch of messages."
        )]
        private partial void LogExceptionWhileProcessingBatch(Exception exception);

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
            IReadOnlyList<ConsumeResult<TKey, TValue>> batch,
            CancellationToken cancellationToken
        );

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
    }
}