using Confluent.Kafka;
using MessageTrigger.Common;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;
using System.Threading.Channels;

namespace MessageTrigger.Kafka
{
    internal class BufferedKafkaMessageConsumerBatchMessageProcessor<TKey, TValue>
    {
        private const int DefaultChannelSize = 256;
        private const int DefaultBatchSize = 64;
        private static readonly TimeSpan DefaultBatchTimeout = TimeSpan.FromMilliseconds(250);

        private readonly ILogger<BufferedKafkaMessageConsumerBatchMessageProcessor<TKey, TValue>> logger;
        private readonly int channelSize;
        private readonly int batchSize;
        private readonly TimeSpan batchTimeout;
        private readonly Func<IConsumer<TKey, TValue>> consumerFactory;
        private readonly string topic;
        private readonly IMessageProcessor<IEnumerable<IKafkaMessage<TKey, TValue>>> kafkaMessageBatchProcessor;

        public BufferedKafkaMessageConsumerBatchMessageProcessor(
            ILogger<BufferedKafkaMessageConsumerBatchMessageProcessor<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            IMessageProcessor<IEnumerable<IKafkaMessage<TKey, TValue>>> kafkaMessageBatchProcessor,
            int channelSize = DefaultChannelSize,
            int batchSize = DefaultBatchSize,
            TimeSpan? batchTimeout = null!)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(consumerFactory);
            ArgumentNullException.ThrowIfNull(kafkaMessageBatchProcessor);
            ArgumentException.ThrowIfNullOrWhiteSpace(topic);
            this.logger = logger;
            this.consumerFactory = consumerFactory;
            this.topic = topic;
            this.kafkaMessageBatchProcessor = kafkaMessageBatchProcessor;
            this.channelSize = channelSize;
            this.batchSize = batchSize;
            this.batchTimeout = batchTimeout ?? DefaultBatchTimeout;
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

        private Channel<ConsumeResult<TKey, TValue>> CreateChannel()
        {
            var options = new BoundedChannelOptions(channelSize)
            {
                FullMode = BoundedChannelFullMode.Wait
            };
            return Channel.CreateBounded<ConsumeResult<TKey, TValue>>(options);
        }

        private IConsumer<TKey, TValue> CreateAndConfigureConsumer()
        {
            var consumer = consumerFactory();
            consumer.Subscribe(topic);
            return consumer;
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

        private async Task ProcessBatch(
            IConsumer<TKey, TValue> consumer,
            List<ConsumeResult<TKey, TValue>> batch,
            CancellationToken cancellationToken
        )
        {
            await ProcessMessages(batch, cancellationToken).ConfigureAwait(false);
            StoreOffsetsForBatch(consumer, batch);
        }

        private static void StoreOffsetsForBatch(
            IConsumer<TKey, TValue> consumer,
            List<ConsumeResult<TKey, TValue>> batch
        )
        {
            var maxOffsetPerPartition =
                batch
                .GroupBy(
                    static consumeResult => consumeResult.Partition
                )
                .Select(
                    static consumeResultGroup =>
                        consumeResultGroup.MaxBy(
                            static consumeResult => consumeResult.Offset.Value
                        )
                );

            foreach (var x in maxOffsetPerPartition)
            {
                consumer.StoreOffset(x);
            }
        }

        private async Task ProcessMessages(List<ConsumeResult<TKey, TValue>> batch, CancellationToken cancellationToken)
        {
            var kafkaMessages =
                batch
                .Select(static consumeResult =>
                    new KafkaMessage<TKey, TValue>(
                        consumeResult.Message.Key,
                        consumeResult.Message.Value,
                        consumeResult.TopicPartitionOffset
                    )
                )
                .ToArray();
            await kafkaMessageBatchProcessor.ProcessAsync(
                kafkaMessages,
                cancellationToken
            ).ConfigureAwait(false);
        }
    }
}
