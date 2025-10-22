using Confluent.Kafka;
using MessageTrigger.Core.Processing;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;
using System.Threading.Channels;

namespace MessageTrigger.Kafka
{
    internal class BufferedKafkaMessageConsumerWithSequentialProcessor<TKey, TValue> : BufferedKafkaMessageConsumerBase<TKey, TValue>
    {
        private readonly ILogger<BufferedKafkaMessageConsumerWithSequentialProcessor<TKey, TValue>> logger;
        private readonly IMessageProcessor<IKafkaMessage<TKey, TValue>> kafkaMessageProcessor;

        public BufferedKafkaMessageConsumerWithSequentialProcessor(
            ILogger<BufferedKafkaMessageConsumerWithSequentialProcessor<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            IMessageProcessor<IKafkaMessage<TKey, TValue>> kafkaMessageProcessor,
            int channelSize
        ) : base(
            logger,
            consumerFactory,
            topic,
            channelSize
        )
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(kafkaMessageProcessor);
            this.logger = logger;
            this.kafkaMessageProcessor = kafkaMessageProcessor;
        }

        protected override async Task<long> DispatchMessageProcessing(
            IConsumer<TKey, TValue> consumer,
            ChannelReader<ConsumeResult<TKey, TValue>> channelReader,
            CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await channelReader
                    .ReadAllAsync(
                        async (consumeResult) =>
                        {
                            await ProcessMessage(
                                consumer,
                                consumeResult,
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

        private async Task ProcessMessage(
            IConsumer<TKey, TValue> consumer,
            ConsumeResult<TKey, TValue> consumeResult,
            CancellationToken cancellationToken)
        {
            await kafkaMessageProcessor.ProcessAsync(
                new KafkaMessage<TKey, TValue>(
                    consumeResult.Message.Key,
                    consumeResult.Message.Value,
                    consumeResult.TopicPartitionOffset
                ),
                cancellationToken
            ).ConfigureAwait(false);

            consumer.StoreOffset(consumeResult.TopicPartitionOffset);
        }
    }
}
