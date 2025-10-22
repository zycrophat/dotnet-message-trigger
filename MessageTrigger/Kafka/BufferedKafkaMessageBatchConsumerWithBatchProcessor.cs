using Confluent.Kafka;
using MessageTrigger.Core.Consuming;
using MessageTrigger.Core.Processing;
using Microsoft.Extensions.Logging;

namespace MessageTrigger.Kafka
{
    internal class BufferedKafkaMessageBatchConsumerWithBatchProcessor<TKey, TValue> : BufferedKafkaMessageBatchConsumerBase<TKey, TValue>, IMessageConsumer
    {
        private readonly ILogger<BufferedKafkaMessageBatchConsumerWithBatchProcessor<TKey, TValue>> logger;
        private readonly IMessageProcessor<IEnumerable<IKafkaMessage<TKey, TValue>>> kafkaMessageBatchProcessor;

        public BufferedKafkaMessageBatchConsumerWithBatchProcessor(
            ILogger<BufferedKafkaMessageBatchConsumerWithBatchProcessor<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            IMessageProcessor<IEnumerable<IKafkaMessage<TKey, TValue>>> kafkaMessageBatchProcessor,
            int channelSize,
            int batchSize,
            TimeSpan? batchTimeout
        ) : base(
            logger,
            consumerFactory,
            topic,
            channelSize,
            batchSize,
            batchTimeout
        )
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(kafkaMessageBatchProcessor);
            this.logger = logger;
            this.kafkaMessageBatchProcessor = kafkaMessageBatchProcessor;
        }

        protected override async Task<TopicPartitionOffset[]> ProcessMessages(List<ConsumeResult<TKey, TValue>> batch, CancellationToken cancellationToken)
        {
            logger.LogDebug(
                "Processing batch of {BatchSize} messages",
                batch.Count
            );
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

            var maxTopicPartitionOffsets =
                batch
                .GroupBy(
                    static consumeResult => consumeResult.Partition
                )
                .Select(
                    static consumeResultGroup =>
                        consumeResultGroup.MaxBy(
                            static consumeResult => consumeResult.Offset.Value
                        )!.TopicPartitionOffset
                )
                .ToArray();

            return maxTopicPartitionOffsets;
        }
    }
}
