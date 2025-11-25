using Confluent.Kafka;
using MessageTrigger.Core.Consuming;
using MessageTrigger.Core.Processing;
using Microsoft.Extensions.Logging;
using System.Collections.ObjectModel;

namespace MessageTrigger.Kafka
{
    public partial class BufferedKafkaMessageBatchConsumerWithBatchProcessor<TKey, TValue> : BufferedKafkaMessageBatchConsumerBase<TKey, TValue>, IMessageConsumer
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

        protected override async Task<TopicPartitionOffset[]> ProcessMessages(IReadOnlyList<ConsumeResult<TKey, TValue>> batch, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(batch);
            LogProcessingBatchOfMessage(batch.Count);
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

        [LoggerMessage(
            LogLevel.Debug,
            Message = "Processing batch of {batchSize} messages"
        )]
        private partial void LogProcessingBatchOfMessage(int batchSize);
    }
}
