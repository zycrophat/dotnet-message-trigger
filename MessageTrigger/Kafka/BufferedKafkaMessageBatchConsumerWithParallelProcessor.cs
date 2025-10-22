using Confluent.Kafka;
using MessageTrigger.Common;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace MessageTrigger.Kafka
{
    internal class BufferedKafkaMessageBatchConsumerWithParallelProcessor<TKey, TValue> : BufferedKafkaMessageBatchConsumerBase<TKey, TValue>
    {
        private readonly ILogger<BufferedKafkaMessageBatchConsumerWithParallelProcessor<TKey, TValue>> logger;
        private readonly IMessageProcessor<IKafkaMessage<TKey, TValue>> kafkaMessageProcessor;
        private readonly int maxDegreeOfParallelism;

        public BufferedKafkaMessageBatchConsumerWithParallelProcessor(
            ILogger<BufferedKafkaMessageBatchConsumerWithParallelProcessor<TKey, TValue>> logger,
            Func<IConsumer<TKey, TValue>> consumerFactory,
            string topic,
            IMessageProcessor<IKafkaMessage<TKey, TValue>> kafkaMessageProcessor,
            int channelSize,
            int batchSize,
            TimeSpan? batchTimeout,
            int? maxDegreeOfParallelism = null!
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
            ArgumentNullException.ThrowIfNull(kafkaMessageProcessor);
            this.logger = logger;
            this.kafkaMessageProcessor = kafkaMessageProcessor;
            this.maxDegreeOfParallelism = maxDegreeOfParallelism ?? Environment.ProcessorCount;
        }

        protected override async Task<TopicPartitionOffset[]> ProcessMessages(List<ConsumeResult<TKey, TValue>> batch, CancellationToken cancellationToken)
        {
            logger.LogInformation(
                "Processing batch of {BatchSize} messages with max degree of parallelism {MaxDegreeOfParallelism}",
                batch.Count,
                maxDegreeOfParallelism
            );
            var kafkaMessages =
                batch
                .Select(static consumeResult =>
                    new KafkaMessage<TKey, TValue>(
                        consumeResult.Message.Key,
                        consumeResult.Message.Value,
                        consumeResult.TopicPartitionOffset
                    )
                );

            var partitionToMaxOffset = new ConcurrentDictionary<int, TopicPartitionOffset>();

            await Parallel.ForEachAsync(
                kafkaMessages,
                new ParallelOptions
                { 
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = maxDegreeOfParallelism
                },
                async (kafkaMessage, cnclToken) =>
                {
                    await kafkaMessageProcessor.ProcessAsync(
                        kafkaMessage,
                        cnclToken
                    ).ConfigureAwait(false);
                    partitionToMaxOffset.AddOrUpdate(
                        kafkaMessage.TopicPartitionOffset.Partition,
                        kafkaMessage.TopicPartitionOffset,
                        (_, currentMaxTopicPartitionOffset) =>
                            kafkaMessage.TopicPartitionOffset.Offset.Value > currentMaxTopicPartitionOffset.Offset.Value
                            ? kafkaMessage.TopicPartitionOffset
                            : currentMaxTopicPartitionOffset
                    );
                }
            ).ConfigureAwait(false);

            return [.. partitionToMaxOffset.Values ];
        }
    }
}
