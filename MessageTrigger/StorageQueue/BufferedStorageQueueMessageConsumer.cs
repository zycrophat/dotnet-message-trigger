using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using MessageTrigger.Common;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;
using System.Threading.Channels;

namespace MessageTrigger.StorageQueue
{
    public class BufferedStorageQueueMessageConsumer
    {
        private const int DefaultChannelSize = 256;
        private const int DefaultNumberOfMessagesPerFetch = 32;
        private static readonly TimeSpan DefaultVisibilityTimeout = TimeSpan.FromSeconds(30);

        private readonly ILogger<BufferedStorageQueueMessageConsumer> logger;
        private readonly int channelSize;
        private readonly int numberOfMessagesPerFetch;
        private readonly int maxDegreeOfParallelism;
        private readonly TimeSpan visibilityTimeout;
        private readonly QueueClient queueClient;
        private readonly IMessageProcessor<QueueMessage> storageQueueMessageProcessor;

        public BufferedStorageQueueMessageConsumer(
            ILogger<BufferedStorageQueueMessageConsumer> logger,
            QueueClient queueClient,
            IMessageProcessor<QueueMessage> storageQueueMessageProcessor,
            int channelSize = DefaultChannelSize,
            int numberOfMessagesPerFetch = DefaultNumberOfMessagesPerFetch,
            int? maxDegreeOfParallelism = null!,
            TimeSpan? visibilityTimeout = null!)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(queueClient);
            ArgumentNullException.ThrowIfNull(storageQueueMessageProcessor);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(numberOfMessagesPerFetch, 0);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(numberOfMessagesPerFetch, 32);
            this.logger = logger;
            this.queueClient = queueClient;
            this.storageQueueMessageProcessor = storageQueueMessageProcessor;
            this.channelSize = channelSize;
            this.numberOfMessagesPerFetch = numberOfMessagesPerFetch;
            this.visibilityTimeout = visibilityTimeout ?? DefaultVisibilityTimeout;
            this.maxDegreeOfParallelism = maxDegreeOfParallelism ?? Environment.ProcessorCount;
        }

        public async Task ConsumeAsync(CancellationToken cancellationToken)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var channel = CreateChannel();
            try
            {
                var cnclToken = cts.Token;
                var writeMessagesToChannelTask =
                    WriteMessagesToChannelAsync(channel.Writer, cnclToken);
                var dispatchMessageProcessingTask =
                    DispatchMessageProcessingAsync(channel.Reader, cnclToken);
                
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
                await cts.CancelAsync().ConfigureAwait(false);
                throw;
            }
        }

        private Channel<QueueMessageInTransit> CreateChannel()
        {
            var options = new BoundedChannelOptions(channelSize)
            {
                FullMode = BoundedChannelFullMode.Wait
            };
            return Channel.CreateBounded<QueueMessageInTransit>(options);
        }

        private async Task<long> DispatchMessageProcessingAsync(
            ChannelReader<QueueMessageInTransit> channelReader,
            CancellationToken cancellationToken
        )
        {
            try
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    return await channelReader
                        .ReadAllConcurrentlyAsync(
                            maxDegreeOfParallelism,
                            cancellationToken,
                            async messageInTransit =>
                            {
                                await ProcessMessageAsync(
                                    messageInTransit,
                                    cancellationToken
                                ).ConfigureAwait(false);
                            }
                        ).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "An exception has been caught while processing a batch of messages.");
                    throw;
                }
        }

        private async Task WriteMessagesToChannelAsync(
            ChannelWriter<QueueMessageInTransit> writer,
            CancellationToken cancellationToken
        )
        {
            while (true)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var response = await queueClient.ReceiveMessagesAsync(
                        numberOfMessagesPerFetch,
                        visibilityTimeout,
                        cancellationToken
                    ).ConfigureAwait(false);
                    
                    if (response.Value.Length != 0)
                    {
                        foreach (var message in response.Value)
                        {
                            await WriteMessageToChannel(writer, message, cancellationToken).ConfigureAwait(false);
                        }
                    } else
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "An exception has been caught while writing messages to the channel.");
                    throw;
                }
            }
        }

        private async Task WriteMessageToChannel(ChannelWriter<QueueMessageInTransit> writer, QueueMessage message, CancellationToken cancellationToken)
        {
            logger.LogDebug("Received message {MessageId} with pop receipt {PopReceipt}", message.MessageId, message.PopReceipt);
            var messageInTransit = new QueueMessageInTransit(
                logger,
                queueClient,
                visibilityTimeout,
                message
            );
            messageInTransit.StartUpdateVisibilityTimeout(cancellationToken);
            await writer.WriteAsync(messageInTransit, cancellationToken).ConfigureAwait(false);
        }

        private async Task ProcessMessageAsync(QueueMessageInTransit queueMessageInTransit, CancellationToken cancellationToken)
        {
            using(queueMessageInTransit)
            {
                var queueMessage = queueMessageInTransit.QueueMessage;
                await storageQueueMessageProcessor.ProcessAsync(
                    queueMessage,
                    cancellationToken
                ).ConfigureAwait(false);
                var popReceipt = await queueMessageInTransit.StopUpdateVisibilityTimeoutAsync().ConfigureAwait(false);
                logger.LogDebug("Deleting message {MessageId} from the queue with pop receipt {PopReceipt}", queueMessage.MessageId, popReceipt);
                var response = await queueClient.DeleteMessageAsync(
                    queueMessage.MessageId,
                    popReceipt,
                    cancellationToken
                ).ConfigureAwait(false);
            }
        }

        private class QueueMessageInTransit : IDisposable
        {
            private readonly ILogger<BufferedStorageQueueMessageConsumer> logger;
            private readonly QueueClient queueClient;
            private readonly TimeSpan visibilityTimeout;
            private Task<string> updateVisibilityTimeoutTask = null!;
            private CancellationTokenSource cancellationTokenSourceForVisibilityTimeoutUpdate = null!;
            private bool disposedValue;

            internal QueueMessageInTransit(
                ILogger<BufferedStorageQueueMessageConsumer> logger,
                QueueClient queueClient,
                TimeSpan visibilityTimeout,
                QueueMessage queueMessage)
            {
                this.logger = logger;
                this.queueClient = queueClient;
                this.visibilityTimeout = visibilityTimeout;
                QueueMessage = queueMessage;
            }

            internal QueueMessage QueueMessage { get; private init; }

            internal void StartUpdateVisibilityTimeout(CancellationToken cancellationToken)
            {
                cancellationTokenSourceForVisibilityTimeoutUpdate = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                updateVisibilityTimeoutTask = UpdateVisibilityTimeoutAsync(cancellationTokenSourceForVisibilityTimeoutUpdate.Token);
            }

            internal async Task<string> StopUpdateVisibilityTimeoutAsync()
            {
                await cancellationTokenSourceForVisibilityTimeoutUpdate.CancelAsync().ConfigureAwait(false);

                var popReceipt = await updateVisibilityTimeoutTask.ConfigureAwait(false);
                return popReceipt;
            }

            private async Task<string> UpdateVisibilityTimeoutAsync(CancellationToken cancellationToken)
            {
                var popReceipt = QueueMessage.PopReceipt;
                var delay = visibilityTimeout / 3d;
                using var periodicTimer = new PeriodicTimer(delay);
                try
                {
                    while (await periodicTimer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                        try
                        {
                            logger.LogDebug("Updating visibility timeout for message {MessageId} with pop receipt {PopReceipt}", QueueMessage.MessageId, popReceipt);
                            var response = await queueClient.UpdateMessageAsync(
                                QueueMessage.MessageId,
                                popReceipt,
                                message: null!,
                                visibilityTimeout: visibilityTimeout,
                                cancellationToken: cancellationToken
                            ).ConfigureAwait(false);
                            if (!response.GetRawResponse().IsError)
                            {
                                popReceipt = response.Value.PopReceipt;
                                logger.LogDebug("Visibility timeout updated for message {MessageId} with new pop receipt {PopReceipt}", QueueMessage.MessageId, popReceipt);
                            }
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            logger.LogError(ex, "An exception has been caught while updating the message visibility timeout");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // NOP
                }
                return popReceipt;
            }

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        cancellationTokenSourceForVisibilityTimeoutUpdate.Cancel();
                        cancellationTokenSourceForVisibilityTimeoutUpdate.Dispose();
                    }

                    cancellationTokenSourceForVisibilityTimeoutUpdate = null!;
                    disposedValue = true;
                }
            }

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                Dispose(disposing: true);
                GC.SuppressFinalize(this);
            }
        }
    }

}
