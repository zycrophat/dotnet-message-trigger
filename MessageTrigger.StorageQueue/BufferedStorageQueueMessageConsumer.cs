using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using MessageTrigger.Core.Consuming;
using MessageTrigger.Core.Processing;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;
using System.Threading.Channels;

namespace MessageTrigger.StorageQueue
{
    public partial class BufferedStorageQueueMessageConsumer : IMessageConsumer
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

        private async Task WriteMessagesToChannelAsync(
            ChannelWriter<QueueMessageInTransit> writer,
            CancellationToken cancellationToken
        )
        {
            QueueMessageInTransit?[] messagesInTransit =
                new QueueMessageInTransit[numberOfMessagesPerFetch];
            while (true)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    LogFetchingMessagesFromQueue(queueClient.Name);
                    var response = await queueClient.ReceiveMessagesAsync(
                        numberOfMessagesPerFetch,
                        visibilityTimeout,
                        cancellationToken
                    ).ConfigureAwait(false);

                    if (response.Value.Length != 0)
                    {
                        try
                        {
                            Array.Clear(messagesInTransit);
                            for (int i = 0; i < response.Value.Length; i++)
                            {
                                var message = response.Value[i];
                                LogReceivedMessage(message.MessageId, message.PopReceipt);
                                var messageInTransit = new QueueMessageInTransit(
                                    logger,
                                    queueClient,
                                    visibilityTimeout,
                                    message
                                );
                                messageInTransit.StartUpdateVisibilityTimeout(cancellationToken);
                                messagesInTransit[i] = messageInTransit;
                            }
                            for (int i = 0; i < response.Value.Length; i++)
                            {
                                await WriteMessageToChannel(writer, messagesInTransit[i]!, cancellationToken).ConfigureAwait(false);
                            }
                        }
                        catch (Exception)
                        {
                            foreach (var maybeMessageInTransit in messagesInTransit)
                            {
                                if (maybeMessageInTransit is QueueMessageInTransit messageInTransit)
                                {
                                    await messageInTransit.DisposeAsync().ConfigureAwait(false);
                                }
                            }
                            throw;
                        }
                    }
                    else
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    LogExceptionWhileWritingMessagesToChannel(ex);
                    throw;
                }
                              
            }
        }

        [LoggerMessage(
            LogLevel.Debug,
            Message = "Fetching messages from queue {queue}"
        )]
        private partial void LogFetchingMessagesFromQueue(string queue);

        [LoggerMessage(
            LogLevel.Error,
            Message = "An exception has been caught while writing messages to the channel."
        )]
        private partial void LogExceptionWhileWritingMessagesToChannel(Exception exception);


        private static async Task WriteMessageToChannel(ChannelWriter<QueueMessageInTransit> writer, QueueMessageInTransit messageInTransit, CancellationToken cancellationToken)
        {
            await writer.WriteAsync(messageInTransit, cancellationToken).ConfigureAwait(false);
        }

        [LoggerMessage(
            LogLevel.Debug,
            Message = "Received message {messageId} with pop receipt {popReceipt}"
        )]
        private partial void LogReceivedMessage(string messageId, string popReceipt);

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

        private async Task ProcessMessageAsync(QueueMessageInTransit queueMessageInTransit, CancellationToken cancellationToken)
        {
            await using (queueMessageInTransit.ConfigureAwait(false))
            {
                var queueMessage = queueMessageInTransit.QueueMessage;
                await storageQueueMessageProcessor.ProcessAsync(
                    queueMessage,
                    cancellationToken
                ).ConfigureAwait(false);
                var popReceipt = await queueMessageInTransit.StopUpdateVisibilityTimeoutAsync().ConfigureAwait(false);
                LogDeletingMessageFromQueue(queueMessage.MessageId, popReceipt);
                var response = await queueClient.DeleteMessageAsync(
                    queueMessage.MessageId,
                    popReceipt,
                    cancellationToken
                ).ConfigureAwait(false);
            }
        }

        [LoggerMessage(
            LogLevel.Debug,
            Message = "Deleting message {messageId} from the queue with pop receipt {popReceipt}"
        )]
        private partial void LogDeletingMessageFromQueue(string messageId, string popReceipt);

        private partial class QueueMessageInTransit : IAsyncDisposable
        {
            private readonly ILogger<BufferedStorageQueueMessageConsumer> logger;
            private readonly QueueClient queueClient;
            private readonly TimeSpan visibilityTimeout;
            private Task<string> updateVisibilityTimeoutTask = null!;
            private CancellationTokenSource cancellationTokenSourceForVisibilityTimeoutUpdate = null!;

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
                            LogUpdatingVisibilityTimeout(QueueMessage.MessageId, popReceipt);
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
                                LogVisibilityTimeoutUpdated(QueueMessage.MessageId, popReceipt);
                            }
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            LogExceptionWhileUpdatingVisibilityTimeout(ex);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // NOP
                }
                return popReceipt;
            }

            [LoggerMessage(
                LogLevel.Debug,
                Message = "Updating visibility timeout for message {messageId} with pop receipt {popReceipt}"
            )]
            private partial void LogUpdatingVisibilityTimeout(string messageId, string popReceipt);

            [LoggerMessage(
                LogLevel.Debug,
                Message = "Visibility timeout updated for message {messageId} with new pop receipt {popReceipt}"
            )]
            private partial void LogVisibilityTimeoutUpdated(string messageId, string popReceipt);

            [LoggerMessage(
                LogLevel.Error,
                Message = "An exception has been caught while updating the message visibility timeout"
            )]
            private partial void LogExceptionWhileUpdatingVisibilityTimeout(Exception exception);

            protected virtual async ValueTask DisposeAsyncCore()
            {
                if (cancellationTokenSourceForVisibilityTimeoutUpdate is not null)
                {
                    await cancellationTokenSourceForVisibilityTimeoutUpdate.CancelAsync().ConfigureAwait(false);
                    cancellationTokenSourceForVisibilityTimeoutUpdate.Dispose();
                }

                cancellationTokenSourceForVisibilityTimeoutUpdate = null!;
            }

            public async ValueTask DisposeAsync()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                await DisposeAsyncCore().ConfigureAwait(false);
                GC.SuppressFinalize(this);
            }
        }
    }

}
