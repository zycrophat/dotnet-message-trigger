using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using MessageTrigger;
using MessageTrigger.Common;
using MessageTrigger.Kafka;
using MessageTrigger.StorageQueue;
using Microsoft.Extensions.Logging;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (s, e) =>
{
    Console.WriteLine("Canceling...");
    cts.Cancel();
    e.Cancel = true;
};


using var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole().SetMinimumLevel(LogLevel.Debug));

var queueClient = new QueueClient("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;", "myqueue");
await queueClient.CreateIfNotExistsAsync(cancellationToken: cts.Token).ConfigureAwait(false);

var messageProcessor =
    new SampleMessageProcessor(
        loggerFactory.CreateLogger<SampleMessageProcessor>()
    );

var storageQueueMessageConsumer =
    new BufferedStorageQueueMessageConsumer(
        loggerFactory.CreateLogger<BufferedStorageQueueMessageConsumer>(),
        queueClient,
        messageProcessor,
        visibilityTimeout: TimeSpan.FromSeconds(20)
    );

try
{
    await storageQueueMessageConsumer.ConsumeAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // NOP
}


IMessageProcessor<IEnumerable<IKafkaMessage<string, object>>> bla = null!;
IMessageProcessor<IEnumerable<IKafkaMessage<string, string>>> blubb = bla;

