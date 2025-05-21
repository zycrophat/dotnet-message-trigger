using Azure.Storage.Queues.Models;
using MessageTrigger.Common;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageTrigger
{
    internal class SampleMessageProcessor : IMessageProcessor<QueueMessage>
    {
        private readonly ILogger<SampleMessageProcessor> logger;

        public SampleMessageProcessor(
            ILogger<SampleMessageProcessor> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);
            this.logger = logger;
        }

        public async Task ProcessAsync(QueueMessage message, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("Starting to process message: {MessageId}", message.MessageId);
            await Task.Delay(TimeSpan.FromSeconds(60), cancellationToken); // Simulate some processing delay
            logger.LogInformation("Finished processing message: {MessageId}", message.MessageId);
        }
    }
}
