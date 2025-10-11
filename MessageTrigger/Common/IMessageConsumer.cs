using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageTrigger.Common
{
    internal interface IMessageConsumer
    {
        public Task ConsumeAsync(CancellationToken cancellationToken = default);
    }
}
