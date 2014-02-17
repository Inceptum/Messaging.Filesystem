using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Inceptum.Messaging.Transports;

namespace Inceptum.Messaging.Filesystem
{
    /// <summary>
    /// Filesystem transport when a folder is a queue and a file is a message
    /// </summary>
    public class FilesystemTransportFactory : ITransportFactory
    {
        public string Name { get { return "Filesystem"; } }

        public ITransport Create(TransportInfo transportInfo, Action onFailure)
        {
            return new Transport(transportInfo, onFailure);
        }
    }
}
