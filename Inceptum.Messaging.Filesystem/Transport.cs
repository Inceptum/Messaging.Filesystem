using System;
using System.Collections.Generic;
using System.IO;
using Inceptum.Messaging.Contract;
using Inceptum.Messaging.Transports;

namespace Inceptum.Messaging.Filesystem
{
    internal class Transport : ITransport
    {
        private readonly TransportInfo m_TransportInfo;
        private Action m_OnFailure;
        private bool m_IsDisposed;
        private readonly object m_SyncRoot = new object();
        private readonly List<IProcessingGroup> m_ProcessingGroups = new List<IProcessingGroup>();
        private readonly string m_JailedTag;

        public Transport(TransportInfo transportInfo, Action onFailure)
        {
            m_TransportInfo = transportInfo;
            m_JailedTag = (transportInfo.JailStrategy ?? JailStrategy.None).CreateTag();
            m_OnFailure = onFailure;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            if (m_IsDisposed) return;

            lock (m_SyncRoot)
            {
                if (m_IsDisposed) return;
                m_OnFailure = () => { };

                var array = m_ProcessingGroups.ToArray();
                foreach (var g in array)
                {
                    g.Dispose();
                }

                m_IsDisposed = true;
            }
        }

        public IProcessingGroup CreateProcessingGroup(Action onFailure)
        {
            IProcessingGroup group;
            lock (m_SyncRoot)
            {
                group = new ProcessingGroup(m_JailedTag);
                m_ProcessingGroups.Add(group);
            }
            return group;
        }

        public bool VerifyDestination(Destination destination, EndpointUsage usage, bool configureIfRequired, out string error)
        {
            throw new NotImplementedException();
        }
    }
}