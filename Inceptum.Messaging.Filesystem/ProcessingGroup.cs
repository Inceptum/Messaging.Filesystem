using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Castle.Core.Logging;
using Inceptum.Messaging.Contract;
using Inceptum.Messaging.Transports;

namespace Inceptum.Messaging.Filesystem
{
    internal class ProcessingGroup : IProcessingGroup
    {
        private readonly string m_JailedTag;
        private readonly List<FileSystemWatcher> m_Watchers = new List<FileSystemWatcher>();

        internal ProcessingGroup(string jailedTag)
        {
            m_JailedTag = jailedTag;
        }

        public void Send(string destination, BinaryMessage message, int ttl)
        {
            send(destination, message, ttl);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IDisposable Subscribe(string destination, Action<BinaryMessage, Action<bool>> callback, string messageType)
        {
            return subscribe(destination, handler(callback), messageType);
        }

        private static FileSystemEventHandler handler(Action<BinaryMessage, Action<bool>> callback)
        {
            return (sender, args) =>
            {
                var message = read(args.FullPath);
                if (message != null) callback(message, b => { });
            };
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public RequestHandle SendRequest(string destination, BinaryMessage message, Action<BinaryMessage> callback)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IDisposable RegisterHandler(string destination, Func<BinaryMessage, BinaryMessage> handler, string messageType)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Dispose()
        {
            lock (m_Watchers)
            {
                var watchers = m_Watchers.ToArray();
                foreach (FileSystemWatcher watcher in watchers)
                {
                    watcher.Dispose();
                }
                m_Watchers.Clear();
            }
        }

        private void send(string destination, BinaryMessage message, int ttl)
        {
            if (!String.IsNullOrWhiteSpace(m_JailedTag))
            {
                destination = Path.Combine(destination, m_JailedTag);
            }

            string file = String.Format("{0}.{1}", Guid.NewGuid(), message.Type);
            string fullPath = Path.Combine(destination, file);
            string fullPathFolder = Path.GetDirectoryName(fullPath) ?? "";
            if (!Directory.Exists(fullPathFolder)) Directory.CreateDirectory(fullPathFolder);

            File.WriteAllBytes(fullPath, message.Bytes ?? new byte[0]);
        }

        private static BinaryMessage read(string path)
        {
            FileStream file = null;
            try
            {
                //TODO: WTF!!
                Thread.Sleep(200);
                file = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Delete);
                var memoryStream = new MemoryStream();
                file.CopyTo(memoryStream);
                return new BinaryMessage {Bytes = memoryStream.ToArray(), Type = Path.GetExtension(path)};
            }
            catch (FileNotFoundException)
            {
                return null;
            }
            catch (IOException)
            {
                return null;
            }
            finally
            {
                if (file != null)
                {
                    File.Delete(path);
                    file.Close();
                }
                
            }
        }

        private void initialSubscribeRead(string destination, FileSystemEventHandler eventHandler, string messageType)
        {
            foreach (var file in Directory.GetFiles(destination, "*." + (String.IsNullOrWhiteSpace(messageType) ? "*" : messageType)))
            {
                string dirName = Path.GetDirectoryName(file);
                string fileName = Path.GetFileName(file);
                Scheduler.Default.Schedule(_ => eventHandler(this, new FileSystemEventArgs(WatcherChangeTypes.Created, dirName ?? "", fileName)));
            }
        }

        private IDisposable subscribe(string destination, FileSystemEventHandler eventHandler, string messageType)
        {
            lock (m_Watchers)
            {
                if (!String.IsNullOrWhiteSpace(m_JailedTag))
                {
                    destination = Path.Combine(destination, m_JailedTag);
                    if (!Directory.Exists(destination)) Directory.CreateDirectory(destination);
                }

                var watcher = new FileSystemWatcher();
                watcher.NotifyFilter = NotifyFilters.Attributes | NotifyFilters.CreationTime | NotifyFilters.DirectoryName | NotifyFilters.FileName | NotifyFilters.LastAccess |
                                         NotifyFilters.LastWrite | NotifyFilters.Security | NotifyFilters.Size;

                watcher.Path = destination;
                watcher.IncludeSubdirectories = false;
                watcher.Filter = "*." + (String.IsNullOrWhiteSpace(messageType) ? "*" : messageType);
                watcher.EnableRaisingEvents = true;
                watcher.Created += eventHandler;
                watcher.Changed += eventHandler;
                watcher.Error += OnError;
                m_Watchers.Add(watcher);

                initialSubscribeRead(destination, eventHandler, messageType);

                return watcher;
            }
        }

        public virtual Destination CreateTemporaryDestination()
        {
            throw new NotImplementedException("Temporaey folders not implemented");
        }

        private void OnError(object sender, ErrorEventArgs e)
        {
        }
    }
}