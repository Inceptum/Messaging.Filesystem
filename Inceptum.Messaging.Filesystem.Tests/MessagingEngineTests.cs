using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;
using Inceptum.Messaging.Contract;
using NUnit.Framework;
using Rhino.Mocks;

namespace Inceptum.Messaging.Filesystem.Tests
{
    internal class TransportConstants
    {
        public const string QUEUE1 = @"D:\FilesystemMessagingTests";
        public const string QUEUE2 = @"D:\FilesystemMessagingTests2";
        public const string TRANSPORT_ID1 = "tr1";
        public const string TRANSPORT_ID2 = "tr2";
        public const string USERNAME = "user not used";
        public const string PASSWORD = "pass not used";
        public const string BROKER = "broker not used for now";
    }

    public class ObjectMother
    {
        public static ITransportResolver MockTransportResolver(string transportFactory)
        {
            var transports = new Dictionary<string, TransportInfo>
            {
                {TransportConstants.TRANSPORT_ID1, new TransportInfo(TransportConstants.BROKER, TransportConstants.USERNAME, TransportConstants.PASSWORD, "None", transportFactory)},
                {TransportConstants.TRANSPORT_ID2, new TransportInfo(TransportConstants.BROKER, TransportConstants.USERNAME, TransportConstants.PASSWORD, "MachineName", transportFactory)}
            };
            var resolver = new TransportResolver(transports);
            return resolver;
        }

        public static Endpoint Endpoint1 = new Endpoint(TransportConstants.TRANSPORT_ID1, TransportConstants.QUEUE1, serializationFormat: "fake");
        public static Endpoint Endpoint2 = new Endpoint(TransportConstants.TRANSPORT_ID2, TransportConstants.QUEUE2, serializationFormat: "fake");
        public static Endpoint Endpoint3 = new Endpoint(TransportConstants.TRANSPORT_ID1, TransportConstants.QUEUE1, serializationFormat: "fakeint", sharedDestination: true);
        public static Endpoint Endpoint4 = new Endpoint(TransportConstants.TRANSPORT_ID1, TransportConstants.QUEUE1, serializationFormat: "fake", sharedDestination: true);
    }

    [TestFixture, Ignore("Uses real files test environment")]
    public class MessagingEngineTests
    {
        [Test]
        public void Send()
        {
            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof (string), new FakeStringSerializer());
                engine.Send("hello world", ObjectMother.Endpoint1);
            }
        }

        [Test]
        public void Send_SharedDestination()
        {
            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
                engine.SerializationManager.RegisterSerializer("fakeint", typeof(int), new FakeIntSerializer());
                engine.Send("hello world", ObjectMother.Endpoint4);
                engine.Send(20, ObjectMother.Endpoint3);
            }
        }

        [Test]
        public void Send_JailedTag()
        {
            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
                engine.Send("hello world", ObjectMother.Endpoint2);
            }
        }

        [Test]
        public void Subscribe_ReadsInitial()
        {
            foreach (var file in Directory.GetFiles(TransportConstants.QUEUE1)) File.Delete(file);

            File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "initial 1" });
            File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "initial 2" });
            File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "initial 3" });

            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
                var receivedMessages = new List<object>();
                using (engine.Subscribe<string>(ObjectMother.Endpoint1, receivedMessages.Add))
                {
                    Thread.Sleep(1000);
                }

                foreach (var receivedMessage in receivedMessages)
                {
                    Console.WriteLine(receivedMessage);
                }

                Assert.AreEqual(3, receivedMessages.Count);
            }
        }

        [Test]
        public void Subscribe_Watcher()
        {
            foreach (var file in Directory.GetFiles(TransportConstants.QUEUE1)) File.Delete(file);

            File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "initial 1" });
            File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "initial 2" });

            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
                var receivedMessages = new List<string>();
                using (engine.Subscribe<string>(ObjectMother.Endpoint1, s => { Console.WriteLine(s); receivedMessages.Add(s);}))
                {
                    File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "hello 1" });
                    File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "hello 2" });
                    File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "hello 3" });
                    File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "hello 4" });
                    File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "hello 5" });
                    File.WriteAllLines(TransportConstants.QUEUE1 + "\\" + Guid.NewGuid() + ".String", new[] { "hello 6" });

                    Thread.Sleep(2000);
                }

                Assert.AreEqual(8, receivedMessages.Count);
            }
        }

        [Test]
        public void SendSubscribe_WithJailedTag()
        {
            foreach (var file in Directory.GetFiles(TransportConstants.QUEUE2 + "\\" + Environment.MachineName)) File.Delete(file);
            foreach (var file in Directory.GetFiles(TransportConstants.QUEUE1)) File.Delete(file);

            Send();
            Send_JailedTag();

            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
                var receivedMessages1 = new List<string>();
                var receivedMessages2 = new List<string>();

                var d1 = engine.Subscribe<string>(ObjectMother.Endpoint1, s => { Console.WriteLine(s); receivedMessages1.Add(s); });
                var d2 = engine.Subscribe<string>(ObjectMother.Endpoint2, s => { Console.WriteLine(s); receivedMessages2.Add(s); });

                using (new CompositeDisposable(d1, d2))
                {
                    Send();
                    Send_JailedTag();
                    Send_JailedTag();
                    Send_JailedTag();
                    Send_JailedTag();
                    Send_JailedTag();
                    Thread.Sleep(2000);
                }

                Assert.AreEqual(2, receivedMessages1.Count);
                Assert.AreEqual(6, receivedMessages2.Count);
            }
        }

        [Test]
        public void SubscribeTwoClients()
        {
            foreach (var file in Directory.GetFiles(TransportConstants.QUEUE1)) File.Delete(file);

            for (int i = 0; i < 10; i++)
            {
                Send();
            }

            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");

            IMessagingEngine engine1 = new MessagingEngine(resolver, new FilesystemTransportFactory());
            engine1.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
            var receivedMessages1 = new List<string>();

            IMessagingEngine engine2 = new MessagingEngine(resolver, new FilesystemTransportFactory());
            engine2.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
            var receivedMessages2 = new List<string>();

            var d1 = engine1.Subscribe<string>(ObjectMother.Endpoint1, s => { Console.WriteLine(s ?? "null"); receivedMessages1.Add(s); });
            var d2 = engine2.Subscribe<string>(ObjectMother.Endpoint1, s => { Console.WriteLine(s ?? "null"); receivedMessages2.Add(s); });

            for (int i = 0; i < 30; i++)
            {
                Send();
            }

            Thread.Sleep(1000);

            d1.Dispose();
            d2.Dispose();
            engine1.Dispose();
            engine2.Dispose();

            Assert.AreEqual(40, receivedMessages1.Count + receivedMessages2.Count);
        }

        [Test]
        public void Subscribe_SharedDestination()
        {
            foreach (var file in Directory.GetFiles(TransportConstants.QUEUE1)) File.Delete(file);

            Send();
            Send_SharedDestination();

            ITransportResolver resolver = ObjectMother.MockTransportResolver("Filesystem");
            using (IMessagingEngine engine = new MessagingEngine(resolver, new FilesystemTransportFactory()))
            {
                engine.SerializationManager.RegisterSerializer("fake", typeof(string), new FakeStringSerializer());
                engine.SerializationManager.RegisterSerializer("fakeint", typeof(int), new FakeIntSerializer());
                var receivedMessages1 = new List<string>();
                var receivedMessages3 = new List<int>();

                var d1 = engine.Subscribe<string>(ObjectMother.Endpoint4, s => { Console.WriteLine(s); receivedMessages1.Add(s); });
                var d3 = engine.Subscribe<Int32>(ObjectMother.Endpoint3, s => { Console.WriteLine(s); receivedMessages3.Add(s); });

                using (new CompositeDisposable(d1, d3))
                {
                    Send();
                    Send_SharedDestination();
                    Send();
                    Send_SharedDestination();
                    Send_SharedDestination();
                    Thread.Sleep(2000);
                }

                Assert.AreEqual(7, receivedMessages1.Count);
                Assert.AreEqual(4, receivedMessages3.Count);
            }
        }
    }
}
