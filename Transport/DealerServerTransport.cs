using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace Axon.ZeroMQ
{
    public interface IDealerServerTransport : IServerTransport
    {
        IZeroMQClientEndpoint Endpoint { get; }
        string Identity { get; }
    }

    public class DealerServerTransport : AServerTransport, IDealerServerTransport
    {
        private readonly IZeroMQClientEndpoint endpoint;
        public IZeroMQClientEndpoint Endpoint => endpoint;

        private readonly IDiscoverer<IZeroMQClientEndpoint> discoverer;
        public IDiscoverer<IZeroMQClientEndpoint> Discoverer => discoverer;

        //private readonly string identity;
        //public string Identity
        //{
        //    get
        //    {
        //        return identity;
        //    }
        //}

        private readonly int idleTimeout;
        public int IdleTimeout
        {
            get
            {
                return this.idleTimeout;
            }
        }

        private readonly ConcurrentQueue<TransportMessage> receiveBuffer;
        private ConcurrentQueue<TransportMessage> ReceiveBuffer => receiveBuffer;

        private readonly ConcurrentDictionary<string, TransportMessage> TaggedReceiveBuffer;

        //private readonly ConcurrentQueue<Message> sendBuffer;
        //private ConcurrentQueue<Message> SendBuffer => sendBuffer;

        //private readonly ConcurrentDictionary<string, Message> taggedReceiveBuffer;
        //private ConcurrentDictionary<string, Message> TaggedReceiveBuffer => taggedReceiveBuffer;

        //private readonly ConcurrentQueue<Message> sendBuffer;
        //private ConcurrentQueue<Message> SendBuffer => sendBuffer;

        private Task ListeningTask { get; set; }

        //private DealerSocket Socket { get; set; }
        private NetMQQueue<TransportMessage> AltSendBuffer;

        public DealerServerTransport(IZeroMQClientEndpoint endpoint, int idleTimeout = 0)
            : base()
        {
            this.endpoint = endpoint;

            //this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            this.idleTimeout = idleTimeout;

            this.receiveBuffer = new ConcurrentQueue<TransportMessage>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, TransportMessage>();
            //this.sendBuffer = new ConcurrentQueue<Message>();
            //this.taggedReceiveBuffer = new ConcurrentDictionary<string, Message>();
            //this.sendBuffer = new ConcurrentQueue<Message>();
        }
        public DealerServerTransport(IDiscoverer<IZeroMQClientEndpoint> discoverer, int idleTimeout = 0)
            : base()
        {
            this.discoverer = discoverer;

            //this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            this.idleTimeout = idleTimeout;

            this.receiveBuffer = new ConcurrentQueue<TransportMessage>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, TransportMessage>();
            //this.sendBuffer = new ConcurrentQueue<Message>();
            //this.taggedReceiveBuffer = new ConcurrentDictionary<string, Message>();
            //this.sendBuffer = new ConcurrentQueue<Message>();
        }

        public override Task Listen()
        {
            if (!this.IsRunning)
            {
                this.IsRunning = true;

                this.ListeningTask = Task.Factory.StartNew(() => this.Handler(), TaskCreationOptions.LongRunning).Unwrap();
            }

            return Task.FromResult(false);
        }

        public override async Task Close()
        {
            this.IsRunning = false;

            await this.ListeningTask;
        }

        public override async Task<TransportMessage> Receive()
        {
            var message = await this.GetBufferedData();

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(string messageId)
        {
            var message = await this.GetBufferedTaggedData(messageId, 30000);

            this.OnMessageReceived(message);

            return message;
        }

        public override async Task<TaggedTransportMessage> ReceiveTagged()
        {
            var taggedMessage = await this.GetBufferedTaggedData();

            this.OnMessageReceived(taggedMessage.Message);

            return taggedMessage;
        }

        public override async Task Send(TransportMessage message)
        {
            await this.EnsureListening();

            var forwardedMessage = TransportMessage.FromMessage(message);

            this.OnMessageSending(forwardedMessage);

            this.AltSendBuffer.Enqueue(forwardedMessage);
        }
        public override async Task Send(string messageId, TransportMessage message)
        {
            await this.EnsureListening();

            var forwardedMessage = TransportMessage.FromMessage(message);

            var encodedMessageId = System.Text.Encoding.ASCII.GetBytes(messageId);
            forwardedMessage.Metadata.Add($"rid[{this.Identity}]", encodedMessageId);

            this.OnMessageSending(forwardedMessage);

            this.AltSendBuffer.Enqueue(forwardedMessage);
        }

        public override Task<Func<Task<TransportMessage>>> SendAndReceive(TransportMessage message)
        {
            throw new NotImplementedException();
        }

        //public override Task<Func<Task<ReceivedData>>> SendAndReceive(byte[] data, IDictionary<string, byte[]> metadata)
        //{
        //    var rid = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
        //    var encodedRid = System.Text.Encoding.ASCII.GetBytes(rid);

        //    var encodedIdentity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

        //    var frames = new Dictionary<string, byte[]>();
        //    foreach (var key in metadata.Keys)
        //        frames.Add(key, metadata[key]);
        //    frames.Add("rid", encodedRid);

        //    var message = new Message(frames, data);
        //    this.SendBuffer.Enqueue(message);

        //    return Task.FromResult(new Func<Task<ReceivedData>>(async () => {
        //        var responseMessage = await this.GetBufferedTaggedData(rid);

        //        var responsePayloadData = responseMessage.Payload;
        //        var responseMetadata = responseMessage.Frames.ToDictionary(p => p.Key, p => p.Value);

        //        this.OnDataReceived(responsePayloadData, responseMetadata);

        //        return new ReceivedData(responsePayloadData, responseMetadata);
        //    }));
        //}

        private async Task Handler()
        {
            while (this.IsRunning)
            {
                try
                {
                    DateTime lastActivityTime;

                    this.IsListening = false;

                    using (var socket = new DealerSocket())
                    using (this.AltSendBuffer = new NetMQQueue<TransportMessage>())
                    using (var poller = new NetMQPoller() { socket, this.AltSendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(socket, $"inproc://monitor.dealerserver.{Guid.NewGuid().ToString()}", SocketEvents.Connected | SocketEvents.Disconnected))
                    {
                        socket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                //Console.WriteLine("receiving");

                                var netmqMessage = new NetMQMessage();
                                if (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    try
                                    {
                                        lastActivityTime = DateTime.UtcNow;

                                        var message = netmqMessage.ToMessage();

                                        this.OnMessageReceiving(message);

                                        byte[] encodedRid;
                                        if (message.Metadata.TryPluck($"rid[{this.Identity}]", out encodedRid))
                                        {
                                            var decodedRid = System.Text.Encoding.ASCII.GetString(encodedRid);

                                            this.TaggedReceiveBuffer.TryAdd(decodedRid, message);
                                        }
                                        else
                                        {
                                            this.ReceiveBuffer.Enqueue(message);
                                        }

                                        ////var message = netmqMessage.ToMessage(false);

                                        //////if (message.Frames.ContainsKey("rid"))
                                        //////{
                                        //////    var decodedRid = System.Text.Encoding.ASCII.GetString(message.Frames["rid"]);

                                        //////    Console.WriteLine("  Tagged message buffered " + decodedRid);

                                        //////    this.TaggedReceiveBuffer.TryAdd(decodedRid, message);
                                        //////}
                                        //////else
                                        //////{
                                        ////this.ReceiveBuffer.Enqueue(message);
                                        //////}
                                    }
                                    catch (Exception ex)
                                    {
                                        //var forwardedMessage = new Message(1, new Dictionary<string, byte[]>(), System.Text.Encoding.UTF8.GetBytes(ex.Message));
                                        //e.Socket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
                                        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        //var lastFlushed = DateTime.UtcNow;
                        //this.Socket.SendReady += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.SendBuffer.IsEmpty)
                        //        {
                        //            while (this.SendBuffer.TryDequeue(out Message message))
                        //            {
                        //                Console.WriteLine("sending");

                        //                if (!e.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to send message");
                        //                }
                        //            }

                        //            lastFlushed = DateTime.UtcNow;
                        //        }

                        //        if ((DateTime.UtcNow - lastFlushed).TotalSeconds > 1)
                        //            Task.Delay(10).Wait();
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                        //    }
                        //};
                        this.AltSendBuffer.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                while (this.AltSendBuffer.TryDequeue(out TransportMessage message, TimeSpan.Zero))
                                {
                                    lastActivityTime = DateTime.UtcNow;

                                    if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage()))
                                    {
                                        Console.WriteLine("Failed to send message");
                                    }

                                    this.OnMessageSent(message);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        var endpoint = await this.ResolveEndpoint(5000);

                        monitor.Connected += (sender, e) =>
                        {
                            Console.WriteLine($"Dealer socket conntected to {endpoint.ToConnectionString()}");
                            this.IsListening = true;
                        };
                        monitor.Disconnected += (sender, e) =>
                        {
                            Console.WriteLine($"Dealer socket disconntected from {endpoint.ToConnectionString()}");
                            this.IsListening = false;
                        };

                        Console.WriteLine($"Attempting to connect to {endpoint.ToConnectionString()}");
                        monitor.StartAsync();
                        monitor.AttachToPoller(poller);

                        var pollerTask = new Task(poller.Run);
                        pollerTask.ContinueWith((Task task) =>
                        {
                            var ex = task.Exception;

                            Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            this.IsListening = false;
                        }, TaskContinuationOptions.OnlyOnFaulted);
                        pollerTask.Start();

                        socket.Connect(endpoint.ToConnectionString());

                        var start = DateTime.Now;
                        while (!this.IsListening)
                        {
                            if ((DateTime.Now - start).TotalMilliseconds > 5000)
                            {
                                if (this.Discoverer != null)
                                {
                                    Console.WriteLine($"Blacklisting endpoint {endpoint.ToConnectionString()}");
                                    await this.Discoverer.Blacklist(endpoint);
                                }

                                throw new Exception("Connection timeout");
                            }

                            await Task.Delay(1000);
                        }

                        //System.Threading.Thread.Sleep(5000);
                        //Console.WriteLine($"SENDING GREETING {this.Socket.HasOut}");
                        //this.Socket.SendMultipartMessage(new Message(0, new Dictionary<string, byte[]>()
                        //{
                        //    { "Greeting", new byte[] { 1 } }
                        //}, new byte[] { 1 }).ToNetMQMessage(true));

                        lastActivityTime = DateTime.UtcNow;
                        while (this.IsListening && this.IsRunning)
                        {
                            // Console.WriteLine("Heartbeat");

                            //Console.WriteLine($"SENDING GREETING {this.Socket.HasOut}");
                            try
                            {
                                //this.AltSendBuffer.Enqueue(new Message(0, new Dictionary<string, byte[]>() { { "Greeting", new byte[] { 1 } } }, new byte[] { 1 }));
                                this.AltSendBuffer.Enqueue(new TransportMessage(
                                    new byte[] { 1 },
                                    new VolatileTransportMetadata(new List<VolatileTransportMetadataFrame>()
                                    {
                                        new VolatileTransportMetadataFrame("Greeting", new byte[] { 1 })
                                    })
                                ));
                                //if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), new Message(0, new Dictionary<string, byte[]>() { { "Greeting", new byte[] { 1 } } }, new byte[] { 1 }).ToNetMQMessage(true)))
                                //{
                                //    Console.WriteLine("Failed to register dealer server");
                                //}
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }

                            await Task.Delay(1000);

                            if (this.IdleTimeout > 0 && (DateTime.UtcNow - lastActivityTime).TotalMilliseconds > this.IdleTimeout)
                                this.IsRunning = false;
                        }

                        //Console.WriteLine("Cleaning up");
                        poller.StopAsync();
                        socket.Disconnect(endpoint.ToConnectionString());
                        monitor.DetachFromPoller();
                        monitor.Stop();

                        //Console.WriteLine("Ready to reinitialize");
                    }

                    if (this.IsRunning)
                        await Task.Delay(1000);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        private DateTime lastGetBufferedData = DateTime.UtcNow;
        private async Task<TransportMessage> GetBufferedData(int timeout = 0)
        {
            TransportMessage message;
            if (!this.ReceiveBuffer.IsEmpty && this.ReceiveBuffer.TryDequeue(out message))
            {
                this.lastGetBufferedData = DateTime.UtcNow;

                return message;
            }
            else
            {
                var start = DateTime.Now;

                while (this.IsRunning)
                {
                    if (!this.ReceiveBuffer.IsEmpty && this.ReceiveBuffer.TryDequeue(out message))
                    {
                        this.lastGetBufferedData = DateTime.UtcNow;

                        return message;
                    }
                    else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
                    {
                        throw new Exception("Message timeout");
                    }

                    if ((DateTime.UtcNow - this.lastGetBufferedData).TotalSeconds > 1)
                        await Task.Delay(1);
                }

                throw new Exception("Transport stopped");
            }
        }

        private DateTime lastGetBufferedTaggedData = DateTime.UtcNow;
        private async Task<TaggedTransportMessage> GetBufferedTaggedData(int timeout = 0)
        {
            TransportMessage message;
            string tag;

            tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            if (!string.IsNullOrEmpty(tag) && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            {
                this.lastGetBufferedTaggedData = DateTime.UtcNow;
                // Console.WriteLine("Received Tagged Message");

                return new TaggedTransportMessage(tag, message);
            }
            else
            {
                var start = DateTime.Now;

                while (this.IsRunning)
                {
                    tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
                    if (!string.IsNullOrEmpty(tag) && this.TaggedReceiveBuffer.TryRemove(tag, out message))
                    {
                        this.lastGetBufferedTaggedData = DateTime.UtcNow;
                        // Console.WriteLine("Received Tagged Message");

                        return new TaggedTransportMessage(tag, message);
                    }
                    else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
                    {
                        throw new Exception("Tagged message timeout");
                    }

                    if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
                        await Task.Delay(1);
                }

                throw new Exception("Transport stopped");
            }
        }
        private async Task<TransportMessage> GetBufferedTaggedData(string rid, int timeout = 0)
        {
            TransportMessage message;
            if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(rid, out message))
            {
                this.lastGetBufferedTaggedData = DateTime.UtcNow;
                // Console.WriteLine("Received Tagged Message");

                return message;
            }
            else
            {
                var start = DateTime.Now;

                while (this.IsRunning)
                {
                    if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(rid, out message))
                    {
                        this.lastGetBufferedTaggedData = DateTime.UtcNow;
                        // Console.WriteLine("Received Tagged Message");

                        return message;
                    }
                    else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
                    {
                        throw new Exception("Tagged message timeout");
                    }

                    if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
                        await Task.Delay(1);
                }

                throw new Exception("Transport stopped");
            }
        }

        //private DateTime lastGetBufferedTaggedData = DateTime.UtcNow;
        //private async Task<Message> GetBufferedTaggedData(string rid)
        //{
        //    Message message;
        //    if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(rid, out message))
        //    {
        //        this.lastGetBufferedTaggedData = DateTime.UtcNow;

        //        return message;
        //    }
        //    else
        //    {
        //        var start = DateTime.Now;

        //        while (true)
        //        {
        //            if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(rid, out message))
        //            {
        //                this.lastGetBufferedTaggedData = DateTime.UtcNow;

        //                return message;
        //            }
        //            else if ((DateTime.Now - start).TotalMilliseconds > 30000)
        //            {
        //                throw new Exception("Tagged message timeout");
        //            }

        //            if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 5)
        //                await Task.Delay(1);
        //        }
        //    }
        //}

        private async Task<IZeroMQClientEndpoint> ResolveEndpoint(int timeout = 0)
        {
            if (this.Discoverer != null)
            {
                var endpoint = await this.Discoverer.Discover(timeout);

                return endpoint;
            }
            else
            {
                return this.Endpoint;
            }
        }

        private async Task EnsureListening()
        {
            if (!this.IsListening)
            {
                await this.Listen();
            }
        }
    }
}
