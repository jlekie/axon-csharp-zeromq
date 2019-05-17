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

        private readonly string identity;
        public string Identity
        {
            get
            {
                return identity;
            }
        }

        private readonly ConcurrentQueue<Message> receiveBuffer;
        private ConcurrentQueue<Message> ReceiveBuffer => receiveBuffer;

        //private readonly ConcurrentQueue<Message> sendBuffer;
        //private ConcurrentQueue<Message> SendBuffer => sendBuffer;

        //private readonly ConcurrentDictionary<string, Message> taggedReceiveBuffer;
        //private ConcurrentDictionary<string, Message> TaggedReceiveBuffer => taggedReceiveBuffer;

        //private readonly ConcurrentQueue<Message> sendBuffer;
        //private ConcurrentQueue<Message> SendBuffer => sendBuffer;

        private Task ListeningTask { get; set; }

        //private DealerSocket Socket { get; set; }
        private NetMQQueue<Message> AltSendBuffer;

        public DealerServerTransport(IZeroMQClientEndpoint endpoint)
            : base()
        {
            this.endpoint = endpoint;

            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.receiveBuffer = new ConcurrentQueue<Message>();
            //this.sendBuffer = new ConcurrentQueue<Message>();
            //this.taggedReceiveBuffer = new ConcurrentDictionary<string, Message>();
            //this.sendBuffer = new ConcurrentQueue<Message>();
        }
        public DealerServerTransport(IDiscoverer<IZeroMQClientEndpoint> discoverer)
            : base()
        {
            this.discoverer = discoverer;

            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.receiveBuffer = new ConcurrentQueue<Message>();
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

        public override async Task<ReceivedData> Receive()
        {
            var message = await this.GetBufferedData();

            var payloadData = message.Payload;
            var metadata = message.Frames.ToDictionary(p => p.Key, p => p.Value);

            this.OnDataReceived(payloadData, metadata);

            return new ReceivedData(payloadData, metadata);
        }

        public override async Task Send(byte[] data, IDictionary<string, byte[]> metadata)
        {
            await this.EnsureListening();

            Console.WriteLine("sending");
            var encodedIdentity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

            var frames = new Dictionary<string, byte[]>();
            foreach (var key in metadata.Keys)
                frames.Add(key, metadata[key]);

            var message = new Message(0, frames, data);
            //this.SendBuffer.Enqueue(message);
            //this.Socket.SendMultipartMessage(message.ToNetMQMessage(true));
            this.AltSendBuffer.Enqueue(message);
        }

        public override Task<Func<Task<ReceivedData>>> SendAndReceive(byte[] data, IDictionary<string, byte[]> metadata)
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
                    this.IsListening = false;

                    using (var socket = new DealerSocket())
                    using (this.AltSendBuffer = new NetMQQueue<Message>())
                    using (var poller = new NetMQPoller() { socket, this.AltSendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(socket, $"inproc://monitor.dealerserver.{Guid.NewGuid().ToString()}", SocketEvents.Connected | SocketEvents.Disconnected))
                    {
                        socket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                Console.WriteLine("receiving");

                                var netmqMessage = new NetMQMessage();
                                if (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    try
                                    {
                                        var message = netmqMessage.ToMessage(false);

                                        //if (message.Frames.ContainsKey("rid"))
                                        //{
                                        //    var decodedRid = System.Text.Encoding.ASCII.GetString(message.Frames["rid"]);

                                        //    Console.WriteLine("  Tagged message buffered " + decodedRid);

                                        //    this.TaggedReceiveBuffer.TryAdd(decodedRid, message);
                                        //}
                                        //else
                                        //{
                                        this.ReceiveBuffer.Enqueue(message);
                                        //}
                                    }
                                    catch (Exception ex)
                                    {
                                        var forwardedMessage = new Message(1, new Dictionary<string, byte[]>(), System.Text.Encoding.UTF8.GetBytes(ex.Message));
                                        e.Socket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
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
                                while (this.AltSendBuffer.TryDequeue(out Message message, TimeSpan.Zero))
                                {
                                    //Console.WriteLine("sending out");

                                    if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                                    {
                                        Console.WriteLine("Failed to send message");
                                    }
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

                        while (this.IsListening && this.IsRunning)
                        {
                            // Console.WriteLine("Heartbeat");

                            //Console.WriteLine($"SENDING GREETING {this.Socket.HasOut}");
                            try
                            {
                                if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), new Message(0, new Dictionary<string, byte[]>() { { "Greeting", new byte[] { 1 } } }, new byte[] { 1 }).ToNetMQMessage(true)))
                                {
                                    Console.WriteLine("Failed to register dealer server");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }

                            await Task.Delay(1000);
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
        private async Task<Message> GetBufferedData(int timeout = 0)
        {
            Message message;
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
