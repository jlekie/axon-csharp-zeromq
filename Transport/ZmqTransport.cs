using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using NetMQ;
using NetMQ.Sockets;

namespace Axon.ZeroMQ
{
    internal struct Message
    {
        public Dictionary<string, byte[]> Frames;
        public byte[] Payload;
        public byte[] Envelope;
        public int Signal;

        public Message(int signal, IDictionary<string ,byte[]> frames, byte[] payload)
        {
            this.Frames = new Dictionary<string, byte[]>(frames);
            this.Payload = payload;
            this.Envelope = null;
            this.Signal = signal;
        }
        public Message(int signal, IDictionary<string ,byte[]> frames, byte[] payload, byte[] envelope)
        {
            this.Frames = new Dictionary<string, byte[]>(frames);
            this.Payload = payload;
            this.Envelope = envelope;
            this.Signal = signal;
        }

        public bool TryPluckFrame(string key, out byte[] data)
        {
            if (this.Frames.ContainsKey(key))
            {
                data = this.Frames[key];
                this.Frames.Remove(key);

                return true;
            }
            else
            {
                data = null;
                return false;
            }
        }
    }
    internal struct TaggedMessage
    {
        public string Tag;
        public Message Message;

        public TaggedMessage(string tag, Message message)
        {
            this.Tag = tag;
            this.Message = message;
        }
    }

    public interface IZeroMQTransport : ITransport
    {
    }

    public interface IRouterServerTransport : IServerTransport
    {
        IZeroMQServerEndpoint Endpoint { get; }
    }

    public interface IDealerClientTransport : IClientTransport
    {
        IZeroMQClientEndpoint Endpoint { get; }
    }

    public class RouterServerTransport : AServerTransport, IRouterServerTransport
    {
        private readonly IZeroMQServerEndpoint endpoint;
        public IZeroMQServerEndpoint Endpoint
        {
            get
            {
                return this.endpoint;
            }
        }

        private readonly string identity;
        public string Identity
        {
            get
            {
                return identity;
            }
        }

        private readonly ConcurrentQueue<Message> ReceiveBuffer;
        private readonly ConcurrentDictionary<string, Message> TaggedReceiveBuffer;
        //private readonly ConcurrentQueue<Message> SendBuffer;

        private Task ListeningTask;

        //private RouterSocket Socket;
        private NetMQQueue<Message> AltSendBuffer;

        public RouterServerTransport(IZeroMQServerEndpoint endpoint)
        {
            this.endpoint = endpoint;

            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.ReceiveBuffer = new ConcurrentQueue<Message>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, Message>();
            //this.SendBuffer = new ConcurrentQueue<Message>();
        }

        public override Task Listen()
        {
            if (!this.IsRunning)
            {
                this.IsRunning = true;
                var connectionString = this.Endpoint.ToConnectionString();
                // this.ListeningTask = Task.Factory.StartNew(() => this.ServerHandler(), TaskCreationOptions.LongRunning);
                this.ListeningTask = Task.Factory.StartNew(() => this.ServerHandler(connectionString), TaskCreationOptions.LongRunning).Unwrap();
            }

            return Task.FromResult(false);
        }
        public override async Task Close()
        {
            this.IsRunning = false;

            await this.ListeningTask;
        }

        public override async Task Send(byte[] data, IDictionary<string, byte[]> metadata)
        {
            await this.EnsureListening();
            // Console.WriteLine(data.Length);

            var frames = new Dictionary<string, byte[]>();
            byte[] envelope = null;
            foreach (var key in metadata.Keys)
            {
                switch (key)
                {
                    case "envelope":
                        envelope = metadata[key];
                        break;
                    default:
                        frames.Add(key, metadata[key]);
                        break;
                }
            }

            // byte[] identityData;
            // if (!frames.TryGetValue("identity", out identityData))
            //     throw new Exception("Identity metadata missing");
            // var identity = System.Text.Encoding.ASCII.GetString(identityData);

            var message = new Message(0, frames, data, envelope);
            //this.SendBuffer.Enqueue(message);
            //this.Socket.SendMultipartMessage(message.ToNetMQMessage(true));
            this.AltSendBuffer.Enqueue(message);
            //Console.WriteLine("AltSendBuffer Size: " + this.AltSendBuffer.Count());
        }
        public override async Task Send(string messageId, byte[] data, IDictionary<string, byte[]> metadata)
        {
            await this.EnsureListening();

            var encodedRid = System.Text.Encoding.ASCII.GetBytes(messageId);
            var encodedIdentity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

            var frames = new Dictionary<string, byte[]>();
            foreach (var key in metadata.Keys)
                frames.Add(key, metadata[key]);
            frames.Add("rid", encodedRid);

            byte[] envelope = null;
            foreach (var key in metadata.Keys)
            {
                switch (key)
                {
                    case "envelope":
                        envelope = metadata[key];
                        break;
                    default:
                        frames.Add(key, metadata[key]);
                        break;
                }
            }

            var message = new Message(0, frames, data, envelope);

            this.AltSendBuffer.Enqueue(message);
            //Console.WriteLine("AltSendBuffer Size: " + this.AltSendBuffer.Count());
        }

        public override async Task<ReceivedData> Receive()
        {
            var message = await this.GetBufferedData();

            var payloadData = message.Payload;
            var metadata = message.Frames.ToDictionary(p => p.Key, p => p.Value);
            metadata.Add("envelope", message.Envelope);

            this.OnDataReceived(payloadData, metadata);

            return new ReceivedData(payloadData, metadata);
        }
        public override async Task<ReceivedData> Receive(string messageId)
        {
            var responseMessage = await this.GetBufferedTaggedData(messageId, 30000);

            if (responseMessage.Signal != 0)
            {
                var errorMessage = System.Text.Encoding.UTF8.GetString(responseMessage.Payload);
                throw new Exception($"Transport error ({responseMessage.Signal}): {errorMessage}");
            }

            var responsePayloadData = responseMessage.Payload;
            var responseMetadata = responseMessage.Frames.ToDictionary(p => p.Key, p => p.Value);

            this.OnDataReceived(responsePayloadData, responseMetadata);

            return new ReceivedData(responsePayloadData, responseMetadata);
        }

        public override async Task<ReceivedTaggedData> ReceiveTagged()
        {
            var taggedMessage = await this.GetBufferedTaggedData();
            var tag = taggedMessage.Tag;
            var message = taggedMessage.Message;

            var payloadData = message.Payload;
            var metadata = message.Frames.ToDictionary(p => p.Key, p => p.Value);
            metadata.Add("envelope", message.Envelope);

            this.OnDataReceived(payloadData, metadata);

            return new ReceivedTaggedData(tag, payloadData, metadata);
        }

        public override Task<Func<Task<ReceivedData>>> SendAndReceive(byte[] data, IDictionary<string, byte[]> metadata)
        {
            throw new NotImplementedException();
        }

        private async Task ServerHandler(string connectionString)
        {
            while (this.IsRunning)
            {
                try
                {
                    //var sendTimer = new NetMQTimer(TimeSpan.FromMilliseconds(10));

                    using (var socket = new RouterSocket())
                    using (this.AltSendBuffer = new NetMQQueue<Message>())
                    using (var poller = new NetMQPoller() { socket, this.AltSendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(socket, $"inproc://monitor.routerserver.{Guid.NewGuid().ToString()}", SocketEvents.Listening | SocketEvents.Closed))
                    {
                        socket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                var netmqMessage = new NetMQMessage();
                                if (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    var message = netmqMessage.ToMessage(true);

                                    byte[] encodedRid;
                                    if (message.TryPluckFrame("rid", out encodedRid))
                                    {
                                        var decodedRid = System.Text.Encoding.ASCII.GetString(encodedRid);

                                        this.TaggedReceiveBuffer.TryAdd(decodedRid, message);
                                        Console.WriteLine("TaggedReceiveBuffer Size: " + this.TaggedReceiveBuffer.Count);
                                    }
                                    else
                                    {
                                        this.ReceiveBuffer.Enqueue(message);
                                        Console.WriteLine("ReceiveBuffer Size: " + this.ReceiveBuffer.Count);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        ////var lastFlushed = DateTime.UtcNow;
                        //this.Socket.SendReady += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.SendBuffer.IsEmpty)
                        //        {
                        //            while (this.SendBuffer.TryDequeue(out Message message))
                        //            {
                        //                //Console.WriteLine("sending");

                        //                if (!e.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to send message");
                        //                }
                        //            }

                        //            //lastFlushed = DateTime.UtcNow;
                        //        }

                        //        //if ((DateTime.UtcNow - lastFlushed).TotalSeconds > 1)
                        //            Task.Delay(1000).Wait();
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                        //    }
                        //};
                        //sendTimer.Elapsed += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.SendBuffer.IsEmpty)
                        //        {
                        //            while (this.SendBuffer.TryDequeue(out Message message))
                        //            {
                        //                //Console.WriteLine("sending");

                        //                if (!this.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to send message");
                        //                }
                        //            }
                        //        }
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
                                    //Console.WriteLine("sending");

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

                        monitor.Listening += (sender, e) =>
                        {
                            Console.WriteLine($"Router socket listening at {connectionString}");
                            this.IsListening = true;
                        };
                        monitor.Closed += (sender, e) =>
                        {
                            Console.WriteLine($"Router socket closed on {connectionString}");
                            this.IsListening = false;
                        };

                        Console.WriteLine($"Attempting to bind socket to {endpoint.ToConnectionString()}");
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

                        socket.Bind(connectionString);

                        var start = DateTime.Now;
                        while (!this.IsListening)
                        {
                            if ((DateTime.Now - start).TotalMilliseconds > 60000)
                                throw new Exception("Socket bind timeout");

                            await Task.Delay(1000);
                        }

                        while (this.IsRunning && this.IsListening)
                        {
                            // Console.WriteLine("Heartbeat");

                            await Task.Delay(1000);
                        }

                        poller.StopAsync();
                        socket.Unbind(connectionString);
                        monitor.DetachFromPoller();
                        monitor.Stop();
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
                // Console.WriteLine("Getting Message");
                // MessageHelpers.WriteMessage(message);

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
                        // Console.WriteLine("Getting Message");
                        // MessageHelpers.WriteMessage(message);

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
        private async Task<TaggedMessage> GetBufferedTaggedData(int timeout = 0)
        {
            Message message;
            string tag;

            tag = this.TaggedReceiveBuffer.Keys.First();
            if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            {
                this.lastGetBufferedTaggedData = DateTime.UtcNow;
                // Console.WriteLine("Received Tagged Message");

                return new TaggedMessage(tag, message);
            }
            else
            {
                var start = DateTime.Now;

                while (this.IsRunning)
                {
                    tag = this.TaggedReceiveBuffer.Keys.First();
                    if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(tag, out message))
                    {
                        this.lastGetBufferedTaggedData = DateTime.UtcNow;
                        // Console.WriteLine("Received Tagged Message");

                        return new TaggedMessage(tag, message);
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
        private async Task<Message> GetBufferedTaggedData(string rid, int timeout = 0)
        {
            Message message;
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

        private async Task EnsureListening()
        {
            if (!this.IsListening)
            {
                await this.Listen();
            }
        }
    }

    public class DealerClientTransport : AClientTransport, IDealerClientTransport
    {
        private readonly IZeroMQClientEndpoint endpoint;
        public IZeroMQClientEndpoint Endpoint
        {
            get
            {
                return this.endpoint;
            }
        }

        private readonly IDiscoverer<IZeroMQClientEndpoint> discoverer;
        public IDiscoverer<IZeroMQClientEndpoint> Discoverer
        {
            get
            {
                return this.discoverer;
            }
        }

        private readonly string identity;
        public string Identity
        {
            get
            {
                return this.identity;
            }
        }

        private readonly int idleTimeout;
        public int IdleTimeout
        {
            get
            {
                return this.idleTimeout;
            }
        }

        private readonly ConcurrentQueue<Message> ReceiveBuffer;
        private readonly ConcurrentDictionary<string, Message> TaggedReceiveBuffer;
        //private readonly ConcurrentQueue<Message> SendBuffer;

        private Task ListeningTask;
        //private DealerSocket Socket { get; set; }
        private NetMQQueue<Message> AltSendBuffer;

        public DealerClientTransport(IZeroMQClientEndpoint endpoint, int idleTimeout = 0)
        {
            this.endpoint = endpoint;

            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            this.idleTimeout = idleTimeout;

            this.ReceiveBuffer = new ConcurrentQueue<Message>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, Message>();
            //this.SendBuffer = new ConcurrentQueue<Message>();
        }
        public DealerClientTransport(IDiscoverer<IZeroMQClientEndpoint> discoverer, int idleTimeout = 0)
        {
            this.discoverer = discoverer;

            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            this.idleTimeout = idleTimeout;

            this.ReceiveBuffer = new ConcurrentQueue<Message>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, Message>();
            //this.SendBuffer = new ConcurrentQueue<Message>();
        }

        public override async Task Connect(int timeout = 0)
        {
            if (!this.IsRunning)
            {
                this.IsRunning = true;
                //var connectionString = this.Endpoint.ToConnectionString();
                // this.ListeningTask = Task.Factory.StartNew(() => this.ServerHandler(), TaskCreationOptions.LongRunning);
                this.ListeningTask = Task.Factory.StartNew(() => this.ServerHandler(), TaskCreationOptions.LongRunning).Unwrap();
            }

            var startTime = DateTime.UtcNow;
            while (!this.IsConnected)
            {
                if (timeout > 0 && (DateTime.UtcNow - startTime).TotalMilliseconds > timeout)
                {
                    this.IsRunning = false;
                    await this.ListeningTask;

                    throw new Exception("Connection timeout");
                }

                await Task.Delay(500);
            }
        }
        public async override Task Close()
        {
            this.IsRunning = false;
            await this.ListeningTask;
        }

        public override async Task Send(byte[] data, IDictionary<string, byte[]> metadata)
        {
            await this.EnsureConnected();

            //Console.WriteLine(data.Length);

            var encodedIdentity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

            var frames = new Dictionary<string, byte[]>();
            foreach (var key in metadata.Keys)
                frames.Add(key, metadata[key]);
            // frames.Add("source_identity", encodedIdentity);

            var message = new Message(0, frames, data);
            //this.SendBuffer.Enqueue(message);
            //this.Socket.SendMultipartMessage(message.ToNetMQMessage(true));
            this.AltSendBuffer.Enqueue(message);
            //Console.WriteLine("AltSendBuffer Size: " + this.AltSendBuffer.Count());
        }
        public override async Task Send(string messageId, byte[] data, IDictionary<string, byte[]> metadata)
        {
            await this.EnsureConnected();

            var encodedRid = System.Text.Encoding.ASCII.GetBytes(messageId);
            var encodedIdentity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

            var frames = new Dictionary<string, byte[]>();
            foreach (var key in metadata.Keys)
                frames.Add(key, metadata[key]);
            frames.Add("rid", encodedRid);

            byte[] envelope = null;
            foreach (var key in metadata.Keys)
            {
                switch (key)
                {
                    case "envelope":
                        envelope = metadata[key];
                        break;
                    default:
                        frames.Add(key, metadata[key]);
                        break;
                }
            }

            var message = new Message(0, frames, data, envelope);

            this.AltSendBuffer.Enqueue(message);
            //Console.WriteLine("AltSendBuffer Size: " + this.AltSendBuffer.Count());
        }

        public override async Task<ReceivedData> Receive()
        {
            var message = await this.GetBufferedData(30000);

            if (message.Signal != 0)
            {
                var errorMessage = System.Text.Encoding.UTF8.GetString(message.Payload);
                throw new Exception($"Transport error ({message.Signal}): {errorMessage}");
            }

            var payloadData = message.Payload;
            var metadata = message.Frames.ToDictionary(p => p.Key, p => p.Value);

            this.OnDataReceived(payloadData, metadata);

            return new ReceivedData(payloadData, metadata);
        }
        public override async Task<ReceivedData> Receive(string messageId)
        {
            var responseMessage = await this.GetBufferedTaggedData(messageId, 30000);

            if (responseMessage.Signal != 0)
            {
                var errorMessage = System.Text.Encoding.UTF8.GetString(responseMessage.Payload);
                throw new Exception($"Transport error ({responseMessage.Signal}): {errorMessage}");
            }

            var responsePayloadData = responseMessage.Payload;
            var responseMetadata = responseMessage.Frames.ToDictionary(p => p.Key, p => p.Value);

            this.OnDataReceived(responsePayloadData, responseMetadata);

            return new ReceivedData(responsePayloadData, responseMetadata);
        }

        public override async Task<Func<Task<ReceivedData>>> SendAndReceive(byte[] data, IDictionary<string, byte[]> metadata)
        {
            await this.EnsureConnected();

            // Console.WriteLine(data.Length);

            var rid = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            var encodedRid = System.Text.Encoding.ASCII.GetBytes(rid);

            var encodedIdentity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

            var frames = new Dictionary<string, byte[]>();
            foreach (var key in metadata.Keys)
                frames.Add(key, metadata[key]);
            frames.Add("rid", encodedRid);

            var message = new Message(0, frames, data);
            //this.SendBuffer.Enqueue(message);
            //this.Socket.SendMultipartMessage(message.ToNetMQMessage(true));
            this.AltSendBuffer.Enqueue(message);

            return new Func<Task<ReceivedData>>(async () => {
                // var getBuffered = System.Diagnostics.Stopwatch.StartNew();
                var responseMessage = await this.GetBufferedTaggedData(rid, 30000);
                // getBuffered.Stop();
                // Console.WriteLine("GetBufferedTaggedData: " + getBuffered.ElapsedMilliseconds);

                if (responseMessage.Signal != 0)
                {
                    var errorMessage = System.Text.Encoding.UTF8.GetString(responseMessage.Payload);
                    throw new Exception($"Transport error ({responseMessage.Signal}): {errorMessage}");
                }

                var responsePayloadData = responseMessage.Payload;
                var responseMetadata = responseMessage.Frames.ToDictionary(p => p.Key, p => p.Value);

                this.OnDataReceived(responsePayloadData, responseMetadata);

                return new ReceivedData(responsePayloadData, responseMetadata);
            });
        }

        private async Task ServerHandler()
        {
            while (this.IsRunning)
            {
                try
                {
                    DateTime lastActivityTime;

                    this.IsConnected = false;

                    //var sendTimer = new NetMQTimer(TimeSpan.FromMilliseconds(10));

                    using (var socket = new DealerSocket())
                    using (this.AltSendBuffer = new NetMQQueue<Message>())
                    using (var poller = new NetMQPoller() { socket, this.AltSendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(socket, $"inproc://monitor.dealerclient.{Guid.NewGuid().ToString()}", SocketEvents.Connected | SocketEvents.Disconnected))
                    {
                        socket.Options.Identity = System.Text.Encoding.ASCII.GetBytes(this.Identity);

                        socket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                var netmqMessage = new NetMQMessage();
                                if (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    lastActivityTime = DateTime.UtcNow;

                                    var message = netmqMessage.ToMessage(false);

                                    if (message.Frames.ContainsKey("rid"))
                                    {
                                        var decodedRid = System.Text.Encoding.ASCII.GetString(message.Frames["rid"]);

                                        this.TaggedReceiveBuffer.TryAdd(decodedRid, message);
                                        Console.WriteLine("TaggedReceiveBuffer Size: " + this.TaggedReceiveBuffer.Count);
                                    }
                                    else
                                    {
                                        this.ReceiveBuffer.Enqueue(message);
                                        Console.WriteLine("ReceiveBuffer Size: " + this.ReceiveBuffer.Count);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        ////var lastFlushed = DateTime.UtcNow;
                        //this.Socket.SendReady += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.SendBuffer.IsEmpty)
                        //        {
                        //            while (this.SendBuffer.TryDequeue(out Message message))
                        //            {
                        //                //Console.WriteLine("sending");

                        //                if (!e.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to send message");
                        //                }
                        //            }

                        //            //lastFlushed = DateTime.UtcNow;
                        //        }

                        //        //if ((DateTime.UtcNow - lastFlushed).TotalSeconds > 1)
                        //            Task.Delay(1000).Wait();
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                        //    }
                        //};
                        //sendTimer.Elapsed += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.SendBuffer.IsEmpty)
                        //        {
                        //            while (this.SendBuffer.TryDequeue(out Message message))
                        //            {
                        //                //Console.WriteLine("sending");

                        //                if (!this.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to send message");
                        //                }
                        //            }
                        //        }
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
                                    lastActivityTime = DateTime.UtcNow;
                                    //Console.WriteLine("sending");

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
                            this.IsConnected = true;
                        };
                        monitor.Disconnected += (sender, e) =>
                        {
                            Console.WriteLine($"Dealer socket disconntected from {endpoint.ToConnectionString()}");
                            this.IsConnected = false;
                        };

                        Console.WriteLine($"Attempting to connect to {endpoint.ToConnectionString()}");
                        monitor.StartAsync();
                        monitor.AttachToPoller(poller);

                        var pollerTask = new Task(poller.Run);
                        pollerTask.ContinueWith((Task task) =>
                        {
                            var ex = task.Exception;

                            Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            this.IsConnected = false;
                        }, TaskContinuationOptions.OnlyOnFaulted);
                        pollerTask.Start();

                        socket.Connect(endpoint.ToConnectionString());

                        var start = DateTime.Now;
                        while (!this.IsConnected)
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

                        lastActivityTime = DateTime.UtcNow;
                        while (this.IsConnected && this.IsRunning)
                        {
                            //Console.WriteLine("Hearbeat");

                            await Task.Delay(1000);

                            if (this.IdleTimeout > 0 && (DateTime.UtcNow - lastActivityTime).TotalMilliseconds > this.IdleTimeout)
                                this.IsRunning = false;
                        }

                        Console.WriteLine("Closing dealer socket...");
                        poller.StopAsync();
                        socket.Disconnect(endpoint.ToConnectionString());
                        monitor.DetachFromPoller();
                        monitor.Stop();

                        this.IsConnected = false;
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
                // Console.WriteLine("Received Message");
                // MessageHelpers.WriteMessage(message);

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
                        // Console.WriteLine("Received Message");
                        // MessageHelpers.WriteMessage(message);

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
        private async Task<Message> GetBufferedTaggedData(int timeout = 0)
        {
            Message message;
            if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(this.TaggedReceiveBuffer.Keys.First(), out message))
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
                    if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(this.TaggedReceiveBuffer.Keys.First(), out message))
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
        private async Task<Message> GetBufferedTaggedData(string rid, int timeout = 0)
        {
            Message message;
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

        private async Task EnsureConnected()
        {
            if (!this.IsConnected)
            {
                await this.Connect();
            }
        }
    }

    internal static class MessageHelpers
    {
        public static Message ToMessage(this NetMQMessage netmqMessage, bool dealerMessage)
        {
            var frames = new Dictionary<string, byte[]>();
            byte[] payload = null;
            byte[] envelope = null;
            int signal = -1;

            int startingFrame = 0;

            if (dealerMessage)
            {
                envelope = netmqMessage[0].ToByteArray();

                startingFrame++;
            }

            signal = netmqMessage[startingFrame].ConvertToInt32();
            startingFrame++;

            var partBuffer = new List<NetMQFrame>();
            for (var a = startingFrame; a < netmqMessage.FrameCount; a++)
            {
                var frame = netmqMessage[a];

                if (frame.IsEmpty || a >= netmqMessage.FrameCount - 1)
                {
                    if (partBuffer.Count == 2)
                    {
                        var name = System.Text.Encoding.ASCII.GetString(partBuffer[0].ToByteArray());
                        var framePayload = partBuffer[1].ToByteArray();

                        frames.Add(name, framePayload);
                    }
                    else if (partBuffer.Count == 0)
                    {
                        payload = frame.ToByteArray();
                    }
                    else
                    {
                        for (var b = 0; a < netmqMessage.FrameCount; a++)
                            Console.WriteLine($"{b}: [ {BitConverter.ToString(netmqMessage[b].ToByteArray()).Replace("-", " ")} ]");

                        throw new Exception("Unexpected frame count " + partBuffer.Count.ToString());
                    }

                    partBuffer.Clear();
                }
                else
                {
                    partBuffer.Add(frame);
                }
            }

            if (payload == null)
                throw new Exception("Missing payload");

            return new Message(signal, frames, payload, envelope);
        }

        public static NetMQMessage ToNetMQMessage(this Message message, bool dealerMessage)
        {
            var netqmMessage = new NetMQMessage();

            if (message.Envelope != null)
                netqmMessage.Append(new NetMQFrame(message.Envelope));

            netqmMessage.Append(message.Signal);

            foreach (var frame in message.Frames)
            {
                netqmMessage.Append(new NetMQFrame(System.Text.Encoding.ASCII.GetBytes(frame.Key)));
                netqmMessage.Append(new NetMQFrame(frame.Value));
                netqmMessage.AppendEmptyFrame();
            }

            netqmMessage.Append(new NetMQFrame(message.Payload));

            return netqmMessage;
        }

        public static void WriteMessage(Message message)
        {
            if (message.Envelope != null)
                Console.WriteLine("  Envelope" + " [ " + BitConverter.ToString(message.Envelope).Replace("-", " ") + " ]");
            foreach (var frame in message.Frames)
                Console.WriteLine("  " + frame.Key + " [ " + BitConverter.ToString(frame.Value).Replace("-", " ") + " ]");
            Console.WriteLine("  Payload" + " [ " + BitConverter.ToString(message.Payload).Replace("-", " ") + " ]");
            Console.WriteLine("  Signal " + message.Signal);
        }
    }
}