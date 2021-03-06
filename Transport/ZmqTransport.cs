using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Text;

using NetMQ;
using NetMQ.Sockets;

namespace Axon.ZeroMQ
{
    //public class ZmqTransportMessage : ATransportMessage
    //{
    //    public static ZmqTransportMessage FromNmqMessage(NetMQMessage nmqm)
    //    {
    //        var signal = nmqm[0].ConvertToInt32();
    //        var payload = nmqm.Last.ToByteArray();

    //        var metadata = new List<TransportMessageMetadata>();
    //        for (var a = 1; a < nmqm.FrameCount - 1; a += 3)
    //        {
    //            var key = Encoding.ASCII.GetString(nmqm[a].ToByteArray());
    //            var data = nmqm[a + 1].ToByteArray();

    //            metadata.Add(new TransportMessageMetadata(key, data));
    //        }

    //        return new ZmqTransportMessage(signal, payload, metadata.ToArray());
    //    }
    //    public static ZmqTransportMessage FromNmqDealerMessage(NetMQMessage nmqm)
    //    {
    //        var envelope = nmqm[0].ToByteArray();
    //        var signal = nmqm[1].ConvertToInt32();
    //        var payload = nmqm.Last.ToByteArray();

    //        var metadata = new List<TransportMessageMetadata>();
    //        for (var a = 2; a < nmqm.FrameCount - 1; a += 3)
    //        {
    //            var key = Encoding.ASCII.GetString(nmqm[a].ToByteArray());
    //            var data = nmqm[a + 1].ToByteArray();

    //            metadata.Add(new TransportMessageMetadata(key, data));
    //        }

    //        return new ZmqTransportMessage(signal, payload, metadata.ToArray());
    //    }

    //    public readonly int Signal;

    //    public ZmqTransportMessage(int signal, byte[] payload, ITransportMessageMetadata[] metadata)
    //        : base(payload, metadata)
    //    {
    //        this.Signal = signal;
    //    }

    //    public NetMQMessage ToNetMQMessage()
    //    {
    //        var nmqm = new NetMQMessage();

    //        nmqm.Append(this.Signal);

    //        foreach (var metadata in this.Metadata)
    //        {
    //            nmqm.Append(new NetMQFrame(Encoding.ASCII.GetBytes(metadata.Id)));
    //            nmqm.Append(new NetMQFrame(metadata.Data));
    //            nmqm.AppendEmptyFrame();
    //        }

    //        nmqm.Append(new NetMQFrame(this.Payload));

    //        return nmqm;
    //    }
    //    public NetMQMessage ToNetMQDealerMessage(byte[] envelope)
    //    {
    //        var nmqm = new NetMQMessage();

    //        nmqm.Append(new NetMQFrame(envelope));
    //        nmqm.Append(this.Signal);

    //        foreach (var metadata in this.Metadata)
    //        {
    //            nmqm.Append(new NetMQFrame(Encoding.ASCII.GetBytes(metadata.Id)));
    //            nmqm.Append(new NetMQFrame(metadata.Data));
    //            nmqm.AppendEmptyFrame();
    //        }

    //        nmqm.Append(new NetMQFrame(this.Payload));

    //        return nmqm;
    //    }
    //}
    //public class ZmqDealerTransportMessage : ATransportMessage
    //{
    //    public static ZmqDealerTransportMessage FromNmqMessage(NetMQMessage nmqm)
    //    {
    //        var envelope = nmqm[0].ToByteArray();
    //        var signal = nmqm[1].ConvertToInt32();
    //        var payload = nmqm.Last.ToByteArray();

    //        var metadata = new List<ZmqTransportMessageMetadata>();
    //        for (var a = 2; a < nmqm.FrameCount - 1; a += 3)
    //        {
    //            var key = Encoding.ASCII.GetString(nmqm[a].ToByteArray());
    //            var data = nmqm[a + 1].ToByteArray();

    //            metadata.Add(new ZmqTransportMessageMetadata(key, data));
    //        }

    //        return new ZmqDealerTransportMessage(signal, envelope, payload, metadata.ToArray());
    //    }
    //    //public static ZmqDealerTransportMessage FromZmqTransportMessage(ZmqTransportMessage sourceMessage)
    //    //{
    //    //    var forwardedMetadata = new List<ITransportMessageMetadata>();

    //    //    var envelopeMetadata = sourceMessage.GetMetadata($"envelope[{this.}]")

    //    //    return new ZmqDealerTransportMessage(sourceMessage.Signal, )
    //    //}

    //    public readonly int Signal;
    //    public readonly byte[] Envelope;

    //    public ZmqDealerTransportMessage(int signal, byte[] envelope, byte[] payload, ITransportMessageMetadata[] metadata)
    //        : base(payload, metadata)
    //    {
    //        this.Signal = signal;
    //        this.Envelope = envelope;
    //    }

    //    public NetMQMessage ToNetMQMessage()
    //    {
    //        var nmqm = new NetMQMessage();

    //        nmqm.Append(new NetMQFrame(this.Envelope));
    //        nmqm.Append(this.Signal);

    //        foreach (var metadata in this.Metadata)
    //        {
    //            nmqm.Append(new NetMQFrame(Encoding.ASCII.GetBytes(metadata.Id)));
    //            nmqm.Append(new NetMQFrame(metadata.Data));
    //            nmqm.AppendEmptyFrame();
    //        }

    //        nmqm.Append(new NetMQFrame(this.Payload));

    //        return nmqm;
    //    }
    //}

    //internal struct Message
    //{
    //    public Dictionary<string, byte[]> Frames;
    //    public byte[] Payload;
    //    public byte[] Envelope;
    //    public int Signal;

    //    public Message(int signal, IDictionary<string ,byte[]> frames, byte[] payload)
    //    {
    //        this.Frames = new Dictionary<string, byte[]>(frames);
    //        this.Payload = payload;
    //        this.Envelope = null;
    //        this.Signal = signal;
    //    }
    //    public Message(int signal, IDictionary<string ,byte[]> frames, byte[] payload, byte[] envelope)
    //    {
    //        this.Frames = new Dictionary<string, byte[]>(frames);
    //        this.Payload = payload;
    //        this.Envelope = envelope;
    //        this.Signal = signal;
    //    }

    //    public bool TryPluckFrame(string key, out byte[] data)
    //    {
    //        if (this.Frames.ContainsKey(key))
    //        {
    //            data = this.Frames[key];
    //            this.Frames.Remove(key);

    //            return true;
    //        }
    //        else
    //        {
    //            data = null;
    //            return false;
    //        }
    //    }
    //}
    //internal struct TaggedMessage
    //{
    //    public string Tag;
    //    public Message Message;

    //    public TaggedMessage(string tag, Message message)
    //    {
    //        this.Tag = tag;
    //        this.Message = message;
    //    }
    //}

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

    public class ConnectionTimeoutException : Exception
    {
        public ConnectionTimeoutException()
        {
        }
        public ConnectionTimeoutException(string message)
            : base(message)
        {
        }
    }

    public class HandlerErrorEventArgs : EventArgs
    {
        public Exception Exception { get; private set; }

        public HandlerErrorEventArgs(Exception exception)
            : base()
        {
            this.Exception = exception;
        }
    }

    public class RouterServerTransport : AServerTransport, IRouterServerTransport
    {
        public event EventHandler<HandlerErrorEventArgs> HandlerError;
        public event EventHandler Listening;
        public event EventHandler Closed;

        private bool isListening = false;
        public override bool IsListening
        {
            get => this.isListening;
        }

        private readonly IZeroMQServerEndpoint endpoint;
        public IZeroMQServerEndpoint Endpoint
        {
            get
            {
                return this.endpoint;
            }
        }

        //private readonly string identity;
        //public string Identity
        //{
        //    get
        //    {
        //        return identity;
        //    }
        //}

        private readonly BlockingCollection<TransportMessage> ReceiveBuffer;
        private readonly ConcurrentDictionary<string, BlockingCollection<TransportMessage>> TaggedReceiveBuffer;
        //private readonly ConcurrentQueue<Message> SendBuffer;

        private Task ListeningTask;

        //private RouterSocket Socket;
        private NetMQQueue<TransportMessage> AltSendBuffer;

        public RouterServerTransport(IZeroMQServerEndpoint endpoint)
            : base()
        {
            this.endpoint = endpoint;

            //this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.ReceiveBuffer = new BlockingCollection<TransportMessage>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, BlockingCollection<TransportMessage>>();
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
            var cancellationSource = new CancellationTokenSource(30000);

            await this.Close(cancellationSource.Token);
        }
        public override async Task Close(CancellationToken cancellationToken)
        {
            this.IsRunning = false;

            await this.ListeningTask;
        }

        public override async Task Send(TransportMessage message)
        {
            this.OnDiagnosticMessage($"[{this.Identity}] ATTEMPTING SEND MESSAGE");

            await this.EnsureListening();

            this.OnDiagnosticMessage($"[{this.Identity}] FORMATTING SEND MESSAGE");

            var forwardedMessage = TransportMessage.FromMessage(message);

            this.OnDiagnosticMessage($"[{this.Identity}] NOTIFYING SEND MESSAGE");

            try
            {
                this.OnDiagnosticMessage($"[{this.Identity}] NOTIFYING SEND MESSAGE 1");
                this.OnMessageSending(forwardedMessage);
                this.OnDiagnosticMessage($"[{this.Identity}] NOTIFYING SEND MESSAGE 2");
            }
            catch
            {
                this.OnDiagnosticMessage("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                //this.OnDiagnosticMessage(ex.Message + ": " + ex.StackTrace);
            }

            this.OnDiagnosticMessage($"[{this.Identity}] ENQUEUING SEND MESSAGE");

            this.AltSendBuffer.Enqueue(forwardedMessage);
        }
        public override Task Send(TransportMessage message, CancellationToken cancellationToken)
        {
            return this.Send(message);
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
        public override Task Send(string messageId, TransportMessage message, CancellationToken cancellationToken)
        {
            return this.Send(messageId, message);
        }

        public override async Task<TransportMessage> Receive()
        {
            var message = this.GetBufferedData();

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(CancellationToken cancellationToken)
        {
            var message = this.GetBufferedData(cancellationToken);

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(string messageId)
        {
            var message = await this.GetBufferedTaggedData(messageId);

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(string messageId, CancellationToken cancellationToken)
        {
            var message = await this.GetBufferedTaggedData(messageId, cancellationToken);

            this.OnMessageReceived(message);

            return message;
        }

        public override async Task<TaggedTransportMessage> ReceiveTagged()
        {
            var taggedMessage = await this.GetBufferedTaggedData();

            this.OnMessageReceived(taggedMessage.Message);

            return taggedMessage;
        }
        public override async Task<TaggedTransportMessage> ReceiveTagged(CancellationToken cancellationToken)
        {
            var taggedMessage = await this.GetBufferedTaggedData(cancellationToken);

            this.OnMessageReceived(taggedMessage.Message);

            return taggedMessage;
        }

        public override Task<Func<Task<TransportMessage>>> SendAndReceive(TransportMessage message)
        {
            throw new NotImplementedException();
        }
        public override Task<Func<Task<TransportMessage>>> SendAndReceive(TransportMessage message, CancellationToken cancellationToken)
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
                    using (this.AltSendBuffer = new NetMQQueue<TransportMessage>())
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
                                    var message = netmqMessage.ToMessage(out var envelope);

                                    message.Metadata.Add($"envelope[{this.Identity}]", envelope);

                                    this.OnMessageReceiving(message);

                                    byte[] encodedRid;
                                    if (message.Metadata.TryPluck($"rid[{this.Identity}]", out encodedRid))
                                    {
                                        var decodedRid = Encoding.ASCII.GetString(encodedRid);

                                        this.TaggedReceiveBuffer.AddOrUpdate(decodedRid, (key) => new BlockingCollection<TransportMessage>(), (key, receiveBuffer) =>
                                        {
                                            receiveBuffer.Add(message);
                                            return receiveBuffer;
                                        });
                                    }
                                    else
                                    {
                                        this.ReceiveBuffer.Add(message);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.OnHandlerError(ex);
                                //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
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
                                while (this.AltSendBuffer.TryDequeue(out var message, TimeSpan.Zero))
                                {
                                    this.OnDiagnosticMessage($"[{this.Identity}] SENDING");
                                    if (!message.Metadata.TryPluckLast($"envelope[{this.Identity}]", out var envelope))
                                        throw new Exception("Message envelope not found");

                                    if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(envelope)))
                                    {
                                        Console.WriteLine("Failed to send message");
                                    }

                                    this.OnDiagnosticMessage($"[{this.Identity}] SENT MESSAGE");

                                    this.OnMessageSent(message);
                                }
                            }
                            catch (Exception ex)
                            {
                                this.OnHandlerError(ex);
                                //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        monitor.Listening += (sender, e) =>
                        {
                            Console.WriteLine($"Router socket listening at {connectionString}");
                            this.isListening = true;

                            this.OnListening();
                        };
                        monitor.Closed += (sender, e) =>
                        {
                            Console.WriteLine($"Router socket closed on {connectionString}");
                            this.isListening = false;

                            this.OnClosed();
                        };

                        Console.WriteLine($"Attempting to bind socket to {endpoint.ToConnectionString()}");
                        var monitorTask = monitor.StartAsync();
                        monitor.AttachToPoller(poller);

                        var pollerTask = new Task(poller.Run);
                        var pollerHandlerTask = pollerTask.ContinueWith((Task task) =>
                        {
                            var ex = task.Exception;

                            this.OnHandlerError(ex);
                            //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            this.isListening = false;
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

                        this.OnClosed();
                    }

                    if (this.IsRunning)
                        await Task.Delay(1000);
                }
                catch (Exception ex)
                {
                    this.OnHandlerError(ex);
                    Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        private DateTime lastGetBufferedData = DateTime.UtcNow;
        private TransportMessage GetBufferedData()
        {
            return this.ReceiveBuffer.Take();
        }
        private TransportMessage GetBufferedData(CancellationToken cancellationToken)
        {
            return this.ReceiveBuffer.Take(cancellationToken);
        }

        private DateTime lastGetBufferedTaggedData = DateTime.UtcNow;
        private async Task<TaggedTransportMessage> GetBufferedTaggedData()
        {
            throw new NotImplementedException();

            //TransportMessage message;
            //string tag;

            //tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //if (!string.IsNullOrEmpty(tag) && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //{
            //    this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //    // Console.WriteLine("Received Tagged Message");

            //    return new TaggedTransportMessage(tag, message);
            //}
            //else
            //{
            //    var start = DateTime.Now;

            //    while (this.IsRunning)
            //    {
            //        tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //        if (!string.IsNullOrEmpty(tag) && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //        {
            //            this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //            // Console.WriteLine("Received Tagged Message");

            //            return new TaggedTransportMessage(tag, message);
            //        }
            //        else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
            //        {
            //            throw new Exception("Tagged message timeout");
            //        }

            //        if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
            //            await Task.Delay(1);
            //    }

            //    throw new Exception("Transport stopped");
            //}
        }
        private async Task<TaggedTransportMessage> GetBufferedTaggedData(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();

            //TransportMessage message;
            //string tag;

            //tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //if (!string.IsNullOrEmpty(tag) && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //{
            //    this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //    // Console.WriteLine("Received Tagged Message");

            //    return new TaggedTransportMessage(tag, message);
            //}
            //else
            //{
            //    var start = DateTime.Now;

            //    while (this.IsRunning)
            //    {
            //        tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //        if (!string.IsNullOrEmpty(tag) && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //        {
            //            this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //            // Console.WriteLine("Received Tagged Message");

            //            return new TaggedTransportMessage(tag, message);
            //        }
            //        else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
            //        {
            //            throw new Exception("Tagged message timeout");
            //        }

            //        if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
            //            await Task.Delay(1);
            //    }

            //    throw new Exception("Transport stopped");
            //}
        }
        private async Task<TransportMessage> GetBufferedTaggedData(string rid)
        {
            var receiveBuffer = this.TaggedReceiveBuffer.GetOrAdd(rid, (key) => new BlockingCollection<TransportMessage>());

            var data = receiveBuffer.Take();

            this.TaggedReceiveBuffer.TryRemove(rid, out _);

            return data;
        }
        private async Task<TransportMessage> GetBufferedTaggedData(string rid, CancellationToken cancellationToken)
        {
            var receiveBuffer = this.TaggedReceiveBuffer.GetOrAdd(rid, (key) => new BlockingCollection<TransportMessage>());

            var data = receiveBuffer.Take(cancellationToken);

            this.TaggedReceiveBuffer.TryRemove(rid, out _);

            return data;
        }

        private async Task EnsureListening()
        {
            if (!this.IsListening)
            {
                await this.Listen();
            }
        }

        protected virtual void OnHandlerError(Exception ex)
        {
            //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
            this.HandlerError?.Invoke(this, new HandlerErrorEventArgs(ex));
        }
        protected virtual void OnListening()
        {
            this.Listening?.Invoke(this, new EventArgs());
        }
        protected virtual void OnClosed()
        {
            this.Closed?.Invoke(this, new EventArgs());
        }
    }

    public class DealerClientTransport : AClientTransport, IDealerClientTransport
    {
        public event EventHandler<HandlerErrorEventArgs> HandlerError;

        private bool isConnected = false;
        public override bool IsConnected
        {
            get => this.isConnected;
        }

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

        //private readonly string identity;
        //public string Identity
        //{
        //    get
        //    {
        //        return this.identity;
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

        private readonly BlockingCollection<TransportMessage> ReceiveBuffer;
        private readonly ConcurrentDictionary<string, BlockingCollection<TransportMessage>> TaggedReceiveBuffer;
        //private readonly ConcurrentQueue<Message> SendBuffer;

        private Task ListeningTask;
        //private DealerSocket Socket { get; set; }
        private NetMQQueue<TransportMessage> AltSendBuffer;

        public DealerClientTransport(IZeroMQClientEndpoint endpoint, int idleTimeout = 0)
            : base()
        {
            this.endpoint = endpoint;

            //this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            this.idleTimeout = idleTimeout;

            this.ReceiveBuffer = new BlockingCollection<TransportMessage>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, BlockingCollection<TransportMessage>>();
            //this.SendBuffer = new ConcurrentQueue<Message>();
        }
        public DealerClientTransport(IDiscoverer<IZeroMQClientEndpoint> discoverer, int idleTimeout = 0)
            : base()
        {
            this.discoverer = discoverer;

            //this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
            this.idleTimeout = idleTimeout;

            this.ReceiveBuffer = new BlockingCollection<TransportMessage>();
            this.TaggedReceiveBuffer = new ConcurrentDictionary<string, BlockingCollection<TransportMessage>>();
            //this.SendBuffer = new ConcurrentQueue<Message>();
        }

        public override async Task Connect()
        {
            var cancellationSource = new CancellationTokenSource(30000);

            await this.Connect(cancellationSource.Token);
        }
        public override async Task Connect(CancellationToken cancellationToken)
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
                if (cancellationToken.IsCancellationRequested)
                {
                    this.IsRunning = false;
                    await this.ListeningTask;

                    throw new OperationCanceledException("Connection timeout", cancellationToken);
                }

                await Task.Delay(500, cancellationToken);
            }
        }

        public override async Task Close()
        {
            var cancellationSource = new CancellationTokenSource(30000);

            await this.Close(cancellationSource.Token);
        }
        public override async Task Close(CancellationToken cancellationToken)
        {
            this.IsRunning = false;

            await this.ListeningTask;
        }

        public override async Task Send(TransportMessage message)
        {
            await this.EnsureConnected();

            var forwardedMessage = TransportMessage.FromMessage(message);

            this.OnMessageSending(forwardedMessage);

            this.AltSendBuffer.Enqueue(forwardedMessage);
        }
        public override Task Send(TransportMessage message, CancellationToken cancellationToken)
        {
            return this.SendAndReceive(message);
        }
        public override async Task Send(string messageId, TransportMessage message)
        {
            await this.EnsureConnected();

            var forwardedMessage = TransportMessage.FromMessage(message);

            var encodedMessageId = System.Text.Encoding.ASCII.GetBytes(messageId);
            forwardedMessage.Metadata.Add($"rid[{this.Identity}]", encodedMessageId);

            this.OnMessageSending(forwardedMessage);

            this.AltSendBuffer.Enqueue(forwardedMessage);
        }
        public override Task Send(string messageId, TransportMessage message, CancellationToken cancellationToken)
        {
            return this.Send(messageId, message);
        }

        public override async Task<TransportMessage> Receive()
        {
            var message = this.GetBufferedData();

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(CancellationToken cancellationToken)
        {
            var message = this.GetBufferedData(cancellationToken);

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(string messageId)
        {
            var message = this.GetBufferedTaggedData(messageId);

            this.OnMessageReceived(message);

            return message;
        }
        public override async Task<TransportMessage> Receive(string messageId, CancellationToken cancellationToken)
        {
            var message = this.GetBufferedTaggedData(messageId, cancellationToken);

            this.OnMessageReceived(message);

            return message;
        }

        public override async Task<TaggedTransportMessage> ReceiveTagged()
        {
            var taggedMessage = await this.GetBufferedTaggedData();

            this.OnMessageReceived(taggedMessage.Message);

            return taggedMessage;
        }
        public override async Task<TaggedTransportMessage> ReceiveTagged(CancellationToken cancellationToken)
        {
            var taggedMessage = await this.GetBufferedTaggedData(cancellationToken);

            this.OnMessageReceived(taggedMessage.Message);

            return taggedMessage;
        }

        public override async Task<Func<Task<TransportMessage>>> SendAndReceive(TransportMessage message)
        {
            await this.EnsureConnected();

            var forwardedMessage = TransportMessage.FromMessage(message);

            var messageId = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            var encodedMessageId = System.Text.Encoding.ASCII.GetBytes(messageId);
            forwardedMessage.Metadata.Add($"rid[{this.Identity}]", encodedMessageId);

            this.OnMessageSending(forwardedMessage);

            this.AltSendBuffer.Enqueue(forwardedMessage);

            return new Func<Task<TransportMessage>>(async () => {
                var responseMessage = this.GetBufferedTaggedData(messageId);

                this.OnMessageReceived(responseMessage);

                return responseMessage;
            });
        }
        public override async Task<Func<Task<TransportMessage>>> SendAndReceive(TransportMessage message, CancellationToken cancellationToken)
        {
            await this.EnsureConnected();

            var forwardedMessage = TransportMessage.FromMessage(message);

            var messageId = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            var encodedMessageId = System.Text.Encoding.ASCII.GetBytes(messageId);
            forwardedMessage.Metadata.Add($"rid[{this.Identity}]", encodedMessageId);

            this.OnMessageSending(forwardedMessage);

            this.AltSendBuffer.Enqueue(forwardedMessage);

            return new Func<Task<TransportMessage>>(async () => {
                var responseMessage = this.GetBufferedTaggedData(messageId, cancellationToken);

                this.OnMessageReceived(responseMessage);

                return responseMessage;
            });
        }

        private async Task ServerHandler()
        {
            while (this.IsRunning)
            {
                try
                {
                    DateTime lastActivityTime;

                    this.isConnected = false;

                    //var sendTimer = new NetMQTimer(TimeSpan.FromMilliseconds(10));

                    using (var socket = new DealerSocket())
                    using (this.AltSendBuffer = new NetMQQueue<TransportMessage>())
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

                                    var message = netmqMessage.ToMessage();

                                    this.OnMessageReceiving(message);

                                    byte[] encodedRid;
                                    if (message.Metadata.TryPluck($"rid[{this.Identity}]", out encodedRid))
                                    {
                                        var decodedRid = Encoding.ASCII.GetString(encodedRid);

                                        this.TaggedReceiveBuffer.AddOrUpdate(decodedRid, (key) => new BlockingCollection<TransportMessage>(), (key, receiveBuffer) =>
                                        {
                                            receiveBuffer.Add(message);
                                            return receiveBuffer;
                                        });
                                    }
                                    else
                                    {
                                        this.ReceiveBuffer.Add(message);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.OnHandlerError(ex);
                                //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
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
                                while (this.AltSendBuffer.TryDequeue(out var message, TimeSpan.Zero))
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
                                this.OnHandlerError(ex);
                                //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        var endpoint = await this.ResolveEndpoint(5000);

                        monitor.Connected += (sender, e) =>
                        {
                            Console.WriteLine($"Dealer socket conntected to {endpoint.ToConnectionString()}");
                            this.isConnected = true;
                        };
                        monitor.Disconnected += (sender, e) =>
                        {
                            Console.WriteLine($"Dealer socket disconntected from {endpoint.ToConnectionString()}");
                            this.isConnected = false;
                        };

                        Console.WriteLine($"Attempting to connect to {endpoint.ToConnectionString()}");
                        var monitorTask = monitor.StartAsync();
                        monitor.AttachToPoller(poller);

                        var pollerTask = new Task(poller.Run);
                        var pollerHandlerTask = pollerTask.ContinueWith((Task task) =>
                        {
                            var ex = task.Exception;

                            this.OnHandlerError(ex);
                            //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            this.isConnected = false;
                        }, TaskContinuationOptions.OnlyOnFaulted);
                        pollerTask.Start();

                        //Console.WriteLine($"Connecting Initial ({this.IsConnected}/{this.IsRunning})...");

                        socket.Connect(endpoint.ToConnectionString());

                        //Console.WriteLine($"Connecting Standby ({this.IsConnected}/{this.IsRunning})...");

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

                        //Console.WriteLine($"Holding pattern ({this.IsConnected}/{this.IsRunning})...");

                        lastActivityTime = DateTime.UtcNow;
                        while (this.IsConnected && this.IsRunning)
                        {
                            //Console.WriteLine("Hearbeat");

                            await Task.Delay(1000);

                            if (this.IdleTimeout > 0 && (DateTime.UtcNow - lastActivityTime).TotalMilliseconds > this.IdleTimeout)
                                this.IsRunning = false;
                        }

                        Console.WriteLine($"Closing dealer socket...");
                        poller.StopAsync();
                        socket.Disconnect(endpoint.ToConnectionString());
                        monitor.DetachFromPoller();
                        monitor.Stop();

                        this.isConnected = false;
                    }

                    if (this.IsRunning)
                        await Task.Delay(1000);
                }
                //catch (ConnectionTimeoutException ex)
                //{
                //}
                catch (Exception ex)
                {
                    this.OnHandlerError(ex);
                    //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        private DateTime lastGetBufferedData = DateTime.UtcNow;
        private TransportMessage GetBufferedData()
        {
            return this.ReceiveBuffer.Take();

            //TransportMessage message;
            //if (!this.ReceiveBuffer.IsEmpty && this.ReceiveBuffer.TryDequeue(out message))
            //{
            //    this.lastGetBufferedData = DateTime.UtcNow;
            //    // Console.WriteLine("Received Message");
            //    // MessageHelpers.WriteMessage(message);

            //    return message;
            //}
            //else
            //{
            //    var start = DateTime.Now;

            //    while (this.IsRunning)
            //    {
            //        if (!this.ReceiveBuffer.IsEmpty && this.ReceiveBuffer.TryDequeue(out message))
            //        {
            //            this.lastGetBufferedData = DateTime.UtcNow;
            //            // Console.WriteLine("Received Message");
            //            // MessageHelpers.WriteMessage(message);

            //            return message;
            //        }
            //        else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
            //        {
            //            throw new Exception("Message timeout");
            //        }

            //        if ((DateTime.UtcNow - this.lastGetBufferedData).TotalSeconds > 1)
            //            await Task.Delay(1);
            //    }

            //    throw new Exception("Transport stopped");
            //}
        }
        private TransportMessage GetBufferedData(CancellationToken cancellationToken)
        {
            return this.ReceiveBuffer.Take(cancellationToken);
        }

        private DateTime lastGetBufferedTaggedData = DateTime.UtcNow;
        private async Task<TaggedTransportMessage> GetBufferedTaggedData()
        {
            throw new NotImplementedException();

            //TransportMessage message;
            //string tag;

            //tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //{
            //    this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //    // Console.WriteLine("Received Tagged Message");

            //    return new TaggedTransportMessage(tag, message);
            //}
            //else
            //{
            //    var start = DateTime.Now;

            //    while (this.IsRunning)
            //    {
            //        tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //        if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //        {
            //            this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //            // Console.WriteLine("Received Tagged Message");

            //            return new TaggedTransportMessage(tag, message);
            //        }
            //        else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
            //        {
            //            throw new Exception("Tagged message timeout");
            //        }

            //        if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
            //            await Task.Delay(1);
            //    }

            //    throw new Exception("Transport stopped");
            //}
        }
        private async Task<TaggedTransportMessage> GetBufferedTaggedData(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();

            //TransportMessage message;
            //string tag;

            //tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //{
            //    this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //    // Console.WriteLine("Received Tagged Message");

            //    return new TaggedTransportMessage(tag, message);
            //}
            //else
            //{
            //    var start = DateTime.Now;

            //    while (this.IsRunning)
            //    {
            //        tag = this.TaggedReceiveBuffer.Keys.FirstOrDefault();
            //        if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(tag, out message))
            //        {
            //            this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //            // Console.WriteLine("Received Tagged Message");

            //            return new TaggedTransportMessage(tag, message);
            //        }
            //        else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
            //        {
            //            throw new Exception("Tagged message timeout");
            //        }

            //        if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
            //            await Task.Delay(1);
            //    }

            //    throw new Exception("Transport stopped");
            //}
        }
        private TransportMessage GetBufferedTaggedData(string rid)
        {
            var receiveBuffer = this.TaggedReceiveBuffer.GetOrAdd(rid, (key) => new BlockingCollection<TransportMessage>());

            var data = receiveBuffer.Take();

            this.TaggedReceiveBuffer.TryRemove(rid, out _);

            return data;

            //TransportMessage message;
            //if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(rid, out message))
            //{
            //    this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //    // Console.WriteLine("Received Tagged Message");

            //    return message;
            //}
            //else
            //{
            //    var start = DateTime.Now;

            //    while (this.IsRunning)
            //    {
            //        if (!this.TaggedReceiveBuffer.IsEmpty && this.TaggedReceiveBuffer.TryRemove(rid, out message))
            //        {
            //            this.lastGetBufferedTaggedData = DateTime.UtcNow;
            //            // Console.WriteLine("Received Tagged Message");

            //            return message;
            //        }
            //        else if (timeout > 0 && (DateTime.Now - start).TotalMilliseconds > timeout)
            //        {
            //            throw new Exception("Tagged message timeout");
            //        }

            //        if ((DateTime.UtcNow - this.lastGetBufferedTaggedData).TotalSeconds > 1)
            //            await Task.Delay(1);
            //    }

            //    throw new Exception("Transport stopped");
            //}
        }
        private TransportMessage GetBufferedTaggedData(string rid, CancellationToken cancellationToken)
        {
            var receiveBuffer = this.TaggedReceiveBuffer.GetOrAdd(rid, (key) => new BlockingCollection<TransportMessage>());

            var data = receiveBuffer.Take(cancellationToken);

            this.TaggedReceiveBuffer.TryRemove(rid, out _);

            return data;
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

        protected virtual void OnHandlerError(Exception ex)
        {
            //Console.WriteLine(ex.Message);
            this.HandlerError?.Invoke(this, new HandlerErrorEventArgs(ex));
        }
    }

    internal static class MessageHelpers
    {
        public static TransportMessage ToMessage(this NetMQMessage netmqMessage)
        {
            var metadata = new VolatileTransportMetadata();

            byte[] payload = null;
            int signal = -1;

            int startingFrame = 0;

            signal = netmqMessage[startingFrame].ConvertToInt32();
            if (signal != 0)
                throw new Exception("Message received with signal code " + signal.ToString());
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

                        metadata.Frames.Add(new VolatileTransportMetadataFrame(name, framePayload));
                    }
                    else if (partBuffer.Count == 0)
                    {
                        payload = frame.ToByteArray();
                    }
                    else
                    {
                        for (var b = 0; b < netmqMessage.FrameCount; b++)
                            Console.WriteLine($"{b}: [ {BitConverter.ToString(netmqMessage[b].ToByteArray()).Replace("-", " ")} ]");

                        for (var b = 0; b < netmqMessage.FrameCount; b++)
                            Console.WriteLine($"{b}: [ {System.Text.Encoding.ASCII.GetString(netmqMessage[b].ToByteArray())} ] [ {System.Text.Encoding.UTF8.GetString(netmqMessage[b].ToByteArray())} ]");

                        throw new Exception("Unexpected frame count (" + a + ") " + partBuffer.Count.ToString());
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

            return new TransportMessage(payload, metadata);
        }
        public static TransportMessage ToMessage(this NetMQMessage netmqMessage, out byte[] envelope)
        {
            var metadata = new VolatileTransportMetadata();

            byte[] payload = null;
            int signal = -1;

            int startingFrame = 0;

            envelope = netmqMessage[0].ToByteArray();
            startingFrame++;

            signal = netmqMessage[startingFrame].ConvertToInt32();
            if (signal != 0)
                throw new Exception("Message received with signal code " + signal.ToString());
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

                        metadata.Frames.Add(new VolatileTransportMetadataFrame(name, framePayload));
                    }
                    else if (partBuffer.Count == 0)
                    {
                        payload = frame.ToByteArray();
                    }
                    else
                    {
                        for (var b = 0; b < netmqMessage.FrameCount; b++)
                            Console.WriteLine($"{b}: [ {BitConverter.ToString(netmqMessage[b].ToByteArray()).Replace("-", " ")} ]");

                        for (var b = 0; b < netmqMessage.FrameCount; b++)
                            Console.WriteLine($"{b}: [ {System.Text.Encoding.ASCII.GetString(netmqMessage[b].ToByteArray())} ] [ {System.Text.Encoding.UTF8.GetString(netmqMessage[b].ToByteArray())} ]");

                        throw new Exception("Unexpected frame count (" + a + ") " + partBuffer.Count.ToString());
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

            return new TransportMessage(payload, metadata);
        }

        public static NetMQMessage ToNetMQMessage(this TransportMessage message)
        {
            var netqmMessage = new NetMQMessage();

            netqmMessage.Append(0);

            foreach (var frame in message.Metadata.Frames)
            {
                netqmMessage.Append(new NetMQFrame(System.Text.Encoding.ASCII.GetBytes(frame.Id)));
                netqmMessage.Append(new NetMQFrame(frame.Data));
                netqmMessage.AppendEmptyFrame();
            }

            netqmMessage.Append(new NetMQFrame(message.Payload));

            return netqmMessage;
        }
        public static NetMQMessage ToNetMQMessage(this TransportMessage message, byte[] envelope)
        {
            var netqmMessage = new NetMQMessage();

            netqmMessage.Append(new NetMQFrame(envelope));

            netqmMessage.Append(0);

            foreach (var frame in message.Metadata.Frames)
            {
                netqmMessage.Append(new NetMQFrame(System.Text.Encoding.ASCII.GetBytes(frame.Id)));
                netqmMessage.Append(new NetMQFrame(frame.Data));
                netqmMessage.AppendEmptyFrame();
            }

            netqmMessage.Append(new NetMQFrame(message.Payload));

            return netqmMessage;
        }

        public static void WriteMessage(TransportMessage message)
        {
            //if (message.Envelope != null)
            //    Console.WriteLine("  Envelope" + " [ " + BitConverter.ToString(message.Envelope).Replace("-", " ") + " ]");
            foreach (var frame in message.Metadata.Frames)
                Console.WriteLine("  " + frame.Id + " [ " + BitConverter.ToString(frame.Data).Replace("-", " ") + " ]");
            Console.WriteLine("  Payload" + " [ " + BitConverter.ToString(message.Payload).Replace("-", " ") + " ]");
            //Console.WriteLine("  Signal " + message.Signal);
        }

        public static NetMQMessage CreateNetMQErrorMessage(string message, ITransportMetadata metadata)
        {
            var netqmMessage = new NetMQMessage();

            netqmMessage.Append(1);

            foreach (var frame in metadata.Frames)
            {
                netqmMessage.Append(new NetMQFrame(System.Text.Encoding.ASCII.GetBytes(frame.Id)));
                netqmMessage.Append(new NetMQFrame(frame.Data));
                netqmMessage.AppendEmptyFrame();
            }

            netqmMessage.Append(new NetMQFrame(Encoding.UTF8.GetBytes(message)));

            return netqmMessage;
        }
        public static NetMQMessage CreateNetMQErrorMessage(byte[] envelope, string message, ITransportMetadata metadata)
        {
            var netqmMessage = new NetMQMessage();

            netqmMessage.Append(new NetMQFrame(envelope));

            netqmMessage.Append(1);

            foreach (var frame in metadata.Frames)
            {
                netqmMessage.Append(new NetMQFrame(System.Text.Encoding.ASCII.GetBytes(frame.Id)));
                netqmMessage.Append(new NetMQFrame(frame.Data));
                netqmMessage.AppendEmptyFrame();
            }

            netqmMessage.Append(new NetMQFrame(Encoding.UTF8.GetBytes(message)));

            return netqmMessage;
        }
    }
}
