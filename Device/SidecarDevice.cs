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
    public class SidecarBackendDiscoverer
    {
        public readonly string Name;
        public readonly IDiscoverer<IZeroMQClientEndpoint> Discoverer;

        public SidecarBackendDiscoverer(string name, IDiscoverer<IZeroMQClientEndpoint> discoverer)
        {
            this.Name = name;
            this.Discoverer = discoverer;
        }
    }

    public class SidecarDevice
    {
        private class RegisteredBackend
        {
            public string ServiceName { get; private set; }
            public IZeroMQClientEndpoint Endpoint { get; private set; }
            public DateTime RegisteredDate { get; private set; }

            public NetMQQueue<TransportMessage> BackendBuffer { get; private set; }

            public SidecarDevice ForwarderDevice { get; private set; }

            public bool IsRunning { get; private set; }
            public bool IsConnected { get; private set; }

            private Task HandlerTask;

            public RegisteredBackend(string serviceName, IZeroMQClientEndpoint endpoint, SidecarDevice forwarderDevice)
            {
                this.ServiceName = serviceName;
                this.Endpoint = endpoint;
                this.ForwarderDevice = forwarderDevice;

                this.Heartbeat();
            }

            public void Heartbeat()
            {
                this.RegisteredDate = DateTime.UtcNow;
            }

            public async Task Connect(int timeout = 0)
            {
                if (!this.IsRunning)
                {
                    this.IsRunning = true;

                    this.HandlerTask = Task.Factory.StartNew(() => this.Handler());
                }

                var startTime = DateTime.UtcNow;
                while (!this.IsConnected)
                {
                    if (timeout > 0 && (DateTime.UtcNow - startTime).TotalMilliseconds > timeout)
                    {
                        this.IsRunning = false;
                        await this.HandlerTask;

                        throw new Exception("Connection timeout");
                    }

                    await Task.Delay(500);
                }
            }
            public async Task Close()
            {
                this.IsRunning = false;
                await this.HandlerTask;
            }

            private async Task Handler()
            {
                while (this.IsRunning)
                {
                    try
                    {
                        using (var socket = new DealerSocket())
                        using (this.BackendBuffer = new NetMQQueue<TransportMessage>())
                        using (var poller = new NetMQPoller() { socket, this.BackendBuffer })
                        using (var monitor = new NetMQ.Monitoring.NetMQMonitor(socket, $"inproc://monitor.forwarderdevice.backend.{Guid.NewGuid().ToString()}", SocketEvents.Connected | SocketEvents.Disconnected))
                        {
                            socket.Options.Identity = System.Text.Encoding.ASCII.GetBytes(Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant());

                            socket.ReceiveReady += (sender, e) =>
                            {
                                try
                                {
                                    var netmqMessage = new NetMQMessage();
                                    if (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                    {
                                        var message = netmqMessage.ToMessage();

                                        this.ForwarderDevice.OnBackendReceived(message);

                                        this.ForwarderDevice.frontendBuffer.Enqueue(message);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                                }
                            };

                            this.BackendBuffer.ReceiveReady += (sender, e) =>
                            {
                                try
                                {
                                    while (this.BackendBuffer.TryDequeue(out TransportMessage message, TimeSpan.Zero))
                                    {
                                        this.ForwarderDevice.OnFrontendForwarded(message);

                                        if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage()))
                                        {
                                            Console.WriteLine("Failed to send message");
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                                }
                            };

                            monitor.Connected += (sender, e) =>
                            {
                                Console.WriteLine($"Dealer socket conntected to {this.Endpoint.ToConnectionString()}");
                                this.IsConnected = true;
                            };
                            monitor.Disconnected += (sender, e) =>
                            {
                                Console.WriteLine($"Dealer socket disconntected from {this.Endpoint.ToConnectionString()}");
                                this.IsConnected = false;
                            };

                            Console.WriteLine($"Attempting to connect to {this.Endpoint.ToConnectionString()}");
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

                            socket.Connect(this.Endpoint.ToConnectionString());

                            var start = DateTime.Now;
                            while (!this.IsConnected)
                            {
                                if ((DateTime.Now - start).TotalMilliseconds > 5000)
                                {
                                    throw new Exception($"Connection timeout [{this.ServiceName}]");
                                }

                                await Task.Delay(1000);
                            }

                            while (this.IsConnected && this.IsRunning)
                            {
                                await Task.Delay(1000);
                            }

                            Console.WriteLine("Closing dealer socket...");
                            poller.StopAsync();
                            socket.Disconnect(this.Endpoint.ToConnectionString());
                            monitor.DetachFromPoller();
                            monitor.Stop();

                            this.IsConnected = false;
                        }

                        if (this.IsRunning)
                            await Task.Delay(1000);
                    }
                    catch (Exception ex)
                    {
                        //Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                    }
                }
            }
        }

        public event EventHandler<MessagingEventArgs> FrontendReceived;
        public event EventHandler<MessagingEventArgs> FrontendForwarded;

        public event EventHandler<MessagingEventArgs> BackendReceived;
        public event EventHandler<MessagingEventArgs> BackendForwarded;

        private readonly string identity;
        public string Identity => identity;

        private readonly IZeroMQServerEndpoint frontendEndpoint;
        public IZeroMQServerEndpoint FrontendEndpoint => frontendEndpoint;

        private readonly SidecarBackendDiscoverer[] backendDiscoverers;
        public SidecarBackendDiscoverer[] BackendDiscoverers => backendDiscoverers;

        private bool isRunning = false;
        public bool IsRunning => isRunning;

        private Task handlerTask;
        private NetMQQueue<TransportMessage> frontendBuffer;
        private ConcurrentDictionary<string, ConcurrentQueue<string>> backendEndpointIds;
        private ConcurrentDictionary<string, RegisteredBackend> backendEndpoints;

        public SidecarDevice(IZeroMQServerEndpoint endpoint, SidecarBackendDiscoverer[] backendDiscoverers)
            : base()
        {
            this.frontendEndpoint = endpoint;
            this.backendDiscoverers = backendDiscoverers;

            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.backendEndpointIds = new ConcurrentDictionary<string, ConcurrentQueue<string>>();
            this.backendEndpoints = new ConcurrentDictionary<string, RegisteredBackend>();
        }

        public Task Start()
        {
            if (!this.IsRunning)
            {
                this.isRunning = true;

                this.handlerTask = Task.WhenAll(
                    Task.Factory.StartNew(() => this.Handler()),
                    Task.Factory.StartNew(() => this.BackendDiscoveryHandler())
                );
            }

            return Task.FromResult(true);
        }
        public async Task Run()
        {
            await this.Start();
            await this.handlerTask;
        }
        public async Task Stop()
        {
            this.isRunning = false;

            await this.handlerTask;
        }

        private async Task Handler()
        {
            while (this.IsRunning)
            {
                try
                {
                    bool isListening = false;
                    var connectionString = this.FrontendEndpoint.ToConnectionString();

                    using (var frontendSocket = new RouterSocket())
                    using (this.frontendBuffer = new NetMQQueue<TransportMessage>())
                    using (var poller = new NetMQPoller() { frontendSocket, this.frontendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(frontendSocket, $"inproc://monitor.forwarderdevice.{Guid.NewGuid().ToString()}", SocketEvents.Listening | SocketEvents.Accepted | SocketEvents.Disconnected | SocketEvents.Closed))
                    {
                        frontendSocket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                var netmqMessage = new NetMQMessage();
                                while (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    var message = netmqMessage.ToMessage(out var envelope);
                                    //var sourceEnvelope = message.Envelope;

                                    string service = message.Metadata.TryGetLast("service", out var encodedService) ? Encoding.UTF8.GetString(encodedService) : null;

                                    this.OnFrontendReceived(message);

                                    var forwardStart = DateTime.UtcNow;
                                    RegisteredBackend registeredBackend = null;
                                    if (!string.IsNullOrEmpty(service))
                                    {
                                        while (this.IsRunning && registeredBackend == null)
                                        {
                                            if (this.backendEndpointIds.TryGetValue(service, out var backendEndpointIds))
                                            {
                                                while (backendEndpointIds.TryDequeue(out string backendIdentifier))
                                                {
                                                    if (this.backendEndpoints.TryGetValue(backendIdentifier, out RegisteredBackend candidateBackend))
                                                    {
                                                        backendEndpointIds.Enqueue(backendIdentifier);

                                                        if (candidateBackend.IsConnected)
                                                        {
                                                            registeredBackend = this.backendEndpoints[backendIdentifier];
                                                            break;
                                                        }
                                                    }
                                                }
                                            }

                                            if (registeredBackend != null || (DateTime.Now - forwardStart).TotalMilliseconds > 30000)
                                            {
                                                break;
                                            }

                                            System.Threading.Thread.Sleep(100);
                                        }
                                    }

                                    if (registeredBackend != null)
                                    {
                                        message.Metadata.Add($"envelope[{this.Identity}]", envelope);

                                        registeredBackend.BackendBuffer.Enqueue(message);
                                    }
                                    else
                                    {
                                        Console.WriteLine("No backends available!!!");

                                        var nmqm = MessageHelpers.CreateNetMQErrorMessage(envelope, "No backends found", message.Metadata);
                                        e.Socket.SendMultipartMessage(nmqm);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        this.frontendBuffer.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                while (this.frontendBuffer.TryDequeue(out TransportMessage message, TimeSpan.Zero))
                                {
                                    if (!message.Metadata.TryPluckLast($"envelope[{this.Identity}]", out var envelope))
                                        throw new Exception("Message envelope not found");

                                    this.OnBackendForwarded(message);

                                    if (!frontendSocket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(envelope)))
                                    {
                                        Console.WriteLine("Failed to forward to frontend");
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
                            Console.WriteLine($"Frontend router socket listening at {connectionString}");
                            isListening = true;
                        };
                        monitor.Closed += (sender, e) =>
                        {
                            Console.WriteLine($"Frontend router socket closed on {connectionString}");
                            isListening = false;
                        };
                        monitor.Accepted += (sender, e) =>
                        {
                            Console.WriteLine($"Frontend router socket connection accepted at {connectionString}");
                        };
                        monitor.Disconnected += (sender, e) =>
                        {
                            Console.WriteLine($"Frontend router socket disconnected at {connectionString}");
                        };

                        Console.WriteLine($"Attempting to bind frontend socket to {connectionString}");
                        monitor.StartAsync();
                        monitor.AttachToPoller(poller);

                        var pollerTask = new Task(poller.Run);
                        pollerTask.ContinueWith((Task task) =>
                        {
                            var ex = task.Exception;

                            Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            isListening = false;
                        }, TaskContinuationOptions.OnlyOnFaulted);
                        pollerTask.Start();

                        frontendSocket.Bind(connectionString);

                        var start = DateTime.Now;
                        while (!isListening)
                        {
                            if ((DateTime.Now - start).TotalMilliseconds > 5000)
                                throw new Exception($"Frontend socket bind timeout ({connectionString})");

                            await Task.Delay(1000);
                        }

                        while (this.IsRunning && isListening)
                        {
                            await Task.Delay(1000);
                        }

                        poller.StopAsync();
                        frontendSocket.Disconnect(connectionString);
                        monitor.DetachFromPoller();
                        monitor.Stop();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        private async Task BackendDiscoveryHandler()
        {
            while (this.IsRunning)
            {
                try
                {
                    foreach (var backendDiscoverer in this.BackendDiscoverers)
                    {
                        var backendEndpointIds = this.backendEndpointIds.GetOrAdd(backendDiscoverer.Name, new ConcurrentQueue<string>());

                        var endpoints = await backendDiscoverer.Discoverer.DiscoverAll();

                        foreach (var endpoint in endpoints)
                        {
                            var endpointId = BitConverter.ToString(endpoint.Encode());

                            var registeredEndpoint = new RegisteredBackend(backendDiscoverer.Name, endpoint, this);

                            if (this.backendEndpoints.TryAdd(endpointId, registeredEndpoint))
                            {
                                registeredEndpoint.Connect();

                                Console.WriteLine($"Backend registered [ {backendDiscoverer.Name}/{endpointId} ] ");
                                backendEndpointIds.Enqueue(endpointId);
                            }
                            else
                            {
                                this.backendEndpoints[endpointId].Heartbeat();
                            }
                        }

                        //foreach (var endpointId in this.backendEndpoints.Keys)
                        //{
                        //    if (!endpoints.Any(endpoint => BitConverter.ToString(endpoint.Encode()) == endpointId))
                        //    {
                        //        if (this.backendEndpoints.TryRemove(endpointId, out RegisteredBackend expiredBackend))
                        //        {
                        //            Console.WriteLine($"Backend {backendDiscoverer.Name}/{endpointId} expired");
                        //            expiredBackend.Close();
                        //        }
                        //    }
                        //}
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                }

                await Task.Delay(5000);
            }
        }

        protected virtual void OnFrontendReceived(TransportMessage message)
        {
            this.FrontendReceived?.Invoke(this, new MessagingEventArgs(message));
        }
        protected virtual void OnFrontendForwarded(TransportMessage message)
        {
            this.FrontendForwarded?.Invoke(this, new MessagingEventArgs(message));
        }

        protected virtual void OnBackendReceived(TransportMessage message)
        {
            this.BackendReceived?.Invoke(this, new MessagingEventArgs(message));
        }
        protected virtual void OnBackendForwarded(TransportMessage message)
        {
            this.BackendForwarded?.Invoke(this, new MessagingEventArgs(message));
        }
    }

    //public class ForwarderDevice
    //{
    //    private class RegisteredBackend
    //    {
    //        public IZeroMQClientEndpoint Endpoint { get; private set; }
    //        public DateTime RegisteredDate { get; private set; }

    //        public NetMQQueue<Message> BackendBuffer { get; private set; }

    //        public ForwarderDevice ForwarderDevice { get; private set; }

    //        public bool IsRunning { get; private set; }
    //        public bool IsConnected { get; private set; }

    //        private Task HandlerTask;

    //        public RegisteredBackend(IZeroMQClientEndpoint endpoint, ForwarderDevice forwarderDevice)
    //        {
    //            this.Endpoint = endpoint;
    //            this.ForwarderDevice = forwarderDevice;

    //            this.Heartbeat();
    //        }

    //        public void Heartbeat()
    //        {
    //            this.RegisteredDate = DateTime.UtcNow;
    //        }

    //        public async Task Connect(int timeout = 0)
    //        {
    //            if (!this.IsRunning)
    //            {
    //                this.IsRunning = true;

    //                this.HandlerTask = Task.Factory.StartNew(() => this.Handler());
    //            }

    //            var startTime = DateTime.UtcNow;
    //            while (!this.IsConnected)
    //            {
    //                if (timeout > 0 && (DateTime.UtcNow - startTime).TotalMilliseconds > timeout)
    //                {
    //                    this.IsRunning = false;
    //                    await this.HandlerTask;

    //                    throw new Exception("Connection timeout");
    //                }

    //                await Task.Delay(500);
    //            }
    //        }
    //        public async Task Close()
    //        {
    //            this.IsRunning = false;
    //            await this.HandlerTask;
    //        }

    //        private async Task Handler()
    //        {
    //            while (this.IsRunning)
    //            {
    //                try
    //                {
    //                    using (var socket = new DealerSocket())
    //                    using (this.BackendBuffer = new NetMQQueue<Message>())
    //                    using (var poller = new NetMQPoller() { socket, this.BackendBuffer })
    //                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(socket, $"inproc://monitor.forwarderdevice.backend.{Guid.NewGuid().ToString()}", SocketEvents.Connected | SocketEvents.Disconnected))
    //                    {
    //                        socket.Options.Identity = System.Text.Encoding.ASCII.GetBytes(Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant());

    //                        socket.ReceiveReady += (sender, e) =>
    //                        {
    //                            try
    //                            {
    //                                var netmqMessage = new NetMQMessage();
    //                                if (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
    //                                {
    //                                    var message = netmqMessage.ToMessage(false);
    //                                    if (message.TryPluckFrame($"envelope[{this.ForwarderDevice.Identity}]", out byte[] envelopeMetadata))
    //                                    {
    //                                        message.Envelope = envelopeMetadata;
    //                                        this.ForwarderDevice.frontendBuffer.Enqueue(message);
    //                                    }
    //                                }
    //                            }
    //                            catch (Exception ex)
    //                            {
    //                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                            }
    //                        };

    //                        this.BackendBuffer.ReceiveReady += (sender, e) =>
    //                        {
    //                            try
    //                            {
    //                                while (this.BackendBuffer.TryDequeue(out Message message, TimeSpan.Zero))
    //                                {
    //                                    if (!socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
    //                                    {
    //                                        Console.WriteLine("Failed to send message");
    //                                    }
    //                                }
    //                            }
    //                            catch (Exception ex)
    //                            {
    //                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                            }
    //                        };

    //                        monitor.Connected += (sender, e) =>
    //                        {
    //                            Console.WriteLine($"Dealer socket conntected to {this.Endpoint.ToConnectionString()}");
    //                            this.IsConnected = true;
    //                        };
    //                        monitor.Disconnected += (sender, e) =>
    //                        {
    //                            Console.WriteLine($"Dealer socket disconntected from {this.Endpoint.ToConnectionString()}");
    //                            this.IsConnected = false;
    //                        };

    //                        Console.WriteLine($"Attempting to connect to {this.Endpoint.ToConnectionString()}");
    //                        monitor.StartAsync();
    //                        monitor.AttachToPoller(poller);

    //                        var pollerTask = new Task(poller.Run);
    //                        pollerTask.ContinueWith((Task task) =>
    //                        {
    //                            var ex = task.Exception;

    //                            Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                            this.IsConnected = false;
    //                        }, TaskContinuationOptions.OnlyOnFaulted);
    //                        pollerTask.Start();

    //                        socket.Connect(this.Endpoint.ToConnectionString());

    //                        var start = DateTime.Now;
    //                        while (!this.IsConnected)
    //                        {
    //                            if ((DateTime.Now - start).TotalMilliseconds > 5000)
    //                            {
    //                                throw new Exception("Connection timeout");
    //                            }

    //                            await Task.Delay(1000);
    //                        }

    //                        while (this.IsConnected && this.IsRunning)
    //                        {
    //                            await Task.Delay(1000);
    //                        }

    //                        Console.WriteLine("Closing dealer socket...");
    //                        poller.StopAsync();
    //                        socket.Disconnect(this.Endpoint.ToConnectionString());
    //                        monitor.DetachFromPoller();
    //                        monitor.Stop();

    //                        this.IsConnected = false;
    //                    }

    //                    if (this.IsRunning)
    //                        await Task.Delay(1000);
    //                }
    //                catch (Exception ex)
    //                {
    //                    Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                }
    //            }
    //        }
    //    }

    //    private readonly string identity;
    //    public string Identity => identity;

    //    private readonly IZeroMQServerEndpoint frontendEndpoint;
    //    public IZeroMQServerEndpoint FrontendEndpoint => frontendEndpoint;

    //    private readonly IDiscoverer<IZeroMQClientEndpoint> backendDiscoverer;
    //    public IDiscoverer<IZeroMQClientEndpoint> BackendDiscoverer => backendDiscoverer;

    //    private bool isRunning = false;
    //    public bool IsRunning => isRunning;

    //    private Task handlerTask;
    //    private NetMQQueue<Message> frontendBuffer;
    //    private ConcurrentQueue<string> backendEndpointIds;
    //    private ConcurrentDictionary<string, RegisteredBackend> backendEndpoints;

    //    public ForwarderDevice(IZeroMQServerEndpoint endpoint, IDiscoverer<IZeroMQClientEndpoint> backendDiscoverer)
    //        : base()
    //    {
    //        this.frontendEndpoint = endpoint;
    //        this.backendDiscoverer = backendDiscoverer;

    //        this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

    //        this.backendEndpointIds = new ConcurrentQueue<string>();
    //        this.backendEndpoints = new ConcurrentDictionary<string, RegisteredBackend>();
    //    }

    //    public Task Start()
    //    {
    //        if (!this.IsRunning)
    //        {
    //            this.isRunning = true;

    //            this.handlerTask = Task.WhenAll(
    //                Task.Factory.StartNew(() => this.Handler()),
    //                Task.Factory.StartNew(() => this.BackendDiscoveryHandler())
    //            );
    //        }

    //        return Task.FromResult(true);
    //    }
    //    public async Task Run()
    //    {
    //        await this.Start();
    //        await this.handlerTask;
    //    }
    //    public async Task Stop()
    //    {
    //        this.isRunning = false;

    //        await this.handlerTask;
    //    }

    //    private async Task Handler()
    //    {
    //        while (this.IsRunning)
    //        {
    //            try
    //            {
    //                bool isListening = false;
    //                var connectionString = this.FrontendEndpoint.ToConnectionString();

    //                using (var frontendSocket = new RouterSocket())
    //                using (this.frontendBuffer = new NetMQQueue<Message>())
    //                using (var poller = new NetMQPoller() { frontendSocket, this.frontendBuffer })
    //                using (var monitor = new NetMQ.Monitoring.NetMQMonitor(frontendSocket, $"inproc://monitor.forwarderdevice.{Guid.NewGuid().ToString()}", SocketEvents.Listening | SocketEvents.Accepted | SocketEvents.Disconnected | SocketEvents.Closed))
    //                {
    //                    frontendSocket.ReceiveReady += (sender, e) =>
    //                    {
    //                        try
    //                        {
    //                            var netmqMessage = new NetMQMessage();
    //                            while (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
    //                            {
    //                                var message = netmqMessage.ToMessage(true);
    //                                var sourceEnvelope = message.Envelope;

    //                                var forwardStart = DateTime.UtcNow;
    //                                RegisteredBackend registeredBackend = null;
    //                                while (this.IsRunning && registeredBackend == null)
    //                                {
    //                                    while (this.backendEndpointIds.TryDequeue(out string backendIdentifier))
    //                                    {
    //                                        if (this.backendEndpoints.TryGetValue(backendIdentifier, out RegisteredBackend candidateBackend))
    //                                        {
    //                                            this.backendEndpointIds.Enqueue(backendIdentifier);

    //                                            if (candidateBackend.IsConnected)
    //                                            {
    //                                                registeredBackend = this.backendEndpoints[backendIdentifier];
    //                                                break;
    //                                            }
    //                                        }

    //                                        //if ((DateTime.UtcNow - registeredBackend.RegisteredDate).TotalMilliseconds < 10000)
    //                                        //{
    //                                        //    this.backendEndpointIds.Enqueue(backendIdentifier);
    //                                        //    break;
    //                                        //}
    //                                        //else
    //                                        //{
    //                                        //    if (this.backendEndpoints.TryRemove(backendIdentifier, out RegisteredBackend expiredBackend))
    //                                        //    {
    //                                        //        Console.WriteLine($"Backend {backendIdentifier} expired");

    //                                        //        expiredBackend.Close().ContinueWith((result) =>
    //                                        //        {
    //                                        //            Console.WriteLine($"Backend {backendIdentifier} closed");
    //                                        //        });
    //                                        //    }
    //                                        //}
    //                                    }

    //                                    if (registeredBackend != null || (DateTime.Now - forwardStart).TotalMilliseconds > 30000)
    //                                    {
    //                                        break;
    //                                    }

    //                                    System.Threading.Thread.Sleep(100);
    //                                }

    //                                if (registeredBackend != null)
    //                                {
    //                                    message.Frames.Add($"envelope[{this.Identity}]", message.Envelope);
    //                                    message.Envelope = null;

    //                                    registeredBackend.BackendBuffer.Enqueue(message);
    //                                }
    //                                else
    //                                {
    //                                    Console.WriteLine("No backends available!!!");

    //                                    var forwardedMessage = new Message(1, message.Frames, System.Text.Encoding.UTF8.GetBytes("No backends found"), message.Envelope);
    //                                    e.Socket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
    //                                }
    //                            }
    //                        }
    //                        catch (Exception ex)
    //                        {
    //                            Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                        }
    //                    };

    //                    this.frontendBuffer.ReceiveReady += (sender, e) =>
    //                    {
    //                        try
    //                        {
    //                            while (this.frontendBuffer.TryDequeue(out Message message, TimeSpan.Zero))
    //                            {
    //                                if (!frontendSocket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
    //                                {
    //                                    Console.WriteLine("Failed to forward to frontend");
    //                                }
    //                            }
    //                        }
    //                        catch (Exception ex)
    //                        {
    //                            Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                        }
    //                    };

    //                    monitor.Listening += (sender, e) =>
    //                    {
    //                        Console.WriteLine($"Frontend router socket listening at {connectionString}");
    //                        isListening = true;
    //                    };
    //                    monitor.Closed += (sender, e) =>
    //                    {
    //                        Console.WriteLine($"Frontend router socket closed on {connectionString}");
    //                        isListening = false;
    //                    };
    //                    monitor.Accepted += (sender, e) =>
    //                    {
    //                        Console.WriteLine($"Frontend router socket connection accepted at {connectionString}");
    //                    };
    //                    monitor.Disconnected += (sender, e) =>
    //                    {
    //                        Console.WriteLine($"Frontend router socket disconnected at {connectionString}");
    //                    };

    //                    Console.WriteLine($"Attempting to bind frontend socket to {connectionString}");
    //                    monitor.StartAsync();
    //                    monitor.AttachToPoller(poller);

    //                    var pollerTask = new Task(poller.Run);
    //                    pollerTask.ContinueWith((Task task) =>
    //                    {
    //                        var ex = task.Exception;

    //                        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //                        isListening = false;
    //                    }, TaskContinuationOptions.OnlyOnFaulted);
    //                    pollerTask.Start();

    //                    frontendSocket.Bind(connectionString);

    //                    var start = DateTime.Now;
    //                    while (!isListening)
    //                    {
    //                        if ((DateTime.Now - start).TotalMilliseconds > 5000)
    //                            throw new Exception($"Frontend socket bind timeout ({connectionString})");

    //                        await Task.Delay(1000);
    //                    }

    //                    while (this.IsRunning && isListening)
    //                    {
    //                        await Task.Delay(1000);
    //                    }

    //                    poller.StopAsync();
    //                    frontendSocket.Disconnect(connectionString);
    //                    monitor.DetachFromPoller();
    //                    monitor.Stop();
    //                }
    //            }
    //            catch (Exception ex)
    //            {
    //                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //            }
    //        }
    //    }

    //    private async Task BackendDiscoveryHandler()
    //    {
    //        while (this.IsRunning)
    //        {
    //            try
    //            {
    //                var endpoints = await this.BackendDiscoverer.DiscoverAll();

    //                foreach (var endpoint in endpoints)
    //                {
    //                    var endpointId = BitConverter.ToString(endpoint.Encode());

    //                    var registeredEndpoint = new RegisteredBackend(endpoint, this);

    //                    if (this.backendEndpoints.TryAdd(endpointId, registeredEndpoint))
    //                    {
    //                        registeredEndpoint.Connect();

    //                        Console.WriteLine($"Backend registered [ {endpointId} ] ");
    //                        this.backendEndpointIds.Enqueue(endpointId);
    //                    }
    //                    else
    //                    {
    //                        this.backendEndpoints[endpointId].Heartbeat();
    //                    }
    //                }

    //                foreach (var endpointId in this.backendEndpoints.Keys)
    //                {
    //                    if (!endpoints.Any(endpoint => BitConverter.ToString(endpoint.Encode()) == endpointId))
    //                    {
    //                        if (this.backendEndpoints.TryRemove(endpointId, out RegisteredBackend expiredBackend))
    //                        {
    //                            Console.WriteLine($"Backend {endpointId} expired");
    //                            expiredBackend.Close();
    //                        }
    //                    }
    //                }
    //            }
    //            catch (Exception ex)
    //            {
    //                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
    //            }

    //            await Task.Delay(5000);
    //        }
    //    }
    //}
}
