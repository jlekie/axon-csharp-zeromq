using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using NetMQ;
using NetMQ.Sockets;

namespace Axon.ZeroMQ
{
    public class ZmqToInprocSplitterDevice
    {
        public event EventHandler<DiagnosticMessageEventArgs> DiagnosticMessage;

        public IZeroMQServerEndpoint FrontendEndpoint { get; }

        private string Identity { get; } = Guid.NewGuid().ToString("N").ToLowerInvariant();
        private ConcurrentDictionary<string, BlockingCollection<InprocClientTransport>> Backends { get; } = new ConcurrentDictionary<string, BlockingCollection<InprocClientTransport>>();

        public bool IsRunning { get; private set; }
        private Task HandlerTask { get; set; }
        private NetMQQueue<TransportMessage> FrontendBuffer { get; set; }

        public ZmqToInprocSplitterDevice(IZeroMQServerEndpoint endpoint)
            : base()
        {
            this.FrontendEndpoint = endpoint;
        }

        public async Task Start()
        {
            if (!this.IsRunning)
            {
                this.IsRunning = true;
                this.HandlerTask = Task.Factory.StartNew(() => this.Handler(), TaskCreationOptions.LongRunning);
            }
        }
        public async Task Run()
        {
            await this.Start();
            await this.HandlerTask;
        }
        public async Task Stop()
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
                    bool isListening = false;
                    var connectionString = this.FrontendEndpoint.ToConnectionString();

                    using (var frontendSocket = new RouterSocket())
                    using (this.FrontendBuffer = new NetMQQueue<TransportMessage>())
                    using (var poller = new NetMQPoller() { frontendSocket, this.FrontendBuffer })
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
                                    string serviceIdentifier = message.Metadata.TryGetLast("serviceIdentifier", out var encodedService) ? Encoding.UTF8.GetString(encodedService) : null;

                                    try
                                    {
                                        var cancellationSource = new CancellationTokenSource(5000);
                                        var registeredBackend = this.ResolveBackend(serviceIdentifier, cancellationSource.Token);
                                        //var registeredBackend = this.InprocTransportScope.GetNextServerTransport(serviceIdentifier, cancellationSource.Token);

                                        message.Metadata.Add($"envelope[{this.Identity}]", envelope);

                                        //registeredBackend.ReceiveBuffer.QueueMessage(message);

                                        registeredBackend.Send(message).ContinueWith(task =>
                                        {
                                            this.OnDiagnosticMessage($"Failed to forward to backend [{task.Exception.InnerException.Message}]");
                                        }, TaskContinuationOptions.OnlyOnFaulted);
                                    }
                                    catch (OperationCanceledException)
                                    {
                                        this.OnDiagnosticMessage($"No backends available for {serviceIdentifier}!!!");

                                        var nmqm = MessageHelpers.CreateNetMQErrorMessage(envelope, "No backends found", message.Metadata);
                                        e.Socket.SendMultipartMessage(nmqm);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.OnDiagnosticMessage(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        this.FrontendBuffer.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                while (this.FrontendBuffer.TryDequeue(out TransportMessage message, TimeSpan.Zero))
                                {
                                    if (!message.Metadata.TryPluckLast($"envelope[{this.Identity}]", out var envelope))
                                        throw new Exception("Message envelope not found");

                                    if (!frontendSocket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(envelope)))
                                    {
                                        this.OnDiagnosticMessage("Failed to forward to frontend");
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.OnDiagnosticMessage(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        monitor.Listening += (sender, e) =>
                        {
                            this.OnDiagnosticMessage($"Frontend router socket listening at {connectionString}");
                            isListening = true;
                        };
                        monitor.Closed += (sender, e) =>
                        {
                            this.OnDiagnosticMessage($"Frontend router socket closed on {connectionString}");
                            isListening = false;
                        };
                        monitor.Accepted += (sender, e) =>
                        {
                            this.OnDiagnosticMessage($"Frontend router socket connection accepted at {connectionString}");
                        };
                        monitor.Disconnected += (sender, e) =>
                        {
                            this.OnDiagnosticMessage($"Frontend router socket disconnected at {connectionString}");
                        };

                        this.OnDiagnosticMessage($"Attempting to bind frontend socket to {connectionString}");
                        monitor.StartAsync();
                        monitor.AttachToPoller(poller);

                        var pollerTask = new Task(poller.Run);
                        pollerTask.ContinueWith(task =>
                        {
                            var ex = task.Exception;

                            this.OnDiagnosticMessage(ex.Message + ": " + ex.StackTrace);
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
                    this.OnDiagnosticMessage(ex.Message + ": " + ex.StackTrace);
                }
            }
        }

        public void AddBackend(string identifier, InprocClientTransport backend)
        {
            this.Backends.GetOrAdd(identifier, (_) => new BlockingCollection<InprocClientTransport>()).Add(backend);

            backend.ReceiveStream.MessageEnqueued += async (sender, e) =>
            {
                var message = await backend.Receive();
                this.FrontendBuffer.Enqueue(message);
            };
        }

        private InprocClientTransport ResolveBackend(string identifier)
        {
            var backends = this.Backends.GetOrAdd(identifier, (_) => new BlockingCollection<InprocClientTransport>());

            var backend = backends.Take();
            while (!backend.IsConnected)
            {
                Console.WriteLine($"Inproc backend expired [{identifier}/{backend.Identity}]");
                backend = backends.Take();
            }

            backends.Add(backend);

            return backend;
        }
        private InprocClientTransport ResolveBackend(string identifier, CancellationToken cancellationToken)
        {
            var backends = this.Backends.GetOrAdd(identifier, (_) => new BlockingCollection<InprocClientTransport>());

            var backend = backends.Take(cancellationToken);
            while (!backend.IsConnected)
            {
                Console.WriteLine($"Inproc backend expired [{identifier}/{backend.Identity}]");
                backend = backends.Take(cancellationToken);
            }

            backends.Add(backend);

            return backend;
        }

        protected virtual void OnDiagnosticMessage(string message)
        {
            this.DiagnosticMessage?.Invoke(this, new DiagnosticMessageEventArgs(message));
        }
    }

    public struct InprocToZmqSplitterBackend
    {
        public string Identifier;
        public string Uri;
    }
    public class InprocToZmqSplitterDevice
    {
        public event EventHandler<DiagnosticMessageEventArgs> DiagnosticMessage;

        private string Identity { get; } = Guid.NewGuid().ToString("N").ToLowerInvariant();

        public InprocServerTransport FrontendTransport { get; }
        public InprocToZmqSplitterBackend[] DiscoverableBackends { get; }

        private ConcurrentDictionary<string, BlockingCollection<IClientTransport>> Backends { get; } = new ConcurrentDictionary<string, BlockingCollection<IClientTransport>>();

        public bool IsRunning { get; private set; }
        private Task HandlerTask { get; set; }

        public InprocToZmqSplitterDevice()
        {
            this.FrontendTransport = new InprocServerTransport();
        }
        public InprocToZmqSplitterDevice(InprocServerTransport frontendTransport)
        {
            this.FrontendTransport = frontendTransport;
        }

        public async Task Start()
        {
            if (!this.IsRunning)
            {
                this.IsRunning = true;
                //this.HandlerTask = Task.Factory.StartNew(() => this.Handler(), TaskCreationOptions.LongRunning);
                this.FrontendTransport.ReceiveStream.MessageEnqueued += this.FrontendTransportMessageEnqueued;
            }
        }
        public async Task Run()
        {
            await this.Start();
            //await this.HandlerTask;
        }
        public async Task Stop()
        {
            this.FrontendTransport.ReceiveStream.MessageEnqueued -= this.FrontendTransportMessageEnqueued;

            this.IsRunning = false;
            //await this.HandlerTask;
        }

        private async void FrontendTransportMessageEnqueued(object sender, EventArgs e)
        {
            var message = await this.FrontendTransport.Receive();
            string serviceIdentifier = message.Metadata.TryGetLast("serviceIdentifier", out var encodedService) ? Encoding.UTF8.GetString(encodedService) : null;

            try
            {
                var cancellationSource = new CancellationTokenSource(5000);
                var registeredBackend = this.ResolveBackend(serviceIdentifier, cancellationSource.Token);

                await registeredBackend.Send(message);
            }
            catch (OperationCanceledException)
            {
                this.OnDiagnosticMessage($"No backends available for {serviceIdentifier}!!!");
            }
        }
        private async void BackendMessageReceived(object sender, MessagingEventArgs e)
        {
            if (string.IsNullOrEmpty(e.Tag))
                await this.FrontendTransport.Send(e.Message);
            else
                await this.FrontendTransport.Send(e.Tag, e.Message);
        }

        //private async Task Handler()
        //{
        //    while (this.IsRunning)
        //    {
        //        //foreach (var discoverableBackend in this.DiscoverableBackends)
        //        //{
        //        //    var parsedUri = new Uri(discoverableBackend.Uri);
        //        //    switch (parsedUri.Scheme.ToLowerInvariant())
        //        //    {
        //        //        case "inproc":

        //        //            break;
        //        //        default:
        //        //            throw new Exception($"Unsupported discovery scheme {parsedUri.Scheme}");
        //        //    }
        //        //}
        //    }
        //}

        public void AddBackend(string identifier, IClientTransport backend)
        {
            this.Backends.GetOrAdd(identifier, (_) => new BlockingCollection<IClientTransport>()).Add(backend);

            backend.MessageReceiving += this.BackendMessageReceived;
        }
        public void AddBackend(string identifier, IClientTransport backend, CancellationToken cancellationToken)
        {
            this.Backends.GetOrAdd(identifier, (_) => new BlockingCollection<IClientTransport>()).Add(backend, cancellationToken);

            backend.MessageReceiving += this.BackendMessageReceived;
        }

        private IClientTransport ResolveBackend(string identifier, CancellationToken cancellationToken)
        {
            var backends = this.Backends.GetOrAdd(identifier, (_) => new BlockingCollection<IClientTransport>());

            var backend = backends.Take(cancellationToken);
            while (!backend.IsConnected)
            {
                Console.WriteLine($"Client backend expired [{identifier}/{backend.Identity}]");
                backend = backends.Take(cancellationToken);
            }

            backends.Add(backend);

            return backend;
        }

        protected virtual void OnDiagnosticMessage(string message)
        {
            this.DiagnosticMessage?.Invoke(this, new DiagnosticMessageEventArgs(message));
        }
    }
}
