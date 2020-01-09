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
    internal class RegisteredBackend
    {
        public byte[] Envelope { get; set; }
        public DateTime RegsiteredDate { get; set; }

        public RegisteredBackend(byte[] envelope, DateTime registeredDate)
        {
            this.Envelope = envelope;
            this.RegsiteredDate = registeredDate;
        }
    }

    public interface IRouterDevice
    {
        string Identity { get; }

        IZeroMQServerEndpoint FrontendEndpoint { get; }
        IZeroMQServerEndpoint BackendEndpoint { get; }
    }

    public class RouterDevice : IRouterDevice
    {
        private readonly string identity;
        public string Identity
        {
            get
            {
                return identity;
            }
        }

        private readonly IZeroMQServerEndpoint frontendEndpoint;
        public IZeroMQServerEndpoint FrontendEndpoint => frontendEndpoint;

        private readonly IZeroMQServerEndpoint backendEndpoint;
        public IZeroMQServerEndpoint BackendEndpoint => backendEndpoint;

        private readonly IAnnouncer frontendAnnouncer;
        public IAnnouncer FrontendAnnouncer => frontendAnnouncer;

        private readonly IAnnouncer backendAnnouncer;
        public IAnnouncer BackendAnnouncer => backendAnnouncer;

        private readonly ConcurrentQueue<string> registeredBackendIdentifiers;
        private ConcurrentQueue<string> RegisteredBackendIdentifiers => registeredBackendIdentifiers;

        private readonly ConcurrentDictionary<string, RegisteredBackend> registeredBackends;
        private ConcurrentDictionary<string, RegisteredBackend> RegisteredBackends => registeredBackends;

        //private readonly ConcurrentQueue<Message> frontendBuffer;
        //private ConcurrentQueue<Message> FrontendBuffer => frontendBuffer;

        //private readonly ConcurrentQueue<Message> backendBuffer;
        //private ConcurrentQueue<Message> BackendBuffer => backendBuffer;

        //private RouterSocket FrontendSocket { get; set; }
        //private RouterSocket BackendSocket { get; set; }

        private bool IsRunning { get; set; }
        private Task FrontendTask { get; set; }
        private Task BackendTask { get; set; }
        private NetMQQueue<TransportMessage> AltFrontendBuffer { get; set; }
        private NetMQQueue<TransportMessage> AltBackendBuffer { get; set; }

        private int sendCount = 0;
        private int receiveCount = 0;

        public RouterDevice(IZeroMQServerEndpoint frontendEndpoint, IZeroMQServerEndpoint backendEndpoint)
            : base()
        {
            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.frontendEndpoint = frontendEndpoint;
            this.backendEndpoint = backendEndpoint;

            this.registeredBackendIdentifiers = new ConcurrentQueue<string>();
            this.registeredBackends = new ConcurrentDictionary<string, RegisteredBackend>();
        }
        public RouterDevice(IZeroMQServerEndpoint frontendEndpoint, IZeroMQServerEndpoint backendEndpoint, IAnnouncer frontendAnnouncer, IAnnouncer backendAnnouncer)
            : base()
        {
            this.identity = Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();

            this.frontendEndpoint = frontendEndpoint;
            this.backendEndpoint = backendEndpoint;

            this.frontendAnnouncer = frontendAnnouncer;
            this.backendAnnouncer = backendAnnouncer;

            this.registeredBackendIdentifiers = new ConcurrentQueue<string>();
            this.registeredBackends = new ConcurrentDictionary<string, RegisteredBackend>();

            //this.frontendBuffer = new ConcurrentQueue<Message>();
            //this.backendBuffer = new ConcurrentQueue<Message>();
        }

        public Task Listen()
        {
            if (!this.IsRunning)
            {
                this.IsRunning = true;

                this.FrontendTask = Task.Factory.StartNew(() => this.FrontendHandler(), TaskCreationOptions.LongRunning).Unwrap();
                this.BackendTask = Task.Factory.StartNew(() => this.BackendHandler(), TaskCreationOptions.LongRunning).Unwrap();
            }

            return Task.FromResult(false);
        }
        public async Task Close()
        {
            this.IsRunning = false;

            await Task.WhenAll(this.FrontendTask, this.BackendTask);
        }

        private async Task FrontendHandler()
        {
            while (this.IsRunning)
            {
                try
                {
                    bool isListening = false;
                    var connectionString = this.FrontendEndpoint.ToConnectionString();

                    using (var frontendSocket = new RouterSocket())
                    using (this.AltFrontendBuffer = new NetMQQueue<TransportMessage>())
                    using (var poller = new NetMQPoller() { frontendSocket, this.AltFrontendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(frontendSocket, $"inproc://monitor.routerdevice.frontend.{Guid.NewGuid().ToString()}", SocketEvents.Listening | SocketEvents.Accepted | SocketEvents.Disconnected | SocketEvents.Closed))
                    {
                        frontendSocket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                var netmqMessage = new NetMQMessage();
                                while (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    //Console.WriteLine("Frontend message received");

                                    var message = netmqMessage.ToMessage(out var envelope);

                                    //this.receiveCount++;
                                    //Console.WriteLine("RECEIVED " + this.receiveCount);

                                    //var sourceEnvelope = message.Envelope;

                                    RegisteredBackend registeredBackend = null;
                                    var forwardStart = DateTime.UtcNow;
                                    while (this.IsRunning && registeredBackend == null)
                                    {
                                        while (this.RegisteredBackendIdentifiers.TryDequeue(out string backendIdentifier))
                                        {
                                            registeredBackend = this.RegisteredBackends[backendIdentifier];

                                            if ((DateTime.UtcNow - registeredBackend.RegsiteredDate).TotalMilliseconds < 2000)
                                            {
                                                this.RegisteredBackendIdentifiers.Enqueue(backendIdentifier);
                                                break;
                                            }
                                            else
                                            {
                                                if (this.RegisteredBackends.TryRemove(backendIdentifier, out RegisteredBackend expiredBackend))
                                                    Console.WriteLine($"Backend {backendIdentifier} expired");
                                            }
                                        }

                                        if (registeredBackend != null || (DateTime.Now - forwardStart).TotalMilliseconds > 30000)
                                        {
                                            break;
                                        }

                                        System.Threading.Thread.Sleep(100);
                                    }

                                    if (registeredBackend != null)
                                    {
                                        //Console.WriteLine($"Forwarding to [ {BitConverter.ToString(registeredBackend.Envelope)} ]");

                                        message.Metadata.Add($"envelope[{this.Identity}]", envelope);
                                        message.Metadata.Add($"backendEnvelope[{this.Identity}]", registeredBackend.Envelope);

                                        this.AltBackendBuffer.Enqueue(message);

                                        ////this.BackendSocket.SendMultipartMessage(message.ToNetMQMessage(true));
                                        //if (!this.BackendSocket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                                        //{
                                        //    Console.WriteLine("Failed to forward to backend");

                                        //    var forwardedMessage = new Message(1, message.Frames, System.Text.Encoding.UTF8.GetBytes("Failed to forward to backend"), sourceEnvelope);
                                        //    this.FrontendSocket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
                                        //}
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

                        //var lastFlushed = DateTime.UtcNow;
                        //frontendSocket.SendReady += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.BackendBuffer.IsEmpty)
                        //        {
                        //            while (this.BackendBuffer.TryDequeue(out Message message))
                        //            {
                        //                Console.WriteLine("Backend forwarding");

                        //                if (!e.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to forward to frontend");

                        //                    //var forwardedMessage = new Message(1, message.Frames, System.Text.Encoding.UTF8.GetBytes("Failed to forward to backend"), sourceEnvelope);
                        //                    //this.FrontendSocket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
                        //                }
                        //            }

                        //            lastFlushed = DateTime.UtcNow;
                        //        }

                        //        if ((DateTime.UtcNow - lastFlushed).TotalSeconds > 1)
                        //            Task.Delay(1).Wait();
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                        //    }
                        //};
                        this.AltFrontendBuffer.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                while (this.AltFrontendBuffer.TryDequeue(out TransportMessage message, TimeSpan.Zero))
                                {
                                    if (!message.Metadata.TryPluck($"envelope[{this.Identity}]", out var envelope))
                                        throw new Exception("Message envelope not found");

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
                            if (this.FrontendAnnouncer != null)
                            {
                                //Console.WriteLine("Registering frontend");
                                await this.FrontendAnnouncer.Register(this.FrontendEndpoint);
                            }

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
        private async Task BackendHandler()
        {
            while (this.IsRunning)
            {
                try
                {
                    while (this.RegisteredBackendIdentifiers.TryDequeue(out string identifier)) { }

                    bool isListening = false;
                    var connectionString = this.BackendEndpoint.ToConnectionString();

                    using (var backendSocket = new RouterSocket())
                    using (this.AltBackendBuffer = new NetMQQueue<TransportMessage>())
                    using (var poller = new NetMQPoller() { backendSocket, this.AltBackendBuffer })
                    using (var monitor = new NetMQ.Monitoring.NetMQMonitor(backendSocket, $"inproc://monitor.routerdevice.backend.{Guid.NewGuid().ToString()}", SocketEvents.Listening | SocketEvents.Disconnected | SocketEvents.Closed | SocketEvents.BindFailed))
                    {
                        backendSocket.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                //Console.WriteLine("Backend message received");
                                //Console.WriteLine($"RECEIVED BACKEND ({this.BackendSocket.HasIn}) ({this.BackendSocket.HasOut})");

                                var netmqMessage = new NetMQMessage();
                                while (e.Socket.TryReceiveMultipartMessage(ref netmqMessage))
                                {
                                    var message = netmqMessage.ToMessage(out var envelope);

                                    //this.BackendBuffer.Enqueue(message);

                                    if (message.Metadata.TryPluck("Greeting", out var encodedGreeting))
                                    {
                                        var identifier = BitConverter.ToString(envelope);

                                        if (this.RegisteredBackends.TryAdd(identifier, new RegisteredBackend(envelope, DateTime.UtcNow)))
                                        {
                                            Console.WriteLine($"Backend registered [ {identifier} ]");
                                            this.RegisteredBackendIdentifiers.Enqueue(identifier);
                                        }
                                        else
                                        {
                                            this.RegisteredBackends[identifier].RegsiteredDate = DateTime.UtcNow;
                                        }
                                    }
                                    else
                                    {
                                        this.AltFrontendBuffer.Enqueue(message);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                            }
                        };

                        //var lastFlushed = DateTime.UtcNow;
                        //backendSocket.SendReady += (sender, e) =>
                        //{
                        //    try
                        //    {
                        //        if (!this.FrontendBuffer.IsEmpty)
                        //        {
                        //            while (this.FrontendBuffer.TryDequeue(out Message message))
                        //            {
                        //                Console.WriteLine("Frontend forwarding");

                        //                if (!e.Socket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(true)))
                        //                {
                        //                    Console.WriteLine("Failed to forward to backend");

                        //                    //var forwardedMessage = new Message(1, message.Frames, System.Text.Encoding.UTF8.GetBytes("Failed to forward to backend"), sourceEnvelope);
                        //                    //this.FrontendSocket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
                        //                }
                        //            }

                        //            lastFlushed = DateTime.UtcNow;
                        //        }

                        //        if ((DateTime.UtcNow - lastFlushed).TotalSeconds > 1)
                        //            Task.Delay(1).Wait();
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        Console.WriteLine(ex.Message + ": " + ex.StackTrace);
                        //    }
                        //};
                        this.AltBackendBuffer.ReceiveReady += (sender, e) =>
                        {
                            try
                            {
                                while (this.AltBackendBuffer.TryDequeue(out TransportMessage message, TimeSpan.Zero))
                                {
                                    if (!message.Metadata.TryPluck($"backendEnvelope[{this.Identity}]", out var envelope))
                                        throw new Exception("Message backend envelope not found");

                                    if (!backendSocket.TrySendMultipartMessage(TimeSpan.FromSeconds(1), message.ToNetMQMessage(envelope)))
                                    {
                                        Console.WriteLine("Failed to forward to backend");

                                        //var forwardedMessage = new Message(1, message.Frames, System.Text.Encoding.UTF8.GetBytes("Failed to forward to backend"), sourceEnvelope);
                                        //this.FrontendSocket.SendMultipartMessage(forwardedMessage.ToNetMQMessage(true));
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
                            Console.WriteLine($"Backend router socket listening at {connectionString}");
                            isListening = true;
                        };
                        monitor.Closed += (sender, e) =>
                        {
                            Console.WriteLine($"Backend router socket accepted connection on {connectionString}");
                            isListening = false;
                        };
                        monitor.Disconnected += (sender, e) =>
                        {
                            Console.WriteLine($"Backend router socket disconnected at {connectionString}");
                        };
                        monitor.BindFailed += (sender, e) =>
                        {
                            Console.WriteLine($"Backend router bind failed {connectionString}");
                        };

                        Console.WriteLine($"Attempting to bind backend socket to {connectionString}");
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

                        backendSocket.Bind(connectionString);

                        var start = DateTime.Now;
                        while (!isListening)
                        {
                            if ((DateTime.Now - start).TotalMilliseconds > 5000)
                                throw new Exception($"Backend socket bind timeout ({connectionString})");

                            await Task.Delay(1000);
                        }

                        while (this.IsRunning && isListening)
                        {
                            if (this.BackendAnnouncer != null)
                            {
                                //Console.WriteLine("Registering backend");
                                await this.BackendAnnouncer.Register(this.BackendEndpoint);
                            }

                            await Task.Delay(1000);
                        }

                        poller.StopAsync();
                        backendSocket.Disconnect(connectionString);
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
    }
}
