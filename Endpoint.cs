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
    public interface IZeroMQEndpoint : IEndpoint
    {
        string ToConnectionString();
        string Serialize();
    }

    public interface IZeroMQServerEndpoint : IZeroMQEndpoint, IServerEndpoint, IEncodableEndpoint
    {
    }

    public abstract class AZeroMQServerEndpoint : AServerEndpoint, IZeroMQServerEndpoint
    {
        public abstract string ToConnectionString();
        public abstract string Serialize();

        public byte[] Encode()
        {
            return System.Text.Encoding.UTF8.GetBytes(this.Serialize());
        }
    }

    public class TcpServerEndpoint : AZeroMQServerEndpoint
    {
        public static TcpServerEndpoint Allocate(string interfaceName = null)
        {
            // string address;

            // var iface = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
            //     .SingleOrDefault(i => i.Name == interfaceName);
            // if (iface == null)
            //     throw new Exception($"Interface '{interfaceName}' does not exist");
            // if (iface.OperationalStatus != System.Net.NetworkInformation.OperationalStatus.Up)
            //     throw new Exception($"Interface '{interfaceName}' is not connected");

            // var ipAddresses = iface.GetIPProperties().UnicastAddresses;
            // var ipv4Address = ipAddresses.FirstOrDefault(addr => addr.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
            // if (ipv4Address == null)
            //     throw new Exception($"Interface '{interfaceName}' does not have a valid IPv4 address");

            // address = ipv4Address.Address.ToString();

            var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            listener.Start();
            var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();

            // return new TcpServerEndpoint(port, address);
            return new TcpServerEndpoint(port, System.Net.IPAddress.Loopback.ToString());
        }

        private readonly string hostname;
        public string Hostname
        {
            get
            {
                return this.hostname;
            }
        }

        private readonly int port;
        public int Port
        {
            get
            {
                return this.port;
            }
        }

        public TcpServerEndpoint(int port)
            : base()
        {
            this.port = port;
        }
        public TcpServerEndpoint(int port, string hostname)
            : base()
        {
            this.port = port;
            this.hostname = hostname;
        }

        public override string ToConnectionString()
        {
            return $"tcp://{(!string.IsNullOrEmpty(this.Hostname) ? this.Hostname : "*")}:{this.Port}";
        }

        public override string Serialize()
        {
            if (string.IsNullOrEmpty(this.Hostname))
                throw new Exception("Cannot encode TCP endpoint; hostname required");

            return $"tcp|{this.Hostname}|{this.Port}";
        }
    }

    public class InprocServerEndpoint : AZeroMQServerEndpoint, IZeroMQEndpoint
    {
        private readonly string identifier;
        public string Identifier
        {
            get
            {
                return this.identifier;
            }
        }

        public InprocServerEndpoint(string identifier)
            : base()
        {
            this.identifier = identifier;
        }

        public override string ToConnectionString()
        {
            return $"inproc://{this.Identifier}";
        }

        public override string Serialize()
        {
            return $"inproc|{this.Identifier}";
        }
    }

    public interface IZeroMQClientEndpoint : IZeroMQEndpoint, IClientEndpoint, IZeroMQServerEndpoint
    {
    }

    public abstract class AZeroMQClientEndpoint : AClientEndpoint, IZeroMQClientEndpoint
    {
        public abstract string ToConnectionString();
        public abstract string Serialize();

        public byte[] Encode()
        {
            return System.Text.Encoding.UTF8.GetBytes(this.Serialize());
        }
    }


    public class TcpClientEndpoint : AZeroMQClientEndpoint, IZeroMQEndpoint
    {
        private readonly string hostname;
        public string Hostname
        {
            get
            {
                return this.hostname;
            }
        }

        private readonly int port;
        public int Port
        {
            get
            {
                return this.port;
            }
        }

        public TcpClientEndpoint(string hostname, int port)
            : base()
        {
            this.hostname = hostname;
            this.port = port;
        }

        public override string ToConnectionString()
        {
            return $"tcp://{this.Hostname}:{this.Port}";
        }

        public override string Serialize()
        {
            return $"tcp|{this.Hostname}|{this.Port}";
        }
    }

    public class InprocClientEndpoint : AZeroMQClientEndpoint, IZeroMQEndpoint
    {
        private readonly string identifier;
        public string Identifier
        {
            get
            {
                return this.identifier;
            }
        }

        public InprocClientEndpoint(string identifier)
            : base()
        {
            this.identifier = identifier;
        }

        public override string ToConnectionString()
        {
            return $"inproc://{this.Identifier}";
        }

        public override string Serialize()
        {
            return $"inproc|{this.Identifier}";
        }
    }

    // public class ServerEndpointEncoder : AEndpointEncoder<IZeroMQServerEndpoint>
    // {
    //     public override byte[] Encode(IZeroMQServerEndpoint endpoint)
    //     {
    //         return System.Text.Encoding.UTF8.GetBytes(endpoint.Serialize());
    //     }
    // }

    public class ClientEndpointDecoder : AEndpointDecoder<IZeroMQClientEndpoint>
    {
        public override IZeroMQClientEndpoint Decode(byte[] payload)
        {
            var decodedPayload = System.Text.Encoding.UTF8.GetString(payload);
            var facets = decodedPayload.Split('|');

            switch (facets[0])
            {
                case "tcp":
                    return new TcpClientEndpoint(facets[1], int.Parse(facets[2]));
                case "inproc":
                    return new InprocClientEndpoint(facets[1]);
                default:
                    throw new Exception($"Unknown endpoint type {facets[0]}");
            }
        }
    }
}