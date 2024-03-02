using System.Net;
using System.Net.Sockets;

namespace Rinha2024.VirtualDb.Extensions;

public static class SocketExtensions
{
    public static void Configure(this TcpListener listener)
    {
        listener.ExclusiveAddressUse = true;
        listener.Server.NoDelay = true;
        listener.Server.Ttl = 255;
        listener.Server.Ttl = 255;
    }

    public static int GetPort(this TcpListener listener)
        => ((IPEndPoint) listener.LocalEndpoint).Port;
}