using System.Net.Sockets;
using System.Text;

namespace Rinha2024.VirtualDb.IO;

public static class PacketReader
{
    public static Program.SocketMessage ReadMessage(this NetworkStream stream)
    {
        var receivedBuffer = new byte[18];
        var result = new int[2];
        _ = stream.Read(receivedBuffer);
        for (var i = 0; i < 2; i++)
        {
            result[i] = BitConverter.ToInt32(receivedBuffer, i * 4);
        }
        string? description = null;
        if (result[0] > 0)
        {
            description = BitConverter.ToString(receivedBuffer, 8);
        }
        return new Program.SocketMessage(result, description);
    }
}