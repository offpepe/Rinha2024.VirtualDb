using System.Net.Sockets;

namespace Rinha2024.VirtualDb;

public static class PacketReader
{
    public static int[] ReadMessage(this NetworkStream stream)
    {
        var receivedBuffer = new byte[8];
        var result = new int[2];
        _ = stream.Read(receivedBuffer);
        for (var i = 0; i < 2; i++)
        {
            result[i] = BitConverter.ToInt32(receivedBuffer, i * 4);
        }
        return result;
    }
}