using System.Net.Sockets;
using System.Text;

namespace Rinha2024.VirtualDb.IO;

public static class PacketReader
{
    public static int[] ReadMessage(this NetworkStream stream)
    {
        var receivedBuffer = new byte[35];
        var result = new int[2];
        _ = stream.Read(receivedBuffer);
        result[0] = BitConverter.ToInt32(receivedBuffer, 0);
        result[1] = BitConverter.ToInt32(receivedBuffer, 4);
        return result;
    }
    public static (int[], string) ReadWriteMessage(this NetworkStream stream)
    {
        var receivedBuffer = new byte[35];
        var result = new int[2];
        _ = stream.Read(receivedBuffer);
        var pos = 0;
        for (var i = 0; i < 2; i++)
        {
            result[i] = BitConverter.ToInt32(receivedBuffer, pos);
            pos += 4;
        }
        var size = BitConverter.ToInt32(receivedBuffer, pos);
        pos += 4;
        var builder = new StringBuilder(size);
        for (var i = 0; i < size; i++)
        {
            builder.Append(BitConverter.ToChar(receivedBuffer, pos));
            pos += 2;
        }
        return (result, builder.ToString());
    }
    
    public static byte ReadOpt(this NetworkStream stream)
    {
        var receivedBuffer = new byte[1];
        _ = stream.Read(receivedBuffer);
        return receivedBuffer[0];
    }
}