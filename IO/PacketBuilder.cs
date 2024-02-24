namespace Rinha2024.Dotnet.IO;

public static class PacketBuilder
{
    
    public static byte[] WriteMessage(ref int[] message)
    {
        using var ms = new MemoryStream();
        foreach (var item in message)
        {
            ms.Write(BitConverter.GetBytes(item));
        }
        return ms.ToArray();
    }
    
}