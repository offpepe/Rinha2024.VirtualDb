using Rinha2024.VirtualDb;

namespace Rinha2024.Dotnet.IO;

public static class PacketBuilder
{
    
    public static byte[] WriteMessage(int[] message)
    {
        using var ms = new MemoryStream();
        foreach (var item in message)
        {
            ms.Write(BitConverter.GetBytes(item));
        }
        return ms.ToArray();
    }
    
    public static byte[] WriteMessage(int[] message, LinkedList<Program.Transaction> transactions)
    {
        using var ms = new MemoryStream();
        foreach (var item in message)
        {
            ms.Write(BitConverter.GetBytes(item));
        }
        ms.Write(BitConverter.GetBytes(transactions.Count()));
        foreach (var transaction in transactions)
        {
            ms.Write(BitConverter.GetBytes(transaction.Value));
            ms.Write(BitConverter.GetBytes(transaction.Type));
            foreach (var c in transaction.Description)
            {
                ms.Write(BitConverter.GetBytes(c));
            }
            foreach (var c in transaction.CreatedAt)
            {   
                ms.Write(BitConverter.GetBytes(c));
            }
            
        }
        return ms.ToArray();
    }
    
}