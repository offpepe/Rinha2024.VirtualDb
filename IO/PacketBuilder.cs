namespace Rinha2024.VirtualDb.IO;

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
    
    public static byte[] WriteMessage(int[] message, Transaction[] transactions, int length)
    {
        using var ms = new MemoryStream();
        foreach (var item in message)
        {
            ms.Write(BitConverter.GetBytes(item));
        }
        ms.Write(BitConverter.GetBytes(length));
        foreach (var transaction in transactions)
        {
            if (transaction.Value == 0) break;
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