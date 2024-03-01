namespace Rinha2024.VirtualDb.IO;

public static class PacketBuilder
{
    
    public static byte[] WriteMessage(int[] message)
    {
        using var ms = new MemoryStream(8);
        foreach (var item in message)
        {
            ms.Write(BitConverter.GetBytes(item));
        }
        return ms.ToArray();
    }
    
    public static byte[] WriteMessage(int[] message, Transaction[] transactions, int length)
    {
        using var ms = new MemoryStream(356);
        foreach (var item in message)
        {
            ms.Write(BitConverter.GetBytes(item));
        }
        ms.Write(BitConverter.GetBytes(length));
        if (length == 0) return ms.ToArray();
        foreach (var transaction in transactions)
        {
            if (transaction.Value == 0) break;
            ms.Write(BitConverter.GetBytes(transaction.Value));
            ms.Write(BitConverter.GetBytes(transaction.Type));
            ms.Write(BitConverter.GetBytes(transaction.Description.Length));
            foreach (var c in transaction.Description)  
            {
                ms.Write(BitConverter.GetBytes(c));
            }
            ms.Write(BitConverter.GetBytes(transaction.CreatedAt.ToBinary()));
        }
        return ms.ToArray();
    }
    
}