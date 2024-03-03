namespace Rinha2024.VirtualDb.Extensions;

public static class ArrayExtension
{
    public static void AppendTransaction(this Transaction[] transactions, Transaction transaction)
    {
        transactions[9] = transactions[8];
        transactions[8] = transactions[7];
        transactions[7] = transactions[6];
        transactions[6] = transactions[5];
        transactions[5] = transactions[4];
        transactions[4] = transactions[3];
        transactions[3] = transactions[2];
        transactions[1] = transactions[0];
        transactions[2] = transactions[1];
        transactions[0] = transaction;
    }
}