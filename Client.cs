namespace Rinha2024.VirtualDb;

public class Client
{
    public Client(int value, int limit)
    {
        Value = value;
        Limit = limit;
    }

    public int Value { get; set; }
    public int Limit { get; set; }
    public Transaction[] Transactions { get; set; } = new Transaction[10];
    public int FilledLenght = 0;
    public void SetValue(int newBalance) => this.Value = newBalance;

    public void AddTransaction(Transaction transaction)
    {
        Transactions[9] = Transactions[8];
        Transactions[8] = Transactions[7];
        Transactions[7] = Transactions[6];
        Transactions[6] = Transactions[5];
        Transactions[5] = Transactions[4];
        Transactions[4] = Transactions[3];
        Transactions[3] = Transactions[2];
        Transactions[2] = Transactions[1];
        Transactions[1] = Transactions[0];
        Transactions[0] = transaction;
        if (FilledLenght < 10) FilledLenght++;
    }

    public IEnumerable<Transaction> GetTransactions(int count)
    {
        for (var i = 0; i < count; i++) yield return Transactions.ElementAt(i);
    }
};

public readonly record struct Transaction(int Value, char Type, string Description, string CreatedAt);

public readonly record struct TransactionRequest(Guid Id, int[] Parameters, string Description);

