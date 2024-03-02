using System.Net.Sockets;
using Rinha2024.VirtualDb.IO;

namespace Rinha2024.VirtualDb;

public class Client
{
    public Client(int value, int limit)
    {
        Value = value;
        Limit = limit;
    }

    public int Value { get; private set; }
    public int Limit { get; init; }
    
    public int FilledLenght;
    public Transaction[] Transactions { get; set; } = new Transaction[10];

    private SpinLock _lock = new SpinLock();
    
    public int[] DoTransaction(int value, string description)
    {
        try
        {
            var locked = false;
            _lock.Enter(ref locked);
            var newBalance = Value + value;
            var isDebit = value < 0;
            if (isDebit && -newBalance > Limit)
            {
                return [0, -1];
            }
            Value = newBalance;
            AddTransaction(new Transaction(isDebit ? -value : value, isDebit ? 'd' : 'c', description, DateTime.Now));
            return [newBalance, Limit];
        }
        finally
        {
            _lock.Exit();
        }
    }
    public void SetValue(int newBalance) => Value = newBalance;
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
};

public readonly record struct Transaction(int Value, char Type, string Description, DateTime CreatedAt);

public class TransactionRequest(int[] parameters, string description, NetworkStream stream)
{
    public int[] Parameters { get; } = parameters;
    public string Description { get; } = description;
    public void SendResponse(int[] response) => stream.Write(PacketBuilder.WriteMessage(response));
};

