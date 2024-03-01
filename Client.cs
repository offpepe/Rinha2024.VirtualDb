using System.Diagnostics;
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

    public int Value { get; set; }
    public int Limit { get; init; }
    public int FilledLenght;
    public Transaction[] Transactions { get; set; } = new Transaction[10];
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
};

public readonly record struct Transaction(int Value, char Type, string Description, string CreatedAt);

public class TransactionRequest(int[] parameters, string description, NetworkStream stream)
{
    public int[] Parameters { get; } = parameters;
    public string Description { get; } = description;
    private readonly Stream _stream = stream;
    public void SendResponse(int[] response) => _stream.Write(PacketBuilder.WriteMessage(response));
};

