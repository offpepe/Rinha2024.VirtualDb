using System.Collections.Concurrent;
using System.Net.Sockets;
using Rinha2024.VirtualDb.Extensions;

namespace Rinha2024.VirtualDb;

public class Client
{
    public Client(int value, int limit, int id)
    {
        Value = value;
        Limit = limit;
        Id = id;
        new Thread(PersistTransactionData).Start();
    }
    public int Id { get; init; }
    public int Value { get; set; }
    public int Limit { get; set; }
    
    public int FilledLenght;
    public Transaction[] Transactions { get; } = new Transaction[10];
    private readonly ConcurrentQueue<Transaction> _transactionIOQueue = new();
    private FileStream? _fileStream;

    private SpinLock _lock;
    
    public (int, int) DoTransaction(int value, string description)
    {
        var locked = false;
        _lock.Enter(ref locked);
        try
        {
            if (!locked) return (0, -1);
            var newBalance = Value + value;
            var isDebit = value < 0;
            if (isDebit && -newBalance > Limit)
            {
                return (0, -1);
            }
            Value = newBalance;
            PersistClientData();
            AddTransaction(new Transaction(Math.Abs(value), isDebit ? 'd' : 'c', description, DateTime.Now));
            return (newBalance, Limit);
        }
        finally
        {
            _lock.Exit();
        }
    }
    

    private void AddTransaction(Transaction transaction)
    {
       Transactions.AppendTransaction(transaction);
        if (FilledLenght < 10) FilledLenght++;
        _transactionIOQueue.Enqueue(transaction);
    }
    

    private void PersistClientData()
    {
        _fileStream ??= File.OpenWrite($"./data/{Id}.capv");
        _fileStream.Position = 4;
        var writeBuffer = new byte[4];
        var newValue = BitConverter.GetBytes(Value);
        writeBuffer[0] = newValue[0];
        writeBuffer[1] = newValue[1];
        writeBuffer[2] = newValue[2];
        writeBuffer[3] = newValue[3];
        _fileStream.Write(writeBuffer);
    }
    
    private void PersistTransactionData()
    {
        while (true)
        {
            if (_transactionIOQueue.IsEmpty || !_transactionIOQueue.TryDequeue(out var transaction))
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
                continue;
            }
            var size = 18 + transaction.Description.Length * 2;
            using var transactionFile = File.Open($"./data/{Id}-transactions.capv", FileMode.Append);
            var buffer = new byte[size];
            var valuebytes = BitConverter.GetBytes(transaction.Value);
            buffer[0] = valuebytes[0];
            buffer[1] = valuebytes[1];
            buffer[2] = valuebytes[2];
            buffer[3] = valuebytes[3];
            var typeBytes = BitConverter.GetBytes(transaction.Type);
            buffer[4] = typeBytes[0];
            buffer[5] = typeBytes[1];
            var sizedesc = transaction.Description.Length;
            var sizeBytes = BitConverter.GetBytes(sizedesc);
            buffer[6] = sizeBytes[0];
            buffer[7] = sizeBytes[1];
            buffer[8] = sizeBytes[2];
            buffer[9] = sizeBytes[3];
            var pos = 10;
            foreach (var c in transaction.Description)
            {
                var descriptionBytes = BitConverter.GetBytes(c);
                buffer[pos] = descriptionBytes[0];
                pos++;
                buffer[pos] = descriptionBytes[1];
                pos++;
            }

            var dateBytes = BitConverter.GetBytes(transaction.CreatedAt.ToBinary());
            buffer[pos] = dateBytes[0];
            pos++;
            buffer[pos] = dateBytes[1];
            pos++;
            buffer[pos] = dateBytes[2];
            pos++;
            buffer[pos] = dateBytes[3];
            pos++;
            buffer[pos] = dateBytes[4];
            pos++;
            buffer[pos] = dateBytes[5];
            pos++;
            buffer[pos] = dateBytes[6];
            pos++;
            buffer[pos] = dateBytes[7];
            transactionFile.Write(buffer);
        }
    }
};

public readonly record struct Transaction(int Value, char Type, string Description, DateTime CreatedAt);

public class TransactionRequest(int[] parameters, string description, NetworkStream stream)
{
    public int[] Parameters { get; } = parameters;
    public string Description { get; } = description;
};

