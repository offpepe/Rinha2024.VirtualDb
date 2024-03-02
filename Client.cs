using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Text;
using Rinha2024.VirtualDb.Extensions;

namespace Rinha2024.VirtualDb;

[SuppressMessage("ReSharper", "FunctionNeverReturns")]
public class Client
{
    public Client(int value, int limit, int id)
    {
        Value = value;
        Limit = limit;
        Id = id;
        var storageFolder = Environment.GetEnvironmentVariable("storage_folder") ?? "./data";
        _storagePath = $"{storageFolder}/{id}.capv";
        _transactionStoragePath = $"{storageFolder}/{id}-transactions.capv";
        if (File.Exists(_storagePath))
        {
            GetDiskStorageData();
        }
        else
        {
            CreateDiskStorageData();
        }
        new Thread(CreateTransactionRoutine).Start();
        new Thread(UpdateRoutine).Start();
    }
    public int Id { get; init; }
    public int Value { get; set; }
    public int Limit { get; set; }
    
    public int FilledLenght;
    public Transaction[] Transactions { get; } = new Transaction[10];
    private readonly ConcurrentQueue<Transaction> _transactionIoQueue = new();
    private readonly ConcurrentQueue<int> _clientIoQueue = new();

    private readonly string _storagePath;
    private readonly string _transactionStoragePath;
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
            _clientIoQueue.Enqueue(newBalance);
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
        _transactionIoQueue.Enqueue(transaction);
    }
    

    private void UpdateRoutine()
    {
        var value = 0;
        var writeBuffer = new byte[4];
        FileStream? stream = null;
        while (true)
        {
            if (_clientIoQueue.IsEmpty || !_clientIoQueue.TryDequeue(out value))
            {
                Thread.Sleep(1);  
                continue;
            }
            stream ??= File.OpenWrite(_storagePath);
            stream.Position = 4;
            var newValue = BitConverter.GetBytes(value);
            writeBuffer[0] = newValue[0];
            writeBuffer[1] = newValue[1];
            writeBuffer[2] = newValue[2];
            writeBuffer[3] = newValue[3];
            stream.Write(writeBuffer);
            if (!_clientIoQueue.IsEmpty) continue;
            stream.Close();
            stream.Dispose();
            stream = null;
        }
    }
    
    private void CreateTransactionRoutine()
    {
        FileStream? transactionStream = null; 
        while (true)
        {
            if (_transactionIoQueue.IsEmpty || !_transactionIoQueue.TryDequeue(out var transaction))
            {
                Thread.Sleep(1);
                continue;
            }
            transactionStream ??= File.Open(_transactionStoragePath, FileMode.Append);
            var size = 18 + transaction.Description.Length * 2;
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
            transactionStream.Write(buffer);
            if (!_transactionIoQueue.IsEmpty) continue;
            transactionStream.Close();
            transactionStream.Dispose();
            transactionStream = null;
        }
    }

    private void CreateDiskStorageData()
    {
        using var clientStream = File.Create(_storagePath);
        using var transactionStream = File.Create(_transactionStoragePath);
        var writeBuffer = new byte[8];
        var limitBytes = BitConverter.GetBytes(Limit);
        writeBuffer[0] = limitBytes[0];
        writeBuffer[1] = limitBytes[1];
        writeBuffer[2] = limitBytes[2];
        writeBuffer[3] = limitBytes[3];
        var valueBytes = BitConverter.GetBytes(Value);
        writeBuffer[4] = valueBytes[0];
        writeBuffer[5] = valueBytes[1];
        writeBuffer[6] = valueBytes[2];
        writeBuffer[7] = valueBytes[3];
        clientStream.Write(writeBuffer);
    }
    
    private void GetDiskStorageData()
    { 
        using var readStream = File.OpenRead(_storagePath);
        if (readStream.Length != 8)
        {
            CreateDiskStorageData();
            return;
        }
        var buffer = new byte[8];   
        _ = readStream.Read(buffer);
        Limit = BitConverter.ToInt32(buffer, 0);
        Value = BitConverter.ToInt32(buffer, 4);
        if (!File.Exists(_transactionStoragePath))
        {
            using var stream = File.Create(_transactionStoragePath);
            return;
        }
        using var transactionReadStream = File.OpenRead(_transactionStoragePath);
        if (transactionReadStream.Length == 0) return; 
        var transactionBuffer = new byte[transactionReadStream.Length];
        _ = transactionReadStream.Read(transactionBuffer);
        var position = 0;
        for (var i = 0; i < 10; i++)
        {
            var value = BitConverter.ToInt32(transactionBuffer, position);
            position += 4;
            var type = BitConverter.ToChar(transactionBuffer, position);
            position += 2;
            var size = BitConverter.ToInt32(transactionBuffer, position);
            position += 4;
            var description = new StringBuilder(size);
            for (var j = 0; j < size; j++)
            {
                description.Append(BitConverter.ToChar(transactionBuffer, position));
                position += 2;
            }
            var createdAt = BitConverter.ToInt64(transactionBuffer, position);
            position += 8;
            Transactions.AppendTransaction(new Transaction(value, type, description.ToString(), DateTime.FromBinary(createdAt)));
            FilledLenght++;
            if (position == transactionReadStream.Length) break;
        }
    }
};

public readonly record struct Transaction(int Value, char Type, string Description, DateTime CreatedAt);

