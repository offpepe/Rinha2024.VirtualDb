﻿using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Sockets;
using Rinha2024.VirtualDb.IO;

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
    public Transaction[] Transactions { get; set; } = new Transaction[10];
    private readonly ConcurrentQueue<Transaction> _persistencyQueue = new();

    private SpinLock _lock;
    
    public (int, int) DoTransaction(int value, string description)
    {
        var locked = false;
        _lock.Enter(ref locked);
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
        if (locked) _lock.Exit();
        return (newBalance, Limit);
    }
    public void SetValue(int newBalance) => Value = newBalance;

    private void AddTransaction(Transaction transaction)
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
        _persistencyQueue.Enqueue(transaction);
    }

    private void PersistClientData()
    {
        using var fileStream  = File.OpenWrite($"./data/{Id}.capv");
        fileStream.Position = 4;
        var writeBuffer = new byte[4];
        var newValue = BitConverter.GetBytes(Value);
        writeBuffer[0] = newValue[0];
        writeBuffer[1] = newValue[1];
        writeBuffer[2] = newValue[2];
        writeBuffer[3] = newValue[3];
        fileStream.Write(writeBuffer);
    }
    
    private void PersistTransactionData()
    {
        while (true)
        {
            if (_persistencyQueue.IsEmpty || !_persistencyQueue.TryDequeue(out var transaction))
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
            var sizeBytes = BitConverter.GetBytes(transaction.Description.Length);
            buffer[6] = sizeBytes[0];
            buffer[7] = sizeBytes[1];
            buffer[8] = sizeBytes[2];
            buffer[9] = sizeBytes[3];
            var pos = 9;
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

