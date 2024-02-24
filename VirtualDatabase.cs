﻿using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Rinha2024.VirtualDb;

public class VirtualDatabase
{
    private int[] _unprocessable = [0, -1];
    private readonly int[][] _clients =
    [
        [0, 0],
        [0, 100000],
        [0, 80000],
        [0, 1000000],
        [0, 10000000],
        [0, 500000],
    ];
    public int Size => _clients.Length;
    public ref int[] GetClient(ref int idx) => ref _clients[idx];


    public int[] DoTransaction(ref int idx, ref int value)
    {
        var client = GetClient(ref idx);
        var isDebit = value < 0;
        var newBalance = client[0] + value;
        if (isDebit && -newBalance > client[1]) return _unprocessable;
        client[0] = newBalance;
        return client;
    }
}