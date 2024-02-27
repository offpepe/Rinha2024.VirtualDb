using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Rinha2024.VirtualDb;

public readonly struct VirtualDatabase
{
    public VirtualDatabase()
    {
        _unprocessable = [0, 1];
        _clients = [
            [0, 0],
            [0, 100000],
            [0, 80000],
            [0, 1000000],
            [0, 10000000],
            [0, 500000],
        ];
    }
    private readonly int[] _unprocessable;
    private readonly int[][] _clients;
    public ref int[] GetClient(ref int idx) => ref _clients[idx];


    public int[] DoTransaction(ref int idx, ref int value)
    {
        var client = GetClient(ref idx);
        var isDebit = value < 0;
        var newBalance = client[1] + value;
        if (isDebit && -newBalance > client[1]) return _unprocessable;
        client[0] = newBalance;
        return client;
    }
}