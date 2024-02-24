using System.Collections.Concurrent;
using System.Net.Sockets;
using Rinha2024.Dotnet.IO;

namespace Rinha2024.VirtualDb.Net;



public class ListenerProcess(int port, ConcurrentQueue<Request> transactionQueue, ConcurrentDictionary<Guid, int[]> results, ConcurrentDictionary<int, int[]> clients, ConcurrentQueue<int> deadPool)
{

    
    
    
}