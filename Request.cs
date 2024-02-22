namespace Rinha2024.VirtualDb;

public sealed class Request
{
    public object Parameter { get; init; } = 0;
    public Guid OperationId { get; init; }
    public bool Read { get; init; }
    
    public int ReadIndex() => (int) Parameter;
    public int[]? ReadTransactionParams() => Parameter as int[];
    
    
}