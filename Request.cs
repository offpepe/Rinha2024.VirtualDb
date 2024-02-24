namespace Rinha2024.VirtualDb;

public readonly record struct Request(int[] Parameters, Guid OperationId);
