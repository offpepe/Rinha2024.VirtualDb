FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
RUN apt update
RUN apt install -y clang zlib1g-dev
WORKDIR /src
COPY ["Rinha2024.VirtualDb.csproj", "./"]
RUN dotnet restore "Rinha2024.VirtualDb.csproj" -p:ON_CLUSTER=true
COPY . .
WORKDIR "/src/"
RUN dotnet build "Rinha2024.VirtualDb.csproj" -p:ON_CLUSTER=true -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Rinha2024.VirtualDb.csproj" -p:ON_CLUSTER=true -c Release -o /app/publish /p:UseAppHost=true

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["./Rinha2024.VirtualDb"]
