FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Aether.Api/Aether.Api.csproj Aether.Api/
RUN dotnet restore Aether.Api/Aether.Api.csproj
COPY Aether.Api/ Aether.Api/
RUN dotnet publish Aether.Api/Aether.Api.csproj -c Release -o /out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /out .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Aether.Api.dll"]
