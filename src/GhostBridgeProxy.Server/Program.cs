using GhostBridgeProxy.Server;
using Yarp.ReverseProxy.Configuration;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<TunnelOptions>(
    builder.Configuration.GetSection(TunnelOptions.SectionName));

builder.Services.AddSingleton<TunnelRegistry>();
builder.Services.AddHostedService<TunnelTcpListenerHostedService>();

builder.Services.AddReverseProxy()
    .LoadFromMemory(GetRoutes(), GetClusters());

builder.WebHost.ConfigureKestrel((ctx, options) =>
{
    var port = ctx.Configuration.GetValue("Tunnel:ControlListenPort", 5080);
    options.ListenAnyIP(port);
});

var app = builder.Build();

app.UseWebSockets();

app.Map("/tunnel", async (HttpContext context, TunnelRegistry registry, IConfiguration configuration, ILogger<TunnelRegistry> log) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    var secret = configuration["Tunnel:SharedSecret"];
    if (!string.IsNullOrEmpty(secret))
    {
        var token = context.Request.Query["token"].FirstOrDefault()
            ?? context.Request.Headers["X-Tunnel-Token"].FirstOrDefault();
        if (token != secret)
        {
            context.Response.StatusCode = 403;
            return;
        }
    }

    using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
    await TunnelAgentConnection.RunAsync(webSocket, registry, log, context.RequestAborted);
});

app.MapReverseProxy();

await app.RunAsync();

static IReadOnlyList<RouteConfig> GetRoutes() => Array.Empty<RouteConfig>();

static IReadOnlyList<ClusterConfig> GetClusters() => Array.Empty<ClusterConfig>();
