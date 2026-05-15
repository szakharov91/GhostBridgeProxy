var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://0.0.0.0:8787");

var app = builder.Build();
app.MapGet("/test-app", () => Results.Text("I'm Alive"));
app.Run();
