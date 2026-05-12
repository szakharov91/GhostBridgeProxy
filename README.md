# 🌉 GhostBridgeProxy

**Secure, self-hosted tunnel from your localhost to the internet**

[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4)](https://dotnet.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

GhostBridgeProxy — это легковесный, self-hosted аналог ngrok, написанный на C# .NET 8. 
Он создает защищенный "призрачный мост" между вашим локальным сервисом и публичным интернетом, 
обходит NAT и файрволы без сложной настройки.

### ✨ Возможности

- 🔒 **Безопасность** — полный контроль над вашими данными (self-hosted)
- 🚀 **Простота** — одна команда для публикации локального сервера
- 🔌 **Мультипротокольность** — HTTP/HTTPS, WebSocket, TCP (SSH, RDP, базы данных)
- 🐳 **Docker Ready** — сервер и клиент в контейнерах
- 📊 **Панель управления** — статистика соединений в реальном времени

### 🚀 Быстрый старт

```bash
# Установка клиента
dotnet tool install --global GhostBridge.Client

# Запуск туннеля для локального приложения
ghostbridge expose http://localhost:3000
# 👻 Tunnel open: https://my-subdomain.your-server.com