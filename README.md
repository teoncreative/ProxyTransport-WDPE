# ProxyTransport for WaterdogPE

A [WaterdogPE](https://github.com/WaterdogPE/WaterdogPE) plugin that connects the proxy to its downstream
servers over **raw TCP or QUIC** instead of RakNet.

```
Bedrock client ──RakNet──▶ WaterdogPE (+this plugin) ──TCP/QUIC──▶ downstream server
```

## Setup

Drop the jar into the proxy's `plugins/` folder and restart. The plugin registers two downstream server types,
`tcp` and `quic`.

## Pointing servers at it

Server info objects have to be created through this plugin, so add it as a dependency of the plugin that
registers your servers:

```yaml
depends:
  - ProxyTransport
```

Then create your servers through the factory for the transport you want, instead of `new ServerInfo(...)`:

```java
ServerInfoFactory QUIC_INFO_FACTORY = ServerInfoType.fromString("quic").getServerInfoFactory();

ServerInfo server = QUIC_INFO_FACTORY.createServerInfo(name, address, publicAddress);
```

Use `"tcp"` for the TCP transport. If you have your own `ServerInfo` subclass, it needs to extend this
plugin's type instead.

## QUIC

QUIC needs an extra JVM flag on the proxy on Java 17+, because the QUIC native has to be loaded into Netty's
class loader:

```
--add-opens java.base/jdk.internal.loader=ALL-UNNAMED
```

If the flag is missing, QUIC is skipped and the log names the exact flag to add — TCP is unaffected.

## Downstream servers

The downstream server has to speak ProxyTransport too:

- **Geyser** — [ProxyTransport-Geyser](https://github.com/teoncreative/ProxyTransport-Geyser)
- **PocketMine-MP** — [ProxyTransport-PM](https://github.com/NetherGamesMC/ProxyTransport-PM)

## Protocol

The wire protocol and the shared implementation live in
[ProxyTransport-Common](https://github.com/teoncreative/ProxyTransport-Common).