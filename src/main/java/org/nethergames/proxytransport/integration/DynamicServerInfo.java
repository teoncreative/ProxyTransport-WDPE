package org.nethergames.proxytransport.integration;

import dev.waterdog.waterdogpe.logger.MainLogger;
import dev.waterdog.waterdogpe.network.connection.client.ClientConnection;
import dev.waterdog.waterdogpe.network.serverinfo.BedrockServerInfo;
import dev.waterdog.waterdogpe.network.serverinfo.ServerInfo;
import dev.waterdog.waterdogpe.network.serverinfo.ServerInfoType;
import dev.waterdog.waterdogpe.player.ProxiedPlayer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import net.jodah.expiringmap.internal.NamedThreadFactory;
import org.nethergames.proxytransport.impl.TransportChannelInitializer;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class DynamicServerInfo extends BedrockServerInfo {

    private final ConcurrentHashMap<InetSocketAddress, Future<QuicChannel>> serverConnections = new ConcurrentHashMap<>();

    public DynamicServerInfo(String serverName, InetSocketAddress address, InetSocketAddress publicAddress) {
        super(serverName, address, publicAddress);
    }

    @Override
    public abstract ServerInfoType getServerType();

    public abstract TransportType determineTransportType(ProxiedPlayer player);

    @Override
    public Future<ClientConnection> createConnection(ProxiedPlayer player) {
        return switch (determineTransportType(player)) {
            case TCP -> TcpTransportServerInfo.createConnection(player, this);
            case QUIC -> QuicTransportServerInfo.createConnection(player, this, serverConnections);
            default -> super.createConnection(player);
        };
    }
}
