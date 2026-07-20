package org.nethergames.proxytransport.integration;

import dev.waterdog.waterdogpe.logger.MainLogger;
import dev.waterdog.waterdogpe.network.connection.client.ClientConnection;
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
import org.apache.logging.log4j.core.jmx.Server;
import org.nethergames.proxytransport.impl.TransportChannelInitializer;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class QuicTransportServerInfo extends ServerInfo {
    public static final int availableCPU = Runtime.getRuntime().availableProcessors();
    public static final ThreadFactory downstreamThreadFactory = new NamedThreadFactory("QUIC-Downstream %s");
    public static final EventLoopGroup downstreamLoopGroup = Epoll.isAvailable() ? new EpollEventLoopGroup(availableCPU, downstreamThreadFactory) : new NioEventLoopGroup(availableCPU, downstreamThreadFactory);

    public static final String TYPE_IDENT = "quic";
    public static final ServerInfoType TYPE = ServerInfoType.builder()
            .identifier(TYPE_IDENT)
            .serverInfoFactory(QuicTransportServerInfo::new)
            .register();

    private final ConcurrentHashMap<InetSocketAddress, Future<QuicChannel>> serverConnections = new ConcurrentHashMap<>();

    public QuicTransportServerInfo(String serverName, InetSocketAddress address, InetSocketAddress publicAddress) {
        super(serverName, address, publicAddress);
    }

    @Override
    public ServerInfoType getServerType() {
        return TYPE;
    }

    @Override
    public Future<ClientConnection> createConnection(ProxiedPlayer player) {
        return createConnection(player, this, this.serverConnections);
    }
    
    public static Future<ClientConnection> createConnection(ProxiedPlayer player, ServerInfo info, ConcurrentHashMap<InetSocketAddress, Future<QuicChannel>> serverConnections) {
        EventLoop eventLoop = player.getProxy().getWorkerEventLoopGroup().next();
        Promise<ClientConnection> promise = eventLoop.newPromise();

        createServerConnection(info, serverConnections, eventLoop, player.getLogger(), info.getAddress()).addListener((Future<QuicChannel> future) -> {
            if (future.isSuccess()) {
                player.getLogger().debug("Creating stream for " + info.getServerName() + " server");
                QuicChannel quicChannel = future.getNow();

                quicChannel.createStream(QuicStreamType.BIDIRECTIONAL, new TransportChannelInitializer(player, info, promise)).addListener((Future<QuicStreamChannel> streamFuture) -> {
                    if (!streamFuture.isSuccess()) {
                        promise.tryFailure(streamFuture.cause());
                        quicChannel.close();
                    }
                });
            } else {
                promise.tryFailure(future.cause());
            }
        });

        return promise;
    }

    private static Future<QuicChannel> createServerConnection(ServerInfo info, ConcurrentHashMap<InetSocketAddress, Future<QuicChannel>> serverConnections, EventLoopGroup eventLoopGroup, MainLogger logger, InetSocketAddress address) {
        EventLoop eventLoop = eventLoopGroup.next();

        // QUIC needs a resolved address; unlike TCP it won't resolve a hostname and NPEs on a null InetAddress.
        final InetSocketAddress target = address.isUnresolved()
                ? new InetSocketAddress(address.getHostString(), address.getPort())
                : address;

        if (serverConnections.containsKey(target)) {
            logger.info("Reusing connection to " + target + " for " + info.getServerName() + " server");
            return serverConnections.get(target);
        }

        logger.info("Creating connection to " + target + " for " + info.getServerName() + " server");
        Promise<QuicChannel> promise = eventLoop.newPromise();
        serverConnections.put(target, promise);

        QuicSslContext sslContext = QuicSslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).applicationProtocols("ng").build();
        ChannelHandler codec = new QuicClientCodecBuilder()
                .sslContext(sslContext)
                .maxIdleTimeout(30, TimeUnit.SECONDS)
                .initialMaxData(10000000)
                .initialMaxStreamDataBidirectionalLocal(1000000)
                .maxRecvUdpPayloadSize(1350)
                .maxSendUdpPayloadSize(1350)
                .activeMigration(false)
                .build();

        new Bootstrap()
                .group(downstreamLoopGroup)
                .handler(codec)
                .channel(getProperSocketChannel())
                .bind(0).addListener((ChannelFuture channelFuture) -> {
                    if (channelFuture.isSuccess()) {
                        QuicChannel.newBootstrap(channelFuture.channel())
                                .streamHandler(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                        ctx.close();
                                    }
                                })
                                .remoteAddress(target)
                                .connect().addListener((Future<QuicChannel> quicChannelFuture) -> {
                                    if (quicChannelFuture.isSuccess()) {
                                        logger.debug("Connection to " + target + " for " + info.getServerName() + " server established");

                                        QuicChannel quicChannel = quicChannelFuture.getNow();
                                        quicChannel.closeFuture().addListener(f -> {
                                            logger.debug("Connection to " + target + " for " + info.getServerName() + " server closed");
                                            channelFuture.channel().close();
                                            serverConnections.remove(target);
                                        });

                                        promise.trySuccess(quicChannel);
                                    } else {
                                        logger.warning("Connection to " + target + " for " + info.getServerName() + " server failed");

                                        promise.tryFailure(quicChannelFuture.cause());
                                        channelFuture.channel().close();
                                        serverConnections.remove(target);
                                    }
                                });
                    } else {
                        promise.tryFailure(channelFuture.cause());
                        channelFuture.channel().close();
                        serverConnections.remove(target);
                    }
                });

        return promise;
    }

    public static Class<? extends DatagramChannel> getProperSocketChannel() {
        return Epoll.isAvailable() ? EpollDatagramChannel.class : NioDatagramChannel.class;
    }
}
