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

    /**
     * Flow control windows. Each side advertises what it is willing to receive, so these bound what
     * the downstream server may send to us: chunk data and StartGamePacket. That is the direction
     * that fills up, which makes this end the one that matters for join bursts.
     * <p>
     * One connection is shared by every player on a server, one stream each, so the connection
     * window covers all of them at once. A player joining pulls several megabytes of chunks, and
     * once the shared window is exhausted every stream stalls, including someone still waiting on
     * StartGamePacket. Keep these in step with QuicProxyTransportServer, which governs the reverse
     * direction.
     */
    private static final int MAX_STREAM_DATA = 2 * 1024 * 1024;
    private static final int MAX_CONNECTION_DATA = 64 * 1024 * 1024;
    private static final int MAX_STREAMS = 256;

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

        // Claim the slot atomically. containsKey followed by put let two simultaneous first logins
        // each build a connection, and the loser's QuicChannel was left with nothing to close it.
        // A cached entry is only reused while it is still live, so a connection that died without
        // its closeFuture having run yet cannot poison every subsequent login to this server.
        Promise<QuicChannel> promise = eventLoop.newPromise();
        Future<QuicChannel> claimed = serverConnections.compute(target,
                (key, existing) -> isUsable(existing) ? existing : promise);

        if (claimed != promise) {
            logger.info("Reusing connection to " + target + " for " + info.getServerName() + " server");
            return claimed;
        }

        logger.info("Creating connection to " + target + " for " + info.getServerName() + " server");

        QuicSslContext sslContext = QuicSslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).applicationProtocols("ng").build();
        ChannelHandler codec = new QuicClientCodecBuilder()
                .sslContext(sslContext)
                .maxIdleTimeout(30, TimeUnit.SECONDS)
                .initialMaxData(MAX_CONNECTION_DATA)
                .initialMaxStreamDataBidirectionalLocal(MAX_STREAM_DATA)
                .initialMaxStreamDataBidirectionalRemote(MAX_STREAM_DATA)
                .initialMaxStreamsBidirectional(MAX_STREAMS)
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
                                            serverConnections.remove(target, promise);
                                        });

                                        promise.trySuccess(quicChannel);
                                    } else {
                                        logger.warning("Connection to " + target + " for " + info.getServerName() + " server failed");

                                        promise.tryFailure(quicChannelFuture.cause());
                                        channelFuture.channel().close();
                                        serverConnections.remove(target, promise);
                                    }
                                });
                    } else {
                        promise.tryFailure(channelFuture.cause());
                        channelFuture.channel().close();
                        serverConnections.remove(target, promise);
                    }
                });

        return promise;
    }

    /**
     * Whether a cached connection can still take new streams. A future that has not resolved yet is
     * usable, because whoever created it is still connecting and everyone else should wait on it.
     * One that failed, or that resolved to a channel which has since gone inactive, must not be
     * handed out again: a dead connection used to stay cached until its closeFuture listener ran,
     * and every login to that server in the meantime got a stream that could never deliver anything.
     */
    private static boolean isUsable(Future<QuicChannel> connection) {
        if (connection == null) {
            return false;
        }
        if (!connection.isDone()) {
            return true;
        }
        if (!connection.isSuccess()) {
            return false;
        }
        QuicChannel channel = connection.getNow();
        return channel != null && channel.isActive();
    }

    public static Class<? extends DatagramChannel> getProperSocketChannel() {
        return Epoll.isAvailable() ? EpollDatagramChannel.class : NioDatagramChannel.class;
    }
}
