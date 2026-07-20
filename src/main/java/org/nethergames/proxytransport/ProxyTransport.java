package org.nethergames.proxytransport;

import dev.waterdog.waterdogpe.network.protocol.ProtocolCodecs;
import dev.waterdog.waterdogpe.plugin.Plugin;
import java.nio.file.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.nethergames.proxytransport.common.transport.TransportLogger;
import org.nethergames.proxytransport.common.util.QuicLibraryInstaller;
import org.nethergames.proxytransport.integration.QuicTransportServerInfo;
import org.nethergames.proxytransport.integration.TcpTransportServerInfo;
import org.nethergames.proxytransport.utils.CodecUpdater;

public class ProxyTransport extends Plugin {

    private static final String QUIC_PROBE_CLASS = "io.netty.incubator.codec.quic.QuicSslContextBuilder";
    private static final String[] QUIC_JARS = {
        // netty-handler first: the QUIC classes need io.netty.handler.ssl.SslContext, which WaterdogPE lacks.
        "/quic-libs/netty-handler.jar",
        "/quic-libs/netty-incubator-codec-classes-quic.jar",
        "/quic-libs/netty-incubator-codec-native-quic.jar",
    };

    private static final String QUICHE_LOGGER = "io.netty.incubator.codec.quic.Quiche";
    private static final String QUICHE_DEBUG_PROPERTY = "proxytransport.quic.debug";

    @Override
    public void onStartup() {
        ProtocolCodecs.addUpdater(new CodecUpdater());

        getLogger().info("ProxyTransport was started.");

        silenceQuicheLogging();

        // Inject the QUIC native onto netty's classloader before any QUIC type is referenced, and only register
        // the QUIC type if that succeeded.
        Path stagingDir = Path.of(System.getProperty("java.io.tmpdir"), "proxytransport-quic-libs");
        if (QuicLibraryInstaller.tryInstall(QUIC_PROBE_CLASS, QUIC_JARS, stagingDir, logger())) {
            registerQuicType();
        }

        getLogger().info("Registered type with name {}", TcpTransportServerInfo.TYPE.getIdentifier());
    }

    /**
     * Pins quiche's logger to INFO unless {@code -Dproxytransport.quic.debug=true} is set. quiche logs every
     * packet it sends and receives, and netty decides whether to enable that native logging from the logger's
     * level while it initializes, so on a proxy running at DEBUG it has to be pinned before the library loads.
     */
    private void silenceQuicheLogging() {
        if (Boolean.getBoolean(QUICHE_DEBUG_PROPERTY)) {
            getLogger().info("quiche debug logging is enabled");
            return;
        }
        Configurator.setLevel(QUICHE_LOGGER, Level.INFO);
    }

    // Separate method so the QUIC classes are only loaded when QUIC is available.
    private void registerQuicType() {
        getLogger().info("Registered type with name {}", QuicTransportServerInfo.TYPE.getIdentifier());
    }

    private TransportLogger logger() {
        return new TransportLogger() {
            @Override
            public void info(String message) {
                getLogger().info(message);
            }

            @Override
            public void warn(String message) {
                getLogger().warn(message);
            }

            @Override
            public void error(String message, Throwable cause) {
                getLogger().error(message, cause);
            }
        };
    }

    @Override
    public void onEnable() {
        getLogger().info("ProxyTransport was enabled.");
    }
}