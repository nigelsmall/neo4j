package org.neo4j.ndp.transport.socket;

import java.io.File;
import java.io.IOException;
import java.security.cert.CertificateException;

import io.netty.channel.Channel;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.ndp.runtime.Sessions;
import org.neo4j.ndp.runtime.internal.StandardSessions;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;

import static org.neo4j.collection.primitive.Primitive.longObjectMap;

public class MiniServer
{
    private final LifeSupport life = new LifeSupport();

    private HostnamePort socketHostnamePort = new HostnamePort( "localhost:7687" );
    private HostnamePort webSocketHostnamePort = new HostnamePort( "localhost:7688" );
    private File storeDir = null;

    public MiniServer()
    {
    }

    public MiniServer withSocketPort( int port )
    {
        socketHostnamePort = new HostnamePort( "localhost:" + port );
        return this;
    }

    public MiniServer withWebSocketPort( int port )
    {
        webSocketHostnamePort = new HostnamePort( "localhost:" + port );
        return this;
    }

    public MiniServer withStore(File dir) {
        this.storeDir = dir;
        return this;
    }

    public void run() throws IOException, CertificateException
    {
        final GraphDatabaseService gdb = storeDir == null ?
                new TestGraphDatabaseFactory().newImpermanentDatabase() :
                new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        final GraphDatabaseAPI api = ((GraphDatabaseAPI) gdb);
        final LogService logging =
                api.getDependencyResolver().resolveDependency( LogService.class );
        final UsageData usageData =
                api.getDependencyResolver().resolveDependency( UsageData.class );
        final JobScheduler scheduler =
                api.getDependencyResolver().resolveDependency( JobScheduler.class );

        final Sessions sessions = life.add( new StandardSessions( api, usageData, logging ) );

        final PrimitiveLongObjectMap<Function<Channel, SocketProtocol>>
                availableVersions = longObjectMap();

        availableVersions.put( SocketProtocolV1.VERSION, new Function<Channel, SocketProtocol>()
        {
            @Override
            public SocketProtocol apply( Channel channel )
            {
                return new SocketProtocolV1( logging, sessions.newSession(), channel );
            }
        } );

//        SelfSignedCertificate ssc = new SelfSignedCertificate();
//        SslContext sslCtx = SslContextBuilder.forServer(
//                ssc.certificate(), ssc.privateKey() ).build();

        // Start services
        SocketTransport socketTransport = new SocketTransport(
                socketHostnamePort, null, logging.getUserLogProvider(), availableVersions );
        WebSocketTransport wsTransport = new WebSocketTransport(
                webSocketHostnamePort, null, logging.getUserLogProvider(), availableVersions );
        life.add( new NettyServer( scheduler.threadFactory( JobScheduler.Groups.gapNetworkIO ),
                asList( socketTransport, wsTransport ) ) );

        life.start();
        try
        {
            System.out.printf( "NDP Server running, press any key to exit." );
            System.in.read();
        }
        finally
        {
            life.shutdown();
            gdb.shutdown();
        }
    }

    /**
     * For manual testing, runs an NDP server
     */
    public static void main( String... args ) throws Exception
    {
        new MiniServer().run();
    }

}
