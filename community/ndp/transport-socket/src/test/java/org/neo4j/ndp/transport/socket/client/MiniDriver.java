package org.neo4j.ndp.transport.socket.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.ndp.messaging.v1.message.InitializeMessage;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.PullAllMessage;
import org.neo4j.ndp.messaging.v1.message.RunMessage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.message;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.acceptedVersions;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.chunk;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.recvChunkHeader;

public class MiniDriver
{
    public static final String CLIENT_NAME = "MiniDriver/1.0";

    private final Connection connection;
    private final LinkedList<Message> outbox;

    private int chunkSize = 16383;

    public MiniDriver( Connection connection ) throws Exception
    {
        assertThat( connection.isConnected(), equalTo( true ) );
        this.connection = connection;
        this.outbox = new LinkedList<>();
        int version = handshake( 1, 0, 0, 0 );
        assertThat( version, equalTo( 1 ) );
        addInitMessage( CLIENT_NAME );
        send();
        recv( 1 );
    }

    public int chunkSize()
    {
        return chunkSize;
    }

    public void setChunkSize( int value )
    {
        chunkSize = value;
    }

    private int handshake( int first, int second, int third, int fourth ) throws IOException,
            InterruptedException
    {
        connection.send( acceptedVersions( first, second, third, fourth ) );
        byte[] response = connection.recv( 4 );
        return 16777216 * response[0] + 65536 * response[1] + 256 * response[2] + response[3];
    }

    private void addInitMessage( String clientName )
    {
        outbox.add( new InitializeMessage( clientName ) );
    }

    public MiniDriver addRunMessage( String statement, Map<String, Object> parameters )
    {
        outbox.add( new RunMessage( statement, parameters ) );
        return this;
    }

    public MiniDriver addRunMessage( String statement )
    {
        addRunMessage( statement, Collections.<String, Object>emptyMap() );
        return this;
    }

    public MiniDriver addPullAllMessage()
    {
        outbox.add( new PullAllMessage() );
        return this;
    }

    public void send() throws IOException
    {
        Message[] messages = outbox.toArray( new Message[outbox.size()] );
        outbox.clear();
        connection.send( chunk( chunkSize, messages ) );
    }

    public Message[] recv( int count ) throws IOException, InterruptedException
    {
        List<Message> messages = new ArrayList<>(count);
        int messageNo = 0;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ( messageNo < count )
        {
            int size = recvChunkHeader( connection );

            if ( size > 0 )
            {
                baos.write( connection.recv( size ) );
            }
            else
            {
                messages.add( message( baos.toByteArray() ) );
                baos = new ByteArrayOutputStream();
                messageNo++;
            }
        }
        return messages.toArray(new Message[count]);
    }

}
