/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ndp.transport.socket.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.ndp.messaging.v1.message.DiscardAllMessage;
import org.neo4j.ndp.messaging.v1.message.InitializeMessage;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.PullAllMessage;
import org.neo4j.ndp.messaging.v1.message.RunMessage;
import org.neo4j.ndp.messaging.v1.message.SuccessMessage;
import org.neo4j.ndp.transport.socket.integration.TransportErrorIT;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.message;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.serialize;

public class MiniDriver implements AutoCloseable
{

    public static final String CLIENT_NAME = "MiniDriver/1.0";

    public static byte[] chunk( Message... messages ) throws IOException
    {
        return chunk( 32, messages );
    }

    public static byte[] chunk( int chunkSize, Message... messages ) throws IOException
    {
        byte[][] serializedMessages = new byte[messages.length][];
        for ( int i = 0; i < messages.length; i++ )
        {
            serializedMessages[i] = serialize( messages[i] );
        }
        return chunk( chunkSize, serializedMessages );
    }

    public static byte[] chunk( int chunkSize, byte[]... messages )
    {
        ByteBuffer output = ByteBuffer.allocate( 1024 ).order( ByteOrder.BIG_ENDIAN );

        for ( byte[] wholeMessage : messages )
        {
            int left = wholeMessage.length;
            while ( left > 0 )
            {
                int size = Math.min( left, chunkSize );
                output.putShort( (short) size );

                int offset = wholeMessage.length - left;
                output.put( wholeMessage, offset, size );

                left -= size;
            }
            output.putShort( (short) 0 );
        }

        output.flip();

        byte[] arrayOutput = new byte[output.limit()];
        output.get( arrayOutput );
        return arrayOutput;
    }

    @SafeVarargs
    public static <T> Matcher<T[]> equalsArray( final Matcher<T>... expectedItems )
    {
        return new TypeSafeMatcher<T[]>()
        {
            @Override
            protected boolean matchesSafely( T[] items )
            {
                for ( int i = 0; i < items.length; i++ )
                {
                    if ( !expectedItems[i].matches( items[i] ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendList( "[", ",", "]", asList( expectedItems ) );
            }

        };
    }

    public static MiniDriver forConnection( Connection connection )
            throws Exception
    {
        return new MiniDriver( connection ).handshake( 1, 0, 0, 0 ).init();
    }

    public static MiniDriver forConnection( Connection connection, int protocolVersion )
            throws Exception
    {
        return new MiniDriver( connection ).handshake( protocolVersion, 0, 0, 0 ).init();
    }

    private final Connection connection;
    private final LinkedList<Message> outbox;

    private int chunkSize = 16383;
    private int protocolVersion = 0;
    private boolean ready = false;    /* set true once initialized */

    public MiniDriver( Connection connection )
    {
        assertThat( connection.isConnected(), equalTo( true ) );
        this.connection = connection;
        this.outbox = new LinkedList<>();
    }

    public MiniDriver init( String clientName ) throws IOException, InterruptedException
    {
        addInitMessage( clientName );
        send();
        Message[] messages = recv( 1 );
        ready = messages[0] instanceof SuccessMessage;   // might be a better way to do this
        return this;
    }

    public MiniDriver init() throws IOException, InterruptedException
    {
        return init( CLIENT_NAME );
    }

    public Connection connection()
    {
        return connection;
    }

    public int chunkSize()
    {
        return chunkSize;
    }

    public void setChunkSize( int value )
    {
        chunkSize = value;
    }

    public MiniDriver handshake( int first, int second, int third, int fourth )
            throws IOException, InterruptedException
    {
        ByteBuffer bb = ByteBuffer.allocate( 4 * 4 ).order( BIG_ENDIAN );
        bb.putInt( first );
        bb.putInt( second );
        bb.putInt( third );
        bb.putInt( fourth );

        connection.send( bb.array() );
        byte[] response = connection.recv( 4 );
        protocolVersion =
                16777216 * response[0] + 65536 * response[1] + 256 * response[2] + response[3];
        return this;
    }

    public int protocolVersion()
    {
        return protocolVersion;
    }

    /**
     * Returns true if connection has been initialised successfully.
     */
    public boolean ready()
    {
        return ready;
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

    public MiniDriver addDiscardAllMessage()
    {
        outbox.add( new DiscardAllMessage() );
        return this;
    }

    public MiniDriver send() throws IOException
    {
        Message[] messages = outbox.toArray( new Message[outbox.size()] );
        outbox.clear();
        connection.send( chunk( chunkSize, messages ) );
        return this;
    }

    public Message[] recv( int count ) throws IOException, InterruptedException
    {
        List<Message> messages = new ArrayList<>( count );
        int messageNo = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ( messageNo < count )
        {
            int size = TransportErrorIT.recvChunkHeader( connection );

            if ( size > 0 )
            {
                out.write( connection.recv( size ) );
            }
            else
            {
                messages.add( message( out.toByteArray() ) );
                out = new ByteArrayOutputStream();
                messageNo++;
            }
        }
        return messages.toArray( new Message[count] );
    }

    @Override
    public void close() throws Exception
    {
        connection.close();
    }
}
