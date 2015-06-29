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
package org.neo4j.ndp.messaging.v1;

import java.io.IOException;
import java.util.HashMap;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.SuccessMessage;
import org.neo4j.packstream.BufferedChannelInput;
import org.neo4j.packstream.BufferedChannelOutput;
import org.neo4j.packstream.PackStream;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1.Writer.NO_OP;

public class RepeatedMessageTest
{
    RecordingByteChannel channel;
    MessageFormat.Reader reader;
    MessageFormat.Writer writer;

    @Before
    public void setUp() throws Exception
    {
        channel = new RecordingByteChannel();
        reader = new PackStreamMessageFormatV1.Reader(
                new PackStream.Unpacker( new BufferedChannelInput( 16 ).reset( channel ) ) );
        writer = new PackStreamMessageFormatV1.Writer(
                new PackStream.Packer( new BufferedChannelOutput( channel, 4 ) ), NO_OP );
    }

    @Test
    public void shouldHandleWriteFlushReadWriteFlushRead( ) throws IOException
    {
        SuccessMessage msg = new SuccessMessage( new HashMap<String, Object>() );
        int times = 100;

        for ( int i = 0; i < times; i++ )
        {
            writer.write( msg ).flush();
            assertThat( unpack( reader, channel ), Matchers.<Message>equalTo( msg ) );
        }
        channel.eof();

    }

    @Test
    public void shouldHandleWriteFlushWriteFlushReadRead() throws IOException
    {
        SuccessMessage msg = new SuccessMessage( new HashMap<String, Object>() );
        int times = 100;

        for ( int i = 0; i < times; i++ )
        {
            writer.write( msg ).flush();
        }
        channel.eof();

        for ( int i = 0; i < times; i++ )
        {
            assertThat( unpack( reader, channel ), Matchers.<Message>equalTo( msg ) );
        }

    }

    @Test
    public void shouldHandleWriteWriteFlushReadRead() throws IOException
    {
        SuccessMessage msg = new SuccessMessage( new HashMap<String, Object>() );
        int times = 100;

        for ( int i = 0; i < times; i++ )
        {
            writer.write( msg );
        }
        writer.flush();
        channel.eof();

        for ( int i = 0; i < times; i++ )
        {
            assertThat( unpack( reader, channel ), Matchers.<Message>equalTo( msg ) );
        }

    }

    private <T extends Message> T unpack( MessageFormat.Reader reader, RecordingByteChannel
            channel )
    {
        // Unpack
        String serialized = HexPrinter.hex( channel.getBytes() );
        RecordingMessageHandler messages = new RecordingMessageHandler();
        try
        {
            reader.read( messages );
        }
        catch ( Throwable e )
        {
            throw new AssertionError( "Failed to unpack message, wire data was:\n" + serialized +
                    "[" + channel
                    .getBytes().length + "b]", e );
        }

        return (T) messages.asList().get( 0 );
    }

}
