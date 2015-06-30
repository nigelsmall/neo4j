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
package org.neo4j.ndp.transport.socket.integration;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.ndp.transport.socket.client.Connection;
import org.neo4j.ndp.transport.socket.client.MiniDriver;
import org.neo4j.ndp.transport.socket.client.SecureSocketConnection;
import org.neo4j.ndp.transport.socket.client.SecureWebSocketConnection;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class HandshakeIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    private MiniDriver driver;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        new Factory<Connection>()
                        {
                            @Override
                            public Connection newInstance()
                            {
                                return new SecureSocketConnection();
                            }
                        },
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        new Factory<Connection>()
                        {
                            @Override
                            public Connection newInstance()
                            {
                                return new SecureWebSocketConnection();
                            }
                        },
                        new HostnamePort( "localhost:7688" )
                } );
    }

    @Test
    public void shouldBeAbleToNegotiatePreferredProtocolVersion() throws Throwable
    {
        // When
        driver.handshake( 1, 0, 0, 0 );

        // Then
        assertThat( driver.protocolVersion(), equalTo( 1 ) );
    }

    @Test
    public void shouldBeAbleToNegotiateSecondBestProtocolVersion() throws Throwable
    {
        // When
        driver.handshake( 0, 1, 0, 0 );

        // Then
        assertThat( driver.protocolVersion(), equalTo( 1 ) );
    }

    @Test
    public void shouldBeAbleToNegotiateThirdBestProtocolVersion() throws Throwable
    {
        // When
        driver.handshake( 0, 0, 1, 0 );

        // Then
        assertThat( driver.protocolVersion(), equalTo( 1 ) );
    }

    @Test
    public void shouldBeAbleToNegotiateFourthBestProtocolVersion() throws Throwable
    {
        // When
        driver.handshake( 0, 0, 0, 1 );

        // Then
        assertThat( driver.protocolVersion(), equalTo( 1 ) );
    }

    @Test
    public void shouldReturnZeroOnZeroInput() throws Throwable
    {
        // When
        driver.handshake( 0, 0, 0, 0 );

        // Then
        assertThat( driver.protocolVersion(), equalTo( 0 ) );
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // When
        driver.handshake( 1337, 0, 0, 0 );

        // Then
        assertThat( driver.protocolVersion(), equalTo( 0 ) );
    }

    @Before
    public void setup() throws Exception
    {
        driver = new MiniDriver( cf.newInstance().connect( address ) );
    }

    @After
    public void teardown() throws Exception
    {
        driver.close();
    }

}
