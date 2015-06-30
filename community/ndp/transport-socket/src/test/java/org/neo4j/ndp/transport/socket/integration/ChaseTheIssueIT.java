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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.transport.socket.client.Connection;
import org.neo4j.ndp.transport.socket.client.MiniDriver;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.equalsArray;

/**
 * Multiple concurrent users should be able to connect simultaneously. We test this with multiple
 * users running
 * load that they roll back, asserting they don't see each others changes.
 */
@RunWith(Parameterized.class)
public class ChaseTheIssueIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return TransportSessionIT.transports();
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // Given
        int numWorkers = 8;
        int numRequests = 10_000;

        for ( int i = 0; i < 1000; i++ )
        {
            try( MiniDriver driver = newDriver() )
            {
                setup(driver);
            }

            List<Callable<Void>> workers = createWorkers( numWorkers, numRequests );
            ExecutorService exec = Executors.newFixedThreadPool( numWorkers );

            try
            {
                // When & then
                for ( Future<Void> f : exec.invokeAll( workers ) )
                {
                    f.get( 60, TimeUnit.SECONDS );
                }
            }
            finally
            {
                exec.shutdownNow();
                exec.awaitTermination( 30, TimeUnit.SECONDS );
            }
        }
    }

    private void setup(MiniDriver driver) throws Exception
    {
        // Get id generation up to 5-digit range, issue seemed more likely here
        System.out.print( "Setup" );
        Message[] msgs = driver.addRunMessage( "FOREACH( n in range(0,10000) | CREATE (:Person) )" )
                .addDiscardAllMessage()
                .addRunMessage( "MATCH (n) DELETE n" )
                .addDiscardAllMessage()
                .send()
                .recv( 4 );
        assertThat( msgs, equalsArray(
                msgSuccess( map( "fields", asList() ) ),
                msgSuccess(),
                msgSuccess( map( "fields", asList() ) ),
                msgSuccess() ) );

        // Create 1000 nodes to read during the main load part
        System.out.println( ".." );
        msgs = driver
                .addRunMessage( "FOREACH( n in range(0,1000) | CREATE (:Person) )" )
                .addDiscardAllMessage()
                .send()
                .recv( 2 );
        assertThat( msgs, equalsArray(
                msgSuccess( map( "fields", asList() ) ),
                msgSuccess() ) );

        // Warmup
        System.out.println("Warmup..");
        for ( int i = 0; i < 1500; i++ )
        {
            driver.addRunMessage( "MATCH (a:Person) RETURN a LIMIT 500" )
                  .addPullAllMessage()
                  .send()
                  .recv( 502 );
        }
        System.out.println("Running.");
    }

    private static void useCase( MiniDriver driver ) throws Exception
    {
        driver.addRunMessage( "MATCH (a:Person) RETURN a LIMIT 500" )
            .addPullAllMessage()
            .send()
            .recv( 502 );
    }

    private List<Callable<Void>> createWorkers( int numWorkers, int numRequests ) throws Exception
    {
        List<Callable<Void>> workers = new LinkedList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            workers.add( newWorker( numRequests ) );
        }
        return workers;
    }

    private Callable<Void> newWorker( final int iterationsToRun ) throws Exception
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                MiniDriver driver = newDriver();

                for ( int i = 0; i < iterationsToRun; i++ )
                {
                    useCase( driver );
                }

                return null;
            }
        };

    }

    private MiniDriver newDriver() throws Exception
    {
        Connection connection = cf.newInstance();
        connection.connect( address );
        return new MiniDriver( connection );
    }

}
