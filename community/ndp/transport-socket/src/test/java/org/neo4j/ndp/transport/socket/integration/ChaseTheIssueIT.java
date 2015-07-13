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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.RecordMessage;
import org.neo4j.ndp.transport.socket.client.Connection;
import org.neo4j.ndp.transport.socket.client.MiniDriver;

import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.transport.socket.client.MiniDriver.equalsArray;

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
        int numWorkers = 1;
        int numRequests = 10_000;

        for ( int i = 1; i <= 1; i++ )
        {
            System.out.println("Run number " + i);
            try ( MiniDriver driver = newDriver() )
            {
                setup( driver );
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

            System.out.println();

        }
    }

    private void setup( MiniDriver driver ) throws Exception
    {

        // Warmup
        System.out.print( "  Warming up" );
        for ( int i = 1; i <= 1500; i++ )
        {
            driver
                    .addRunMessage( "unwind range(1,500) as z return [z,z,z,z,z,z,z] as zzz" )
                    .addPullAllMessage()
                    .send()
                    .recv( 502 );
            if (i % 150 == 0) {
                System.out.print('.');
            }
        }
        System.out.println();
    }

    private static void useCase( MiniDriver driver ) throws Exception
    {
        Message[] msgs = driver
                .addRunMessage( "unwind range(1,500) as z return [z,z,z,z,z,z,z] as zzz" )
                .addPullAllMessage()
                .send()
                .recv( 502 );
        assertThat( msgs[0], msgSuccess( map( "fields", asList( "zzz" ) ) ) );
        for(int i = 1; i <= 500; i++) {
            assertThat( msgs[i], instanceOf( RecordMessage.class ) );
        }
        assertThat( msgs[501], msgSuccess( map() ) );
    }

    private List<Callable<Void>> createWorkers( int numWorkers, int numRequests ) throws Exception
    {
        List<Callable<Void>> workers = new LinkedList<>();
        for ( int i = 1; i <= numWorkers; i++ )
        {
            workers.add( newWorker( i, numRequests ) );
        }
        return workers;
    }

    private Callable<Void> newWorker( final int workerNumber, final int iterationsToRun )
            throws Exception
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                System.out.println("  Starting worker " + workerNumber);

                MiniDriver driver = newDriver();

                for ( int i = 0; i < iterationsToRun; i++ )
                {
                    useCase( driver );
                }

                System.out.println("  Worker " + workerNumber + " completed");

                return null;
            }
        };

    }

    private MiniDriver newDriver() throws Exception
    {
        return MiniDriver.forConnection( cf.newInstance().connect( address ) );
    }

}
