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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Base cursor for relationships.
 */
public abstract class StoreAbstractRelationshipCursor implements RelationshipCursor
{
    protected final RelationshipRecord relationshipRecord;

    protected final RelationshipStore relationshipStore;
    protected StoreStatement storeStatement;

    public StoreAbstractRelationshipCursor( RelationshipRecord relationshipRecord, RelationshipStore relationshipStore,
            StoreStatement storeStatement )
    {
        this.relationshipStore = relationshipStore;
        this.relationshipRecord = relationshipRecord;

        this.storeStatement = storeStatement;
    }

    @Override
    public long getId()
    {
        return relationshipRecord.getId();
    }

    @Override
    public int getType()
    {
        return relationshipRecord.getType();
    }

    @Override
    public long getStartNode()
    {
        return relationshipRecord.getFirstNode();
    }

    @Override
    public long getEndNode()
    {
        return relationshipRecord.getSecondNode();
    }

    @Override
    public long getOtherNode( long nodeId )
    {
        return relationshipRecord.getFirstNode() == nodeId ?
                relationshipRecord.getSecondNode() : relationshipRecord.getFirstNode();
    }

    @Override
    public PropertyCursor properties()
    {
        return storeStatement.acquirePropertyCursor( relationshipRecord.getNextProp() );
    }
}
