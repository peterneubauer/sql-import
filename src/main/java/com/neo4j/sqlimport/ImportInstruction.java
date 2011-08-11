package com.neo4j.sqlimport;

import java.util.HashMap;

import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

public abstract class ImportInstruction
{

    protected String statementStart;
    protected Field[] names;

    private static final String COMMA = ",";

    public ImportInstruction(Field[] fields, String statementStart) {
        names = fields;
        this.statementStart = statementStart;
        
    }
    
    public String getStatementStart()
    {
        return statementStart;
    }

    public Field[] getNames()
    {
        return names;
    }

    public String[] parse( String substring )
    {
        String[] values = new String[names.length];
        try
        {
            String line = substring.trim();

            for ( int i = 0; i < names.length; i++ )
            {
                StringBuffer value = new StringBuffer();
                Field field = names[i];
                boolean lastField = ( i == names.length - 1 );
                switch ( field.type )
                {
                case INTEGER:
                    int indexOf = line.indexOf( lastField ? ");" : COMMA, 2 );
                    value.append( line.subSequence( 1, indexOf ) );
                    line = line.substring( indexOf );
                    break;
                case STRING:
                    int indexOf2 = line.indexOf( lastField ? "');" : "',", 2 );
                    value.append( line.substring( 2, indexOf2 ) );
                    line = line.substring( indexOf2 + 1 );
                    break;
                case INTEGER_AS_STRING:
                    int indexOf3 = line.indexOf( lastField ? "');" : "',", 2 );
                    value.append( line.substring( 2, indexOf3 ) );
                    line = line.substring( indexOf3 + 1 );
                    break;
                }
                values[i] = value.toString();
            }
        }
        catch ( Exception e )
        {
            System.out.println( "Could not parse " + substring );
            // e.printStackTrace();
        }
        return values;

    }

    abstract void createData( BatchInserter neo, BatchInserterIndexProvider indexProvider, HashMap<String, Object> values );

}
