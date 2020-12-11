package uk.ac.qub.csc3021.graph;

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

// This class represents the adjacency matrix of a graph as a sparse matrix
// in coordinate format (COO)
public class SparseMatrixCOO extends SparseMatrix {
	int sources[];
	int destinations[];
    int num_vertices; // Number of vertices in the graph
    int num_edges;    // Number of edges in the graph

    public SparseMatrixCOO( String file ) {
	try {
	    InputStreamReader is
		= new InputStreamReader( new FileInputStream( file ), "UTF-8" );
	    BufferedReader rd = new BufferedReader( is );
	    readFile( rd );
	} catch( FileNotFoundException e ) {
	    System.err.println( "File not found: " + e );
	    return;
	} catch( UnsupportedEncodingException e ) {
	    System.err.println( "Unsupported encoding exception: " + e );
	    return;
	} catch( Exception e ) {
	    System.err.println( "Exception: " + e );
	    return;
	}
    }

    int getNext( BufferedReader rd ) throws Exception {
	String line = rd.readLine();
	if( line == null )
	    throw new Exception( "premature end of file" );
	return Integer.parseInt( line );
    }

    void getNextPair( BufferedReader rd, int pair[] ) throws Exception {
	String line = rd.readLine();
	if( line == null )
	    throw new Exception( "premature end of file" );
	StringTokenizer st = new StringTokenizer( line );
	pair[0] = Integer.parseInt( st.nextToken() );
	pair[1] = Integer.parseInt( st.nextToken() );
    }

    void readFile( BufferedReader rd ) throws Exception {
	String line = rd.readLine();
	if( line == null )
	    throw new Exception( "premature end of file" );
	if( !line.equalsIgnoreCase( "COO" ) )
	    throw new Exception( "file format error -- header" );

	num_vertices = getNext(rd);
	num_edges = getNext(rd);
	sources = new int[num_edges];
	destinations = new int[num_edges];

	int edge[] = new int[2];
	for( int i=0; i < num_edges; ++i ) {
	    getNextPair( rd, edge );
		sources[i] = edge[0];
		destinations[i] = edge[1];
	}
    }

    // Return number of vertices in the graph
    public int getNumVertices() { return num_vertices; }

    // Return number of edges in the graph
    public int getNumEdges() { return num_edges; }

    // Auxiliary function for PageRank calculation
    public void calculateOutDegree( int outdeg[] ) {
		for (int i = 0; i < num_edges; i++) {
			outdeg[sources[i]]++;
		}
    }

    public void iterate( Relax relax ) {
		for (int i = 0; i < num_edges; i++) {
			relax.relax(sources[i], destinations[i]);
		}
    }

    public void iterate( Relax relax, int from, int to ) {
	// Only implement for parallel/concurrent processing
	// if you find it useful
    }
}
