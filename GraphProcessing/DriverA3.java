/*
* Use command-line flag -ea for java VM to enable assertions.
*/
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

import uk.ac.qub.csc3021.graph.*;

// Main class with main() method. Performs the PageRank computation until
// convergence is reached.
class Driver {
   public static void main( String args[] ) {
	if( args.length < 6 ) {
	    System.err.println( "Usage: java Driver inputfile-COO inputfile-CSR inputfile-CSC algorithm num-threads outputfile" );
	    return;
	}

	String inputFileCOO = args[0];
	String inputFileCSR = args[1];
	String inputFileCSC = args[2];
	String algorithm = args[3];
	int num_threads = Integer.parseInt( args[4] );
	String outputFile = args[5];

	// Tell us what you're doing
	System.err.println( "Input file (CSC): " + inputFileCSC );
	System.err.println( "Algorithm: " + algorithm );
	System.err.println( "Number of threads: " + num_threads );
	System.err.println( "Output file: " + outputFile );

	long tm_start = System.nanoTime();

	SparseMatrix matrix;

	// Choose which format you want to read the graph in
	// matrix = new SparseMatrixCOO( inputFileCOO );
	//matrix = new SparseMatrixCSR( inputFileCSR );
	 matrix = new SparseMatrixCSC( inputFileCSC, num_threads );

	double tm_input = (double)(System.nanoTime() - tm_start) * 1e-9;
	System.err.println( "Reading input: " + tm_input + " seconds" );
	tm_start = System.nanoTime();

	// What facilities for parallel execution do we have?
	// ParallelContextHolder.set( new ParallelContextQ2( num_threads ) );
	ParallelContextHolder.set( new ParallelContextQ2(num_threads) );

	try {
	    if( algorithm.equalsIgnoreCase( "PR" ) ) {
		// Calculate PageRank values for the graph
		double PR[] = PageRank.compute( matrix );

		double tm_total = (double)(System.nanoTime() - tm_start) * 1e-9;
		System.err.println( "PageRank: total time: " + tm_total + " seconds" );
		tm_start = System.nanoTime();

		// Dump PageRank values to file
		writeToFile( outputFile, PR );

		double tm_write = (double)(System.nanoTime() - tm_start) * 1e-9;
		System.err.println( "Writing file: " + tm_write + " seconds" );
	    } else if( algorithm.equalsIgnoreCase( "CC" ) ) {
		// Calculate connected components of the graph
		int CC[] = ConnectedComponents.compute( matrix );

		double tm_total = (double)(System.nanoTime() - tm_start) * 1e-9;
		System.out.println( "Connected Components: total time: " + tm_total + " seconds" );
		tm_start = System.nanoTime();

		// Dump component sizes to file
		writeToFile( outputFile, CC );

		double tm_write = (double)(System.nanoTime() - tm_start) * 1e-9;
		System.out.println( "Writing file: " + tm_write + " seconds" );
	    } else if( algorithm.equalsIgnoreCase( "DS" ) ) {
		// Calculate connected components of the graph
		int CC[] = DisjointSetCC.compute( matrix );

		double tm_total = (double)(System.nanoTime() - tm_start) * 1e-9;
		System.out.println( "Disjoint Set: total time: " + tm_total + " seconds" );
		tm_start = System.nanoTime();

		// Dump component sizes to file
		writeToFile( outputFile, CC );

		double tm_write = (double)(System.nanoTime() - tm_start) * 1e-9;
		System.out.println( "Writing file: " + tm_write + " seconds" );
	    } else if( algorithm.equalsIgnoreCase( "OPT" ) ) {
			int CC[] = ConnectedComponents.compute( matrix );

			double tm_total = (double)(System.nanoTime() - tm_start) * 1e-9;
			System.out.println( "Connected Components: total time: " + tm_total + " seconds" );
			tm_start = System.nanoTime();

			// Dump component sizes to file
			writeToFile( outputFile, CC );

			double tm_write = (double)(System.nanoTime() - tm_start) * 1e-9;
			System.out.println( "Writing file: " + tm_write + " seconds" );
	    } else {
		System.err.println( "Unknown algorithm '" + algorithm + "'" );
		return;
	    }
	} finally {
	    ParallelContextHolder.get().terminate();
	}
	System.err.println( "All done" );
   }

   static void writeToFile( String file, double[] v ) {
	try {
	    OutputStreamWriter os
		= new OutputStreamWriter( new FileOutputStream( file ), "UTF-8" );
	    BufferedWriter wr = new BufferedWriter( os );
	    writeToBuffer( wr, v );
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
   static void writeToBuffer( BufferedWriter buf, double[] v ) {
	PrintWriter out = new PrintWriter( buf );
	for( int i=0; i < v.length; ++i )
	    out.println( i + " " + v[i] );
	out.close();
   }
   static void writeToFile( String file, int[] v ) {
	try {
	    OutputStreamWriter os
		= new OutputStreamWriter( new FileOutputStream( file ), "UTF-8" );
	    BufferedWriter wr = new BufferedWriter( os );
	    writeToBuffer( wr, v );
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
   static void writeToBuffer( BufferedWriter buf, int[] v ) {
	PrintWriter out = new PrintWriter( buf );
	for( int i=0; i < v.length; ++i )
	    out.println( i + " " + v[i] );
	out.close();
   }
}
