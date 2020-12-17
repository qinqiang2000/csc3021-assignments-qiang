/*
* Use command-line flag -ea for java VM to enable assertions.
*/
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import uk.ac.qub.csc3021.graph.*;

// Main class with main() method. Performs the PageRank computation until
// convergence is reached.
class Driver {
   public static void main( String args[] ) throws Exception {
		String inputFileCSC = args[2];
		int num_threads = Integer.parseInt( args[4] );
		String outputFile = args[5];

		long tm_start = System.nanoTime();

		int CC[] = DisjointSetCalculator.compute(inputFileCSC, num_threads);
		writeToFile( outputFile, CC );

	   long timeEnd = System.nanoTime();

	   double timeTaken = (timeEnd-tm_start) *  1e-9;
	   System.out.println("time taken: " + timeTaken);
	   System.exit(0);
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
