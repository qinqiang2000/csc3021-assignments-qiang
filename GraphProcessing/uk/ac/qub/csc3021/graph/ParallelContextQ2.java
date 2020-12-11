package uk.ac.qub.csc3021.graph;

public class ParallelContextQ2 extends ParallelContext {
    private class ThreadQ2 extends Thread {

	public void run() {
	}
    };
    
    public ParallelContextQ2( int num_threads_ ) {
	super( num_threads_ );
    }

    public void terminate() { }

    // The iterate method for Q2 should create threads, which each process
    // one graph partition, then wait for them to complete.
    public void iterate( SparseMatrix matrix, Relax relax ) {
	// use matrix.iterate( relax, from, to ); in each thread
    }
}
