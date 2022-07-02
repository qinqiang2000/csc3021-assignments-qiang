package uk.ac.qub.csc3021.graph;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelContextSimple extends ParallelContext {
    CyclicBarrier cyclicBarrier;
    ExecutorService executor;

    public ParallelContextSimple(int num_threads_) {
        super(num_threads_);
        cyclicBarrier = new CyclicBarrier(num_threads_ + 1);
        executor = Executors.newFixedThreadPool(num_threads_);
    }

    public void terminate() {
        executor.shutdown();
    }

    // The iterate method for Q3 should create threads, which each process
    // one graph partition, then wait for them to complete.
    public void iterate(SparseMatrix matrix, Relax relax) {
        int numVertices = matrix.getNumVertices();
        int numThreads = getNumThreads();

        int chunkSize = (numVertices + numThreads - 1) / numThreads;

        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, numVertices);

            executor.execute(new ThreadSimple(start, end, cyclicBarrier, matrix, relax));
        }

        try {
            cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            System.out.println("thrown an exception!");
        }
    }

    private class ThreadSimple extends Thread {
        private int from, to;
        private SparseMatrix matrix;
        private Relax relax;
        private CyclicBarrier barrier;

        public ThreadSimple(int from, int to, CyclicBarrier barrier, SparseMatrix matrix, Relax relax) {
            this.from = from;
            this.to = to;
            this.matrix = matrix;
            this.relax = relax;
            this.barrier = barrier;
        }

        public void run() {
            try {
                matrix.iterate(relax, from, to);
                //System.out.println(getName() + " done: " + from +":" + to);
                barrier.await();
            } catch (Exception e) {
                System.out.println("thrown an exception!");
            }
        }
    }
}
