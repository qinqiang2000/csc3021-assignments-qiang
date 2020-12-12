package uk.ac.qub.csc3021.graph;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ParallelContextQ2 extends ParallelContext {

    public ParallelContextQ2(int num_threads_) {
        super(num_threads_);
    }



    public void terminate() {
    }

    // The iterate method for Q2 should create threads, which each process
    // one graph partition, then wait for them to complete.
    public void iterate(SparseMatrix matrix, Relax relax) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(getNumThreads() + 1);
        int numVertices = matrix.getNumVertices();
        int numThreads = getNumThreads();

        int chunkSize = (numVertices + getNumThreads() - 1) / numThreads;

        ThreadQ2[] tasks = new ThreadQ2[numThreads];

        for(int i = 0; i < numThreads; i++) {

            int start = i * chunkSize;
            int end  = Math.min(start + chunkSize, numVertices);

            tasks[i] = new ThreadQ2(start, end, cyclicBarrier, matrix, relax);
            tasks[i].start();
        }

        try {
            cyclicBarrier.await();
        }
        catch (InterruptedException e) {
            System.out.println("fuck off");
        }
        catch (BrokenBarrierException e) {
            System.out.println("fuck off");
        }
    }




    private class ThreadQ2 extends Thread {

        private int from, to;
        private SparseMatrix matrix;
        private Relax relax;
        private CyclicBarrier barrier;

        public ThreadQ2(int from, int to, CyclicBarrier barrier, SparseMatrix matrix, Relax relax) {
            this.from = from;
            this.to = to;
            this.matrix = matrix;
            this.relax = relax;
            this.barrier = barrier;
        }


        public void run() {
            matrix.iterate( relax, from, to );

            try {
                barrier.await();
            }
            catch (InterruptedException e) {
                System.out.println("fuck off");
            }
            catch (BrokenBarrierException e) {
                System.out.println("fuck off");
            }
        }
    }
}
