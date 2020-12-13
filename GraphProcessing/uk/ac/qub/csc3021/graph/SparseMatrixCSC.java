package uk.ac.qub.csc3021.graph;

import java.io.*;
import java.util.concurrent.CyclicBarrier;

// This class represents the adjacency matrix of a graph as a sparse matrix
// in compressed sparse columns format (CSC). The incoming edges for each
// vertex are listed.
public class SparseMatrixCSC extends SparseMatrix {
    int[] index;
    int[] sources;
    int num_vertices; // Number of vertices in the graph
    int num_edges;    // Number of edges in the graph


    public SparseMatrixCSC(String file, int numThreads) {
        try {
            readFile(file, numThreads);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int getNext(BufferedReader rd) throws Exception {
        String line = rd.readLine();
        if (line == null)
            throw new Exception("premature end of file");
        return Integer.parseInt(line);
    }

    void readFile(String inputFile, int numThreads) throws Exception {
        InputStream inputStream = new FileInputStream(inputFile);
        InputStreamReader is = new InputStreamReader(inputStream, "UTF-8");
        BufferedReader rd = new BufferedReader(is);

        rd.readLine();

        num_vertices = getNext(rd);
        num_edges = getNext(rd);

        rd.close();

        inputStream = new FileInputStream(inputFile);

        byte[] byteRead = new byte[8192];

        UTF8Processor proc = new UTF8Processor(num_vertices, num_edges, numThreads);

        System.out.println("vertices read: " + num_vertices);
        System.out.println("edges read: " + num_edges);

        long startPreProcess = System.nanoTime();
        while ((inputStream.read(byteRead)) != -1) {
            boolean end = proc.processBytes(byteRead);
            if(end) {
                break;
            }
        }

        System.out.println("vertices calculated: " + proc.lineNumber);
        System.out.println("edges calculated: " + proc.edgeNumber);

        index = new int[num_vertices + 1];
        sources = new int[num_edges];

        index[num_vertices] = num_edges;

        CyclicBarrier barrier = new CyclicBarrier(numThreads+1);

        for(int i = 0; i < numThreads-1; i++) {
            ReadFileThread thread = new ReadFileThread(proc.chunkStarts[i][0], proc.chunkStarts[i+1][0], inputFile, sources, index, barrier, proc.chunkStarts[i][1], proc.chunkStarts[i][2]);
            thread.start();
        }

        ReadFileThread thread = new ReadFileThread(proc.chunkStarts[numThreads-1][0], num_vertices, inputFile, sources, index, barrier, proc.chunkStarts[numThreads-1][1], proc.chunkStarts[numThreads-1][2]);
        thread.start();

        barrier.await();

        double timeToPreProcess = (double) (System.nanoTime() - startPreProcess) * 1e-9;
        System.err.println("Time in preprocess: " + timeToPreProcess + " seconds");

    }

    // Return number of vertices in the graph
    public int getNumVertices() {
        return num_vertices;
    }

    // Return number of edges in the graph
    public int getNumEdges() {
        return num_edges;
    }

    //TODO: Make sure this is correct method.
    // Auxiliary function for PageRank calculation
    public void calculateOutDegree(int outdeg[]) {
        for (int i = 0; i < num_edges; ++i) {
            outdeg[sources[i]]++;
        }
    }

    public void iterate(Relax relax) {
        for (int i = 0; i < num_vertices; ++i) {
            for (int j = index[i]; j < index[i + 1]; ++j) {
                relax.relax(sources[j], i);
            }
        }
    }

    public void iterate(Relax relax, int from, int to) {

        for (int i = from; i < to; ++i) {
            for (int j = index[i]; j < index[i + 1]; ++j) {
                relax.relax(sources[j], i);
            }
        }
    }
}

