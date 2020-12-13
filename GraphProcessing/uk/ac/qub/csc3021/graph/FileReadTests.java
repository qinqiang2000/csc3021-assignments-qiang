package uk.ac.qub.csc3021.graph;

import java.io.*;
import java.util.concurrent.CyclicBarrier;

public class FileReadTests {

    public static void main(String[] args) throws Exception {

        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);

        InputStream inputStream = new FileInputStream(inputFile);
        InputStreamReader is = new InputStreamReader(inputStream, "UTF-8");
        BufferedReader rd = new BufferedReader(is);

        rd.readLine();

        int numVertices = getNext(rd);
        int numEdges = getNext(rd);


        rd.close();

        inputStream = new FileInputStream(inputFile);

        byte[] byteRead = new byte[8192];

        UTF8Processor proc = new UTF8Processor(numVertices, numEdges, numThreads);

        System.out.println("vertices read: " + numVertices);
        System.out.println("edges read: " + numEdges);

        long startPreProcess = System.nanoTime();
        while ((inputStream.read(byteRead)) != -1) {
            boolean end = proc.processBytes(byteRead);
            if(end) {
                break;
            }
        }

        System.out.println("vertices calculated: " + proc.lineNumber);
        System.out.println("edges calculated: " + proc.edgeNumber);

        int[] index = new int[numVertices + 1];
        int[] sources = new int[numEdges];

        index[numVertices] = numEdges;

        CyclicBarrier barrier = new CyclicBarrier(numThreads+1);

        for(int i = 0; i < numThreads-1; i++) {
            ReadFileThread thread = new ReadFileThread(proc.chunkStarts[i][0], proc.chunkStarts[i+1][0], inputFile, sources, index, barrier, proc.chunkStarts[i][1], proc.chunkStarts[i][2]);
            thread.start();
        }

        ReadFileThread thread = new ReadFileThread(proc.chunkStarts[numThreads-1][0], numVertices, inputFile, sources, index, barrier, proc.chunkStarts[numThreads-1][1], proc.chunkStarts[numThreads-1][2]);
        thread.start();

        barrier.await();

        double timeToPreProcess = (double) (System.nanoTime() - startPreProcess) * 1e-9;
        System.err.println("Time in preprocess: " + timeToPreProcess + " seconds");

    }


    static int getNext(BufferedReader rd) throws Exception {
        String line = rd.readLine();
        if (line == null)
            throw new Exception("premature end of file");
        return Integer.parseInt(line);
    }
}

