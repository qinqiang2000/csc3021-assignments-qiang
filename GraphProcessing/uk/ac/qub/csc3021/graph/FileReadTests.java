package uk.ac.qub.csc3021.graph;

import java.io.*;

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
            proc.processBytes(byteRead);
        }

        System.out.println("vertices calculated: " + proc.lineNumber);
        System.out.println("edges calculated: " + proc.edgeNumber);

        double timeToPreProcess = (double) (System.nanoTime() - startPreProcess) * 1e-9;
        System.err.println("Time in preprocess: " + timeToPreProcess + " seconds");

        for(int i = 0; i < numThreads; i++) {

            System.out.println("Thread " + i + ": " + proc.chunkStops[i]);

        }

        System.out.println("finished!");
    }

    //3072630



    static int getNext(BufferedReader rd) throws Exception {
        String line = rd.readLine();
        if (line == null)
            throw new Exception("premature end of file");
        return Integer.parseInt(line);
    }
}

class UTF8Processor {

    private int numVertices;
    private int numEdges;
    private int targetChunkSize;
    private int numThreads;
    private int threadId = 0;
    private int nextChunkStop;

    public int[] chunkStops;


    public UTF8Processor(int numVertices, int numEdges, int numThreads) {
        this.numVertices = numVertices;
        this.numEdges = numEdges;
        this.numThreads = numThreads;

        targetChunkSize = (numEdges + numThreads - 1) / numThreads;
        nextChunkStop = targetChunkSize;

        chunkStops = new int[numThreads];

        chunkStops[numThreads-1] = numVertices;
    }

    public int lineNumber = 0;
    public int edgeNumber = 0;

    public void processBytes(byte[] bytes) throws UnsupportedEncodingException {

        for (int i = 0; i < 8192; i++) {

            if (bytes[i] == 0x0a) {
                lineNumber++;

                if(lineNumber == numVertices+3) {
                    return;
                }
            }

            if (bytes[i] == 0x20) {
                edgeNumber++;
                if(edgeNumber > nextChunkStop) {
                    nextChunkStop += targetChunkSize;
                    chunkStops[threadId++] = lineNumber;
                }
            }
        }
    }

    // 0x20 - space
    //0x0a = linefeed
}
