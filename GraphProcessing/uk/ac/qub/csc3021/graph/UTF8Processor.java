package uk.ac.qub.csc3021.graph;

import java.io.UnsupportedEncodingException;

public class UTF8Processor {

    public int numVertices;
    public int numEdges;
    public int targetChunkSize;
    public int numThreads;
    public int threadId = 1;
    public int nextChunkStart;
    public int lastLineEdge = 0;

    public int charNumber = 0;
    public int lastLineChar = 0;

    public int[][] chunkStarts;

    public UTF8Processor(int numVertices, int numEdges, int numThreads) {
        this.numVertices = numVertices;
        this.numEdges = numEdges;
        this.numThreads = numThreads;

        targetChunkSize = (numEdges + numThreads - 1) / numThreads;
        nextChunkStart = targetChunkSize;

        chunkStarts = new int[numThreads][];

        chunkStarts[0] = new int[3];
    }

    public int lineNumber = -3;
    public int edgeNumber = 0;

    public boolean processBytes(byte[] bytes) throws UnsupportedEncodingException {

        for (int i = 0; i < 8192; i++) {

            charNumber++;

            if (bytes[i] == 0x0a) {
                lineNumber++;
                lastLineEdge = edgeNumber;
                lastLineChar = charNumber;
                if(lineNumber == 0) {
                    chunkStarts[0] = new int[3];
                    chunkStarts[0][2] = charNumber;
                }

                if(lineNumber == numVertices+3) {
                    return false;
                }
            }

            if (bytes[i] == 0x20) {
                edgeNumber++;
                if(edgeNumber > nextChunkStart) {
                    nextChunkStart += targetChunkSize;
                    chunkStarts[threadId] = new int[3];
                    chunkStarts[threadId][0] = lineNumber;
                    chunkStarts[threadId][1] = lastLineEdge;
                    chunkStarts[threadId][2] = lastLineChar;
                    threadId++;
                    if(threadId == numThreads) {
                        return true;
                    }
                }
            }
        }

        return false;
    }




    // 0x20 - space
    //0x0a = linefeed
}
