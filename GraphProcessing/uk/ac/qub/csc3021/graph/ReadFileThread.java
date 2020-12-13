package uk.ac.qub.csc3021.graph;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CyclicBarrier;

public class ReadFileThread extends Thread {

    private int startLine;
    private int endLine;
    private String fileName;
    private int[] sources;
    private int[] index;
    private int startEdge;
    private CyclicBarrier barrier;
    private int skipChars;

    public ReadFileThread(int startLine, int endLine, String fileName, int sources[], int[] index, CyclicBarrier barrier, int startEdge, int skipChars) {
        this.startLine = startLine;
        this.endLine = endLine;
        this.fileName = fileName;
        this.sources = sources;
        this.index = index;
        this.barrier = barrier;
        this.startEdge = startEdge;
        this.skipChars = skipChars;
    }

    @Override
    public void run() {

        try {
            InputStreamReader is = new InputStreamReader(new FileInputStream(fileName), "UTF-8");
            BufferedReader rd = new BufferedReader(is);

            rd.skip(skipChars);

            int sourcePosition = startEdge;

            String line = "";

            for (int i = startLine; i < endLine; ++i) {
                line = rd.readLine();
                if (line == null)
                    throw new Exception("premature end of file");
                String elm[] = line.split(" ");
                assert Integer.parseInt(elm[0]) == i : "Error in CSC file";
                index[i] = sourcePosition;
                for (int j = 1; j < elm.length; ++j) {
                    int src = Integer.parseInt(elm[j]);
                    sources[sourcePosition] = src;
                    sourcePosition++;
                }
            }

            barrier.await();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}