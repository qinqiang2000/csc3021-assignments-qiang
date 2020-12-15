package uk.ac.qub.csc3021.graph;

import java.io.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class FileReadTests {

    public static int[] compute(String inputFile, int numThreads) throws Exception {
        FileInputStream inputStream = new FileInputStream(inputFile);

        long fileSize = inputStream.getChannel().size();

        InputStreamReader is = new InputStreamReader(inputStream, "UTF-8");
        BufferedReader rd = new BufferedReader(is);
        rd.readLine();
        int numVertices = getNext(rd);

        rd.close();

        double tm_before_create = System.nanoTime();
        DSCCRelax relax = new DSCCRelax(numVertices);
        double tm_after_create = System.nanoTime();
        double tm_create_relax = (tm_after_create - tm_before_create) * 1e-9;
        System.out.println("Time to create relax: " + tm_create_relax);

        long chunkSize = (fileSize + numThreads - 1) / numThreads;

        ProcessThread[] tasks = new ProcessThread[numThreads];

        AtomicInteger integer = new AtomicInteger();

        for(int i = numThreads-1; i >= 1; i--) {
            long start = i * chunkSize;
            long end  = Math.min(start + chunkSize, fileSize);
            tasks[i] = new ProcessThread(start, end, integer, relax, inputFile, i, numVertices);
            tasks[i].start();
        }

        long end = Math.min(chunkSize, fileSize);

        int BUFFER_SIZE = 65536;

        try {
            long byteNumber = 0;
            inputStream = new FileInputStream(inputFile);

            byte[] startByte = new byte[1];

            int byteFound = 0;

            while(byteFound < 3) {
                inputStream.read(startByte);
                byteNumber++;

                if(startByte[0] == 0x0a) {
                    byteFound++;
                }
            }

            int i;
            byte[] byteBuffer = new byte[BUFFER_SIZE];
            boolean startOfLine = true;
            int currentNumber = 0;
            int currentVertex = 0;
            int unconverted;

            outerLoop:
            while (true) {
                inputStream.read(byteBuffer, 0, BUFFER_SIZE);
                for (i = 0; i < BUFFER_SIZE; i++) {

                    unconverted = byteBuffer[i];

                    if(unconverted < 48) {
                        if(!startOfLine) {
                            relax.relax(currentVertex, currentNumber);
                        }
                        else {
                            currentVertex = currentNumber;
                            startOfLine = false;
                        }

                        currentNumber = 0;

                        // handle other bytes....
                        if(unconverted == 10) {
                            if((byteNumber+i) > end) {
                                break outerLoop;
                            }
                            startOfLine = true;
                        }
                    }
                    else {
                        currentNumber = (currentNumber*10) + unconverted - 48;
                    }
                }

                byteNumber += BUFFER_SIZE;
            }

            synchronized (integer) {
                integer.incrementAndGet();
            }

            inputStream.close();
        }
        catch (Exception e) {

        }


        synchronized (integer) {
            while (integer.get() != numThreads) {
                integer.wait();
            }
        }

        double tm_after_file = System.nanoTime();

        double tm_to_file = (double)(tm_after_file - tm_after_create) * 1e-9;
        System.out.println("Time to process file: " + tm_to_file);

        // 1. Count number of components
        //    and map component IDs to narrow domain
        int ncc = 0;
        int remap[] = new int[numVertices];
        for (int i = 0; i < numVertices; ++i)
            if (relax.find(i) == i)
                remap[i] = ncc++;

        // 2. Calculate size of each component
        int sizes[] = new int[ncc];
        for (int i = 0; i < numVertices; ++i)
            ++sizes[remap[relax.find(i)]];

        double tm_after_mapping = System.nanoTime();

        double tm_do_postprocessing = (double)(tm_after_mapping - tm_after_file) * 1e-9;

        System.out.println("PostProcessing: " + tm_do_postprocessing);

        return sizes;
    }

    private static class ProcessThread extends Thread {

        private long start;
        private long end;
        private AtomicInteger integer;
        private DSCCRelax relax;
        private String inputFile;
        private int threadId;
        int finalVertex;

        public ProcessThread(long start, long end, AtomicInteger integer, DSCCRelax relax, String inputFile, int threadId, int numVertices) {
            this.start = start;
            this.end = end;
            this.integer = integer;
            this.relax = relax;
            this.inputFile = inputFile;
            this.threadId = threadId;
            this.finalVertex = numVertices-1;
        }

        private static final int BUFFER_SIZE = 65536;

        @Override
        public void run() {
            try {
                long byteNumber = start;
                InputStream inputStream = new FileInputStream(inputFile);

                byte[] startByte = new byte[1];

                inputStream.skip(start);
                boolean byteFound = false;

                while(!byteFound) {
                    inputStream.read(startByte);
                    byteNumber++;

                    if(startByte[0] == 0x0a) {
                        byteFound = true;
                    }
                }

                int i;
                byte[] byteBuffer = new byte[BUFFER_SIZE];
                boolean startOfLine = true;
                int currentNumber = 0;
                int currentVertex = 0;
                int unconverted;

outerLoop:
                while (true) {
                    inputStream.read(byteBuffer, 0, BUFFER_SIZE);
                    for (i = 0; i < BUFFER_SIZE; i++) {

                        unconverted = byteBuffer[i];

                        if(unconverted < 48) {
                            if(!startOfLine) {
                                relax.relax(currentVertex, currentNumber);
                            }
                            else {
                                currentVertex = currentNumber;
                                startOfLine = false;
                            }

                            currentNumber = 0;

                            // handle other bytes....
                            if(unconverted == 10) {
                                if((byteNumber+i) > end || currentVertex == finalVertex) {
                                    break outerLoop;
                                }
                                startOfLine = true;
                            }
                        }
                        else {
                            currentNumber = (currentNumber*10) + unconverted - 48;
                        }
                    }

                    byteNumber += BUFFER_SIZE;
                }

                synchronized (integer) {
                    integer.incrementAndGet();
                    integer.notify();
                }

                inputStream.close();
            }
            catch (Exception e) {

            }
        }

    }


    static int getNext(BufferedReader rd) throws Exception {
        String line = rd.readLine();
        if (line == null)
            throw new Exception("premature end of file");
        return Integer.parseInt(line);
    }

    private static class DSCCRelax {

        private Node[] parents;

        DSCCRelax(int length) {
            parents = new DSCCRelax.Node[length];

            for (int i = 0; i < length; i++) {
                parents[i] = new Node(i);
            }
        }

        public void relax(int src, int dst) {
            union(src, dst);
        }

        public int find(int x) {

            Node current = parents[x];

            while (current.parent != current) {
                current = current.parent.parent;
            }

            return current.name;
        }

        private Node findNode(int x) {
            Node current = parents[x];

            while (current.parent != current) {
                current = current.parent.parent;
            }

            return current;
        }

        private void union(int x, int y) {

            Node xSet = findNode(x);
            Node ySet = findNode(y);

            if(xSet.name == ySet.name)
                return;

            if (ySet.name < xSet.name) {
                xSet.parent = ySet;
            } else {
                ySet.parent = xSet;
            }
        }

        private class Node {

            public Node parent;
            public int name;

            public Node(int name) {
                this.name = name;
                this.parent = this;
            }
        }
    }

}

