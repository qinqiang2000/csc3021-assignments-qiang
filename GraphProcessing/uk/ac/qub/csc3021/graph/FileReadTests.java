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
        
        // Start threads in reverse order as the ones processing the end of the file have the most work to do skipping to the start
        for(int i = numThreads-1; i >= 0; i--) {
            long start = i * chunkSize;
            long end  = Math.min(start + chunkSize, fileSize);
            tasks[i] = new ProcessThread(start, end, integer, relax, inputFile, i, numVertices);
            tasks[i].start();
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
                
                //Skip to the start of the next line
                while(!byteFound) {
                    inputStream.read(startByte);
                    byteNumber++;
                    
                    // 0x0a is line feed.
                    if(startByte[0] == 0x0a) {
                        byteFound = true;
                    }
                }

                int i;
                
                // Where the bytes from the file will be kept during processing
                byte[] byteBuffer = new byte[BUFFER_SIZE];

                // The flag that there has been a line-feed, and therefor the next number will be the vertex
                boolean startOfLine = true;
                
                // The current number being read.
                int currentNumber = 0;

                // The vertex of the current line (first number in the line)
                int currentVertex = 0;

                // The raw int value of the UTF-8
                int unconverted;

// We use a loop label here so we can break out to it on line 172.
outerLoop:
                while (true) {
                    inputStream.read(byteBuffer, 0, BUFFER_SIZE);
                    for (i = 0; i < BUFFER_SIZE; i++) {

                        unconverted = byteBuffer[i];
                        
                        // Less than 48 means not a digit
                        if(unconverted < 48) {
                            // If we're in here, that means that we've come to the end of a number.
                            // The current number must be processed.
                            // If its the first number in the line, its the vertex number, and must be saved for later.
                            // If it is NOT the first number in the line, its a source, and relax should be called.


                            // if this is not the first number in the line, the current number must be a source. Therefore run union.
                            if(!startOfLine) {
                                relax.relax(currentVertex, currentNumber);
                            }
                            else {
                            // otherwise the current number must be the vertex number.
                                currentVertex = currentNumber;
                                startOfLine = false;
                            }
                            
                            // Throw away the current number, as it has finished processing.
                            currentNumber = 0;

                            // 10 is line-feed (end of a line)
                            if(unconverted == 10) {

                                // if we're past the end of the target chunk (but at the end of a line)
                                // or we've already read in the final vertex in the file and have finished its line
                                // we can stop
                                if((byteNumber+i) > end || currentVertex == finalVertex) {

                                    //break out of the grandparent loop
                                    break outerLoop;
                                }

                                // Flag that the next number is going to be a vertex, as itll be the first number in a new line
                                startOfLine = true;
                            }
                        }
                        else {
                            // convert the utf-8 char to a digit by reducing it by 48
                            // add to existing number by shifting the digits left by multiplying by 10
                            currentNumber = (currentNumber*10) + unconverted - 48;
                        }
                    }

                    byteNumber += BUFFER_SIZE;
                }

                //notify main thread of update.
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

