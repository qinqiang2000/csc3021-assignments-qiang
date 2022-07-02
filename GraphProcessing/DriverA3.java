/*
 * Use command-line flag -ea for java VM to enable assertions.
 */

import uk.ac.qub.csc3021.graph.*;

import java.io.*;

// Main class with main() method. Performs the PageRank computation until
// convergence is reached.
// multi-thread version using the CSC sparse matrix format
class Driver {
    public static void main(String args[]) {
        if (args.length < 4) {
            System.out.println("Usage: java Driver inputfile-CSC algorithm outputfile num-threads");
            return;
        }

        String inputFileCSC = args[0];
        String algorithm = args[1];
        String outputFile = args[2];
        int num_threads = Integer.parseInt(args[3]);

        // Tell us what you're doing
        System.out.println("Input file (CSC): " + inputFileCSC);
        System.out.println("Algorithm: " + algorithm);
        System.out.println("Output file: " + outputFile);
        System.out.println("Number of threads: " + num_threads);

        long tm_start = System.nanoTime();

        SparseMatrix matrix;

        matrix = new SparseMatrixCSC(inputFileCSC);

        double tm_input = (double) (System.nanoTime() - tm_start) * 1e-9;
        System.out.println("Reading input: " + tm_input + " seconds");
        tm_start = System.nanoTime();

        // What facilities for parallel execution do we have?
        ParallelContextHolder.set(new ParallelContextSimple(num_threads));

        try {
            if (algorithm.equalsIgnoreCase("PR")) {
                // Calculate PageRank values for the graph
                double PR[] = PageRank.compute(matrix);

                double tm_total = (double) (System.nanoTime() - tm_start) * 1e-9;
                System.out.println("PageRank: total time: " + tm_total + " seconds");
                tm_start = System.nanoTime();

                // Dump PageRank values to file
                writeToFile(outputFile, PR);

                double tm_write = (double) (System.nanoTime() - tm_start) * 1e-9;
                System.out.println("Writing file: " + tm_write + " seconds");
            } else if (algorithm.equalsIgnoreCase("CC")) {
                // Calculate connected components of the graph
                int CC[] = ConnectedComponents.compute(matrix);

                double tm_total = (double) (System.nanoTime() - tm_start) * 1e-9;
                System.out.println("Connected Components: total time: " + tm_total + " seconds");
                tm_start = System.nanoTime();

                // Dump component sizes to file
                writeToFile(outputFile, CC);

                double tm_write = (double) (System.nanoTime() - tm_start) * 1e-9;
                System.out.println("Writing file: " + tm_write + " seconds");
            } else if (algorithm.equalsIgnoreCase("DS")) {
                // Calculate connected components of the graph
                int CC[] = DisjointSetCC.compute(matrix);

                double tm_total = (double) (System.nanoTime() - tm_start) * 1e-9;
                System.out.println("Disjoint Set: total time: " + tm_total + " seconds");
                tm_start = System.nanoTime();

                // Dump component sizes to file
                writeToFile(outputFile, CC);

                double tm_write = (double) (System.nanoTime() - tm_start) * 1e-9;
                System.out.println("Writing file: " + tm_write + " seconds");
            } else if (algorithm.equalsIgnoreCase("OPT")) {
                // TODO
                System.out.println("Need to make a choice: DS or CC?");
            } else {
                System.out.println("Unknown algorithm '" + algorithm + "'");
                return;
            }
        } finally {
            ParallelContextHolder.get().terminate();
        }
        System.out.println("All done");
    }

    static void writeToFile(String file, double[] v) {
        try {
            OutputStreamWriter os
                = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            BufferedWriter wr = new BufferedWriter(os);
            writeToBuffer(wr, v);
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e);
            return;
        } catch (UnsupportedEncodingException e) {
            System.out.println("Unsupported encoding exception: " + e);
            return;
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            return;
        }
    }

    static void writeToBuffer(BufferedWriter buf, double[] v) {
        PrintWriter out = new PrintWriter(buf);
        for (int i = 0; i < v.length; ++i)
            out.println(i + " " + v[i]);
        out.close();
    }

    static void writeToFile(String file, int[] v) {
        try {
            OutputStreamWriter os
                = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            BufferedWriter wr = new BufferedWriter(os);
            writeToBuffer(wr, v);
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e);
            return;
        } catch (UnsupportedEncodingException e) {
            System.out.println("Unsupported encoding exception: " + e);
            return;
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            return;
        }
    }

    static void writeToBuffer(BufferedWriter buf, int[] v) {
        PrintWriter out = new PrintWriter(buf);
        for (int i = 0; i < v.length; ++i)
            out.println(i + " " + v[i]);
        out.close();
    }
}
