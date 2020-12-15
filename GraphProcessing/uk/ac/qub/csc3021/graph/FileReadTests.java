package uk.ac.qub.csc3021.graph;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class FileReadTests {

    public static int[] compute(String inputFile) throws Exception {

        InputStream inputStream = new FileInputStream(inputFile);
        InputStreamReader is = new InputStreamReader(inputStream, "UTF-8");
        BufferedReader rd = new BufferedReader(is);
        rd.readLine();
        int numVertices = getNext(rd);
        int numEdges = getNext(rd);

        double tm_before_create = System.nanoTime();
        DSCCRelax relax = new DSCCRelax(numVertices);
        double tm_after_create = System.nanoTime();
        double tm_create_relax = (double)(tm_after_create - tm_before_create) * 1e-9;
        System.out.println("Time to create relax: " + tm_create_relax);

        for(int i = 0; i < numVertices; i++) {
            String line = rd.readLine();

            String[] split = line.split(" ");

            for (int j = 1; j < split.length; j++) {
                relax.relax(i, Integer.parseInt(split[j]));
            }
        }

        double tm_after_file = System.nanoTime();
        double tm_do_file = (double)(tm_after_file - tm_after_create) * 1e-9;

        System.out.println("Time to do file: " + tm_do_file);



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


    static int getNext(BufferedReader rd) throws Exception {
        String line = rd.readLine();
        if (line == null)
            throw new Exception("premature end of file");
        return Integer.parseInt(line);
    }

    private static class DSCCRelax implements Relax {

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

            List<Node> nodes = new ArrayList<>();


            while (current.parent != current) {
                current = current.parent;
            }

            for (Node node:
                    nodes) {
                node.parent = current;
            }

            return current.name;
        }

        private Node findNode(int x) {
            Node current = parents[x];

            List<Node> nodes = new ArrayList<>();

            while (current.parent != current) {
                nodes.add(current);
                current = current.parent;
            }

            for (Node node:
                    nodes) {
                node.parent = current;
            }

            return current;
        }

        private boolean sameSet(int x, int y) {
            return find(x) == find(y);
        }

        private void union(int x, int y) {

            if (sameSet(x, y))
                return;

            Node xSet = findNode(x);
            Node ySet = findNode(y);

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

