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
        rd.close();
        inputStream = new FileInputStream(inputFile);
        is = new InputStreamReader(inputStream, "UTF-8");
        rd = new BufferedReader(is);

        for(int i = 0; i < 3; i++) {
            rd.readLine();
        }

        DSCCRelax relax = new DSCCRelax(numVertices);

        for(int i = 0; i < numVertices; i++) {
            String line = rd.readLine();

            String[] split = line.split(" ");

            for (int j = 1; j < split.length; j++) {
                relax.relax(i, Integer.parseInt(split[j]));
            }
        }

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

