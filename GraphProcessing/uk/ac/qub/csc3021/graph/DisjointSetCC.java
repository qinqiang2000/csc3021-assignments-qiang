package uk.ac.qub.csc3021.graph;

import java.util.concurrent.atomic.AtomicIntegerArray;

// Calculate the connected components using disjoint set data structure
// This algorithm only works correctly for undirected graphs
public class DisjointSetCC {
    public static int[] compute(SparseMatrix matrix) {
        long tm_start = System.nanoTime();

        final int n = matrix.getNumVertices();
        final AtomicIntegerArray parent = new AtomicIntegerArray(n);
        final boolean verbose = true;

        for (int i = 0; i < n; ++i) {
            // Each vertex is a set on their own

        }

        DSCCRelax DSCCrelax = new DSCCRelax(parent);

        double tm_init = (double) (System.nanoTime() - tm_start) * 1e-9;
        System.err.println("Initialisation: " + tm_init + " seconds");
        tm_start = System.nanoTime();

        ParallelContext context = ParallelContextHolder.get();

        // 1. Make pass over graph
        context.iterate(matrix, DSCCrelax);

        double tm_step = (double) (System.nanoTime() - tm_start) * 1e-9;
        if (verbose)
            System.err.println("processing time=" + tm_step + " seconds");
        tm_start = System.nanoTime();

        // Post-process the labels

        // 1. Count number of components
        //    and map component IDs to narrow domain
        int ncc = 0;
        int remap[] = new int[n];
        for (int i = 0; i < n; ++i)
            if (DSCCrelax.find(i) == i)
                remap[i] = ncc++;

        if (verbose)
            System.err.println("Number of components: " + ncc);

        // 2. Calculate size of each component
        int sizes[] = new int[ncc];
        for (int i = 0; i < n; ++i)
            ++sizes[remap[DSCCrelax.find(i)]];

        if (verbose)
            System.err.println("DisjointSetCC: " + ncc + " components");

        return sizes;
    }

    ;

    private static class DSCCRelax implements Relax {

        private static Node[] parents;

        DSCCRelax(AtomicIntegerArray parent_) {
            parents = new Node[parent_.length()];

            for (int i = 0; i < parent_.length(); i++) {
                parents[i] = new Node(i);
            }
        }

        public void relax(int src, int dst) {
            union(src, dst);
        }

        public int find(int x) {

            Node current = parents[x];

            while (current.parent != current) {
                current = current.parent;
            }

            return current.name;
        }

        private Node findNode(int x) {
            Node current = parents[x];

            while (current.parent != current) {
                current = current.parent;
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
