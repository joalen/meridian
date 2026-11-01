package com.joalen.pack;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes, for every object in a repository's pack, the time it ENTERED history:
 *
 *   commit: its own committer time
 *   tree / blob: the committer time of the earliest commit that reaches it
 *   tag: the tagger time (if the tag has one)
 *
 * Commits are processed oldest first (ties broken by sha so the result is
 * deterministic). The first commit to reach an object claims it, and a tree that
 * was already claimed is not entered again: everything beneath it was claimed by
 * an earlier-or-equal commit already. That keeps the walk linear in the number
 * of objects instead of (commits x tree size).
 *
 * Objects no commit reaches get 0. No Hadoop dependencies.
 */
public final class ObjectTimes {

    /** The raw bytes of every commit and tree (and tag) in the repository, by sha. */
    public static final class Contents {
        public final Map<String, byte[]> commits = new HashMap<>();
        public final Map<String, byte[]> trees = new HashMap<>();
        public final Map<String, byte[]> tags = new HashMap<>();
    }

    /** Result of compute(): sha -> epoch seconds, plus a few counts for sanity checks. */
    public static final class Result {
        public final Map<String, Long> time = new HashMap<>();
        public int commits;
        public int unparsableCommits;
    }

    private static final class Commit {
        String sha;
        String treeSha;
        long time;
    }

    public static Result compute(Contents c) {
        Result result = new Result();

        List<Commit> commits = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : c.commits.entrySet()) {
            Commit commit = parseCommit(e.getKey(), e.getValue());
            if (commit == null) {
                result.unparsableCommits++;
                continue;
            }
            commits.add(commit);
        }
        result.commits = commits.size();

        Collections.sort(commits, new Comparator<Commit>() {
            @Override
            public int compare(Commit a, Commit b) {
                int t = Long.compare(a.time, b.time);
                return t != 0 ? t : a.sha.compareTo(b.sha);
            }
        });

        Set<String> visitedTrees = new HashSet<>();
        for (Commit commit : commits) {
            result.time.put(commit.sha, commit.time);
            claimTree(commit.treeSha, commit.time, c, result.time, visitedTrees);
        }

        for (Map.Entry<String, byte[]> e : c.tags.entrySet()) {
            Long t = parseIdentityTime(e.getValue(), "tagger ");
            if (t != null) {
                result.time.put(e.getKey(), t);
            }
        }
        return result;
    }

    /** Iterative walk (no recursion, so deep trees cannot overflow the stack). */
    private static void claimTree(String rootTree, long time, Contents c,
                                  Map<String, Long> out, Set<String> visited) {
        List<String> stack = new ArrayList<>();
        stack.add(rootTree);
        while (!stack.isEmpty()) {
            String treeSha = stack.remove(stack.size() - 1);
            if (!visited.add(treeSha)) {
                continue;             // already claimed with all its contents
            }
            out.put(treeSha, time);
            byte[] data = c.trees.get(treeSha);
            if (data == null) {
                continue;             // not in this pack
            }
            parseTree(data, stack, out, time);
        }
    }

    /**
     * Tree entry: "<mode> <name>\0" followed by 20 raw sha bytes.
     * Mode 40000 is a subtree (pushed for walking), 160000 is a submodule
     * pointer (a commit in another repository, skipped), anything else is a blob.
     */
    private static void parseTree(byte[] data, List<String> subtrees, Map<String, Long> out, long time) {
        int i = 0;
        while (i < data.length) {
            int sp = i;
            while (data[sp] != ' ') sp++;
            String mode = new String(data, i, sp - i, StandardCharsets.US_ASCII);

            int nul = sp + 1;
            while (data[nul] != 0) nul++;

            String sha = hex(data, nul + 1, 20);
            i = nul + 21;

            if (mode.equals("40000")) {
                subtrees.add(sha);
            } else if (!mode.equals("160000")) {
                if (!out.containsKey(sha)) {
                    out.put(sha, time);
                }
            }
        }
    }

    private static Commit parseCommit(String sha, byte[] data) {
        String text = new String(data, StandardCharsets.ISO_8859_1);
        int headerEnd = text.indexOf("\n\n");
        String header = headerEnd < 0 ? text : text.substring(0, headerEnd);

        String tree = null;
        for (String line : header.split("\n")) {
            if (line.startsWith("tree ")) {
                tree = line.substring(5).trim();
                break;
            }
        }
        Long t = parseIdentityTime(data, "committer ");
        if (tree == null || t == null) {
            return null;
        }
        Commit commit = new Commit();
        commit.sha = sha;
        commit.treeSha = tree;
        commit.time = t;
        return commit;
    }

    /**
     * Reads the epoch seconds from a header line like
     *   committer Name <email> 1389123456 -0500
     * Names may contain anything but '>' ends the email, so parse from there.
     */
    static Long parseIdentityTime(byte[] data, String prefix) {
        String text = new String(data, StandardCharsets.ISO_8859_1);
        int headerEnd = text.indexOf("\n\n");
        String header = headerEnd < 0 ? text : text.substring(0, headerEnd);
        for (String line : header.split("\n")) {
            if (line.startsWith(prefix)) {
                int gt = line.lastIndexOf('>');
                if (gt < 0) return null;
                String[] rest = line.substring(gt + 1).trim().split("\\s+");
                if (rest.length < 1) return null;
                try {
                    return Long.parseLong(rest[0]);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private static String hex(byte[] b, int off, int len) {
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = off; i < off + len; i++) {
            sb.append(Character.forDigit((b[i] >> 4) & 0xf, 16)).append(Character.forDigit(b[i] & 0xf, 16));
        }
        return sb.toString();
    }

    private ObjectTimes() {}
}
