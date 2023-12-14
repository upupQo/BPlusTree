package ds.bplus.bptree;

import ds.bplus.util.InvalidBTreeStateException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@SuppressWarnings({"WeakerAccess", "unused"})
@Slf4j
public class BPlusTreePerformanceCounter {
    private int totalNodeReads;
    private int totalInternalNodeReads;
    private int totalLeafNodeReads;
    private int totalOverflowReads;

    private int totalNodeWrites;
    private int totalInternalNodeWrites;
    private int totalLeafNodeWrites;
    private int totalOverflowWrites;


    private int totalInsertionReads;
    private int totalDeletionReads;
    private int totalSearchReads;
    private int totalRangeQueryReads;
    private int totalInsertionWrites;
    private int totalDeletionWrites;
    private int totalSearchWrites;
    private int totalRangeQueryWrites;

    private int pageReads;
    private int pageWrites;

    private int pageInternalReads;
    private int pageLeafReads;
    private int pageOverflowReads;

    private int pageInternalWrites;
    private int pageLeafWrites;
    private int pageOverflowWrites;

    private int totalInsertions;
    private int totalDeletions;
    private int totalSearches;
    private int totalRangeQueries;

    private int totalSplits;
    private int totalRootSplits;
    private int totalInternalNodeSplits;
    private int totalLeafSplits;


    private int totalPages;
    private int totalOverflowPages;
    private int totalInternalNodes;
    private int totalLeaves;

    private int totalInternalNodeDeletions;
    private int totalLeafNodeDeletions;
    private int totalOverflowPagesDeletions;

    private boolean trackIO;
    private BPlusTree bt = null;

    public BPlusTreePerformanceCounter(boolean trackIO) {
        this.trackIO = trackIO;
        resetAllMetrics();
    }

    public void setTrackIO(boolean trackIO) {
        this.trackIO = trackIO;
    }

    void setBTree(BPlusTree bt) {
        this.bt = bt;
    }

    void incrementTotalPages() {
        if(trackIO) {
            totalPages++;
        }
    }

    void incrementTotalOverflowPages() {
        if(trackIO) {
            totalOverflowPages++;
            incrementTotalPages();
        }
    }

    void incrementTotalInternalNodes() {
        if(trackIO) {
            totalInternalNodes++;
            incrementTotalPages();
        }
    }

    void incrementTotalLeaves() {
        if(trackIO) {
            totalLeaves++;
            incrementTotalPages();
        }
    }

    private void incrementTotalNodeReads() {
        if(trackIO) {
            totalNodeReads++;
        }
    }

    private void incrementTotalNodeWrites() {
        if(trackIO) {
            totalNodeWrites++;
        }
    }

    void incrementTotalInsertions() {
        if(trackIO) {
            totalInsertions++;
        }
    }

    private void incrementTotalDeletions() {
        if(trackIO) {
            totalDeletions++;
        }
    }

    void incrementTotalInternalNodeDeletions() {
        if(trackIO) {
            totalInternalNodeDeletions++;
            incrementTotalDeletions();
        }
    }

    public void incrementTotalLeafNodeDeletions() {
        if(trackIO) {
            totalLeafNodeDeletions++;
            incrementTotalDeletions();
        }
    }

    public void incrementTotalOverflowPageDeletions() {
        if(trackIO) {
            totalOverflowPagesDeletions++;
            incrementTotalDeletions();
        }
    }

    void incrementTotalSearches() {
        if(trackIO) {
            totalSearches++;
        }
    }

    void incrementTotalRangeQueries() {
        if(trackIO) {
            totalRangeQueries++;
        }
    }

    private void incrementTotalSplits() {
        if(trackIO) {
            totalSplits++;
        }
    }

    void incrementRootSplits() {
        if(trackIO) {
            totalRootSplits++;
            incrementTotalSplits();
        }
    }

    void incrementInternalNodeSplits() {
        if(trackIO) {
            totalInternalNodeSplits++;
            incrementTotalSplits();
        }
    }

    void incrementTotalLeafSplits() {
        if(trackIO) {
            totalLeafSplits++;
            incrementTotalSplits();
        }
    }

    private void incrementPageReads() {
        if(trackIO) {
            pageReads++;
        }
    }

    private void incrementPageWrites() {
        if(trackIO) {
            pageWrites++;
        }
    }

    public void startPageTracking() {
        setDefaults();
    }

    private void setDefaults() {
        pageReads = 0;
        pageWrites = 0;
        pageInternalReads = 0;
        pageLeafReads = 0;
        pageOverflowReads = 0;

        pageInternalWrites = 0;
        pageLeafWrites = 0;
        pageOverflowWrites = 0;
    }

    private void resetIntermittentPageTracking() {
        setDefaults();
    }

    public int getPageReads() {
        return(pageReads);
    }

    public int getPageWrites() {
        return(pageWrites);
    }

    public int getInterminentInternalPageReads() {
        return(pageInternalReads);
    }

    public int getInterminentLeafPageReads() {
        return(pageLeafReads);
    }

    public int getInterminentOverflowPageReads() {
        return(pageOverflowReads);
    }

    public int getInterminentInternalPageWrites() {
        return(pageInternalWrites);
    }

    public int getInterminentLeafPageWrites() {
        return(pageLeafWrites);
    }

    public int getInterminentOverflowPageWrites() {
        return(pageOverflowWrites);
    }

    public int[] deleteIO(long key, boolean unique, boolean verbose)
            throws IOException, InvalidBTreeStateException {
        startPageTracking();
        DeleteResult r = bt.deleteKey(key, unique);
        //bt.searchKey(key, unique);
        if(verbose) {
            log.info("Key " + key +
                    (r.isFound() ? " has been found" : " was not found"));
            if(r.isFound()) {
                log.info("Number of results returned: " + r.getValues().size());
            }
            log.info("Total page reads for this deletion: " + getPageReads());
            log.info("Total page writes for this deletion: " + getPageWrites());
            log.info("\nBroken down statistics: ");
            log.info("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[9];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();
        res[8] = r.isFound() ? 1 : 0;
        totalDeletionReads += pageReads;
        totalDeletionWrites += pageWrites;

        resetIntermittentPageTracking();
        return res;
    }

    public int[] searchIO(long key, boolean unique, boolean verbose)
            throws IOException, InvalidBTreeStateException {
        startPageTracking();
        SearchResult r = bt.searchKey(key, unique);
        if(verbose) {
            log.info("Key " + key +
                    (r.isFound() ? " has been found" : " was not found"));
            if(r.isFound()) {
                log.info("Number of results returned: " + r.getValues().size());
            }
            log.info("Total page reads for this search: " + getPageReads());
            log.info("Total page writes for this search: " + getPageWrites());
            log.info("\nBroken down statistics: ");
            log.info("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[9];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();
        res[8] = r.isFound() ? 1 : 0;
        totalSearchReads += pageReads;
        totalSearchWrites += pageWrites;

        resetIntermittentPageTracking();
        return res;
    }

    public int[] rangeIO(long minKey, long maxKey,
                       boolean unique, boolean verbose) throws IOException, InvalidBTreeStateException {
        startPageTracking();
        RangeResult rangeQRes =  bt.rangeSearch(minKey, maxKey, unique);
        if(verbose) {
            log.info("Range Query returned: " +
                    (rangeQRes.getQueryResult() != null ?
                            (rangeQRes.getQueryResult().size()) : "0") + " results");
            log.info("Total page reads for this search: " + getPageReads());
            log.info("Total page writes for this search: " + getPageWrites());
            log.info("\nBroken down statistics: ");
            log.info("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[8];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();
        totalRangeQueryReads += pageReads;
        totalRangeQueryWrites += pageWrites;

        resetIntermittentPageTracking();
        return res;
    }

    public int[] insertIO(long key, String value,
                        boolean unique, boolean verbose)
            throws IOException, InvalidBTreeStateException {
        startPageTracking();
        bt.insertKey(key, value, unique);
        if(verbose) {
            log.info("Total page reads for this insertion: " + getPageReads());
            log.info("Total page writes for this insertion: " + getPageWrites());
            log.info("\nBroken down statistics: ");
            log.info("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            log.info("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[8];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();

        totalInsertionReads += pageReads;
        totalInsertionWrites += pageReads;

        resetIntermittentPageTracking();
        return res;
    }

    public int getTotalIntermittentInsertionReads() {
        return(totalInsertionReads);
    }

    public int getTotalIntermittentInsertionWrites() {
        return(totalInsertionWrites);
    }

    public void incrementIntermittentInternalNodeReads() {
        if(trackIO) {
            pageInternalReads++;
            incrementPageReads();
        }
    }

    private void incrementIntermittentLeafNodeReads() {
        if(trackIO) {
            pageLeafReads++;
            incrementPageReads();
        }
    }

    private void incrementIntermittentOverflowPageReads() {
        if(trackIO) {
            pageOverflowReads++;
            incrementPageReads();
        }
    }

    private void incrementIntermittentInternalNodeWrites() {
        if(trackIO) {
            pageInternalWrites++;
            incrementPageWrites();
        }
    }

    private void incrementIntermittentLeafNodeWrites() {
        if(trackIO) {
            pageLeafWrites++;
            incrementPageWrites();
        }
    }

    private void incrementIntermittentOverflowPageWrites() {
        if(trackIO) {
            pageOverflowWrites++;
            incrementPageWrites();
        }
    }


    void incrementTotalInternalNodeReads() {
        if(trackIO) {
            totalInternalNodeReads++;
            incrementTotalNodeReads();
            incrementIntermittentInternalNodeReads();
        }
    }

    void incrementTotalLeafNodeReads() {
        if(trackIO) {
            totalLeafNodeReads++;
            incrementTotalNodeReads();
            incrementIntermittentLeafNodeReads();
        }
    }

    void incrementTotalOverflowReads() {
        if(trackIO) {
            totalOverflowReads++;
            incrementTotalNodeReads();
            incrementIntermittentOverflowPageReads();
        }
    }

    void incrementTotalInternalNodeWrites() {
        if(trackIO) {
            totalInternalNodeWrites++;
            incrementTotalNodeWrites();
            incrementIntermittentInternalNodeWrites();
        }
    }

    void incrementTotalLeafNodeWrites() {
        if(trackIO) {
            totalLeafNodeWrites++;
            incrementTotalNodeWrites();
            incrementIntermittentLeafNodeWrites();
        }
    }

    void incrementTotalOverflowNodeWrites() {
        if(trackIO) {
            totalOverflowWrites++;
            incrementTotalNodeWrites();
            incrementIntermittentOverflowPageWrites();
        }
    }

    private int totalOperationCount() {
        return(totalInsertions + totalSearches +
                totalRangeQueries + totalDeletions);
    }

    public void printTotalStatistics() {
        log.info("\n !! Printing total recorded statistics !!");
        log.info("\nOperations break down");
        log.info("\n\tTotal insertions: " + totalInsertions);
        log.info("\tTotal searches: " + totalSearches);
        log.info("\tTotal range queries: " + totalRangeQueries);
        log.info("\tTotal performed op count: " + totalOperationCount());

        log.info("\nTotal I/O break down (this run only)");
        log.info("\nTotal Read statistics");
        log.info("\n\tTotal reads: " + totalNodeReads);
        log.info("\tTotal Internal node reads: " + totalInternalNodeReads);
        log.info("\tTotal Leaf node reads: " + totalLeafNodeReads);
        log.info("\tTotal Overflow node reads: " + totalOverflowReads);

        log.info("\nTotal Write statistics: ");
        log.info("\n\tTotal writes: " + totalNodeWrites);
        log.info("\tTotal Internal node writes: " + totalInternalNodeWrites);
        log.info("\tTotal Leaf node writes: " + totalLeafNodeWrites);
        log.info("\tTotal Overflow node writes: " + totalOverflowWrites);

        log.info("\nPage creation break down.");
        log.info("\n\tTotal pages created: " + totalPages);
        log.info("\tTotal Internal nodes created: " + totalInternalNodes);
        log.info("\tTotal Leaf nodes created: " + totalLeaves);
        log.info("\tTotal Overflow nodes created: " + totalOverflowPages);

        log.info("\nPage deletion break down.");
        log.info("\n\tTotal pages deleted: " + totalDeletions);
        log.info("\tTotal Internal nodes deleted: " + totalInternalNodeDeletions);
        log.info("\tTotal Leaf nodes deleted: " + totalLeafNodeDeletions);
        log.info("\tTotal Overflow pages deleted: " + totalOverflowPagesDeletions);

        log.info("\nPage split statistics");
        log.info("\n\tTotal page splits: " + totalSplits);
        log.info("\tActual Root splits: " + totalRootSplits);
        log.info("\tInternal node splits: " + totalInternalNodeSplits);
        log.info("\tLeaf node splits: " + totalLeafSplits);
    }

    void resetAllMetrics() {
        totalPages = 0;
        totalInternalNodes = 0;
        totalLeaves = 0;
        totalOverflowPages = 0;

        totalNodeReads = 0;
        totalInternalNodeReads = 0;
        totalOverflowReads = 0;
        totalLeafNodeReads = 0;

        totalNodeWrites = 0;
        totalInternalNodeWrites = 0;
        totalLeafNodeWrites = 0;
        totalOverflowWrites = 0;

        totalInternalNodeDeletions = 0;
        totalLeafNodeDeletions = 0;
        totalOverflowPagesDeletions = 0;

        totalDeletions = 0;
        totalInsertions = 0;
        totalSearches = 0;
        totalRangeQueries = 0;

        totalSplits = 0;
        totalRootSplits = 0;
        totalInternalNodeSplits = 0;
        totalLeafSplits = 0;

        setDefaults();

        totalSearchReads = 0;
        totalSearchWrites = 0;
        totalRangeQueryReads = 0;
        totalRangeQueryWrites = 0;
        totalInsertionReads = 0;
        totalInsertionWrites = 0;
        totalDeletionReads = 0;
        totalDeletionWrites = 0;
    }
}
