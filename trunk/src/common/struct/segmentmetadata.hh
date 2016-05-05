#ifndef SEGMENTMETADATA_HH_
#define SEGMENTMETADATA_HH_

#include <boost/thread/locks.hpp>
#include <boost/iterator/indirect_iterator.hpp>
#include <mutex>
#include <set>
#include <vector>
#include <common/define.hh>
#include "server/bitmap.hh"
#include "common/struct/filemetadata.hh"

#define INVALID_LOG_SEG_ID  INVALID_SEG

/**
 * Segment metadata structure
 */
struct SegmentMetaData {
    sid_t segmentId;                                               /**< Segment id */
    BitMap bitmap;                                                 /**< Bitmap of segment, in pages */
//  std::mutex segmentLock;
    std::vector<SegmentLocation> locations;                        /**< Location of each chunk */
    /** Location of log data chunks (LBA, logSegId) */
    std::map<chunk_id_t, vector<pair<lba_t,sid_t> > > logLocations; 
    /** Set of files associated with the segment */
    std::set<FileMetaData*> fileSet;        
    /** Mapping of segment offset and files associated with the segment */
    std::vector<pair<int, FileMetaData*> > lookupVector;
    /** Mapping of segment offset and file offset */
    std::map<int, LL> fileOffsetMap;                               // Mapping of segment offset to file offset
    vector<sid_t> curLogId;                                        // List of log segment ids associated to this segment
    bool isUpdateLog;                                              /**< Whether this is a log segment */

    /**
     * Constructor
     */
    SegmentMetaData() {}

    /**
     * Constructor
     * \param bitmapSize
     */
    SegmentMetaData(int bitmapSize) : bitmap(bitmapSize) {}

    /**
     * Find the files covered by the part of segment requested
     * \param off starting offset in the segment
     * \param len length of request
     * \return a pair of file reference 1) include the starting offset, 2) passing the end of request (exclusive)
     */
    tuple<OffFmdIt, OffFmdIt> findCover (LL off, int len) {
        // create search lower bound
        pair<int, FileMetaData*> pin (off, nullptr);
        OffFmdIt lit = std::lower_bound(lookupVector.begin(), lookupVector.end(), pin);
        while (lit != lookupVector.begin()
                && (lit == lookupVector.end() || lit->first > off))
            lit--;

        // create search upper bound
        pin.first = off + len - 1;
        OffFmdIt rit = std::upper_bound(lookupVector.begin(), lookupVector.end(), pin);
        while (rit != lookupVector.end() && rit->first <= off + len - 1)
            rit++;
        return tuple<OffFmdIt, OffFmdIt>(lit, rit);
    }

};

/**
 * Extra segment metadata for log segments
 */
struct LogSegmentMetaData {
    sid_t segmentId;                                               /**< Log segment id */
    /** Mapping of log seg chunk id to (data seg id, chunk id in data seg) */
    std::map<chunk_id_t,pair<sid_t, chunk_id_t> > logMeta;    
};

#endif /* SEGMENTMETADATA_HH_ */
