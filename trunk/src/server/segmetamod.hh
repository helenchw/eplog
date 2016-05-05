#ifndef SEGMETAMOD_HH_ 
#define SEGMETAMOD_HH_

#include <string>
#include <unordered_map>
#include "common/define.hh"
#include "common/struct/segmentmetadata.hh"

#define MAX_SEGMENTS 10000000

/**
 * Module for managing segment metadata
 */

class SegmentMetaDataMod {
public:
    /**
     * Constructor
     */
    SegmentMetaDataMod();

    /**
     * Destructor
     */
    ~SegmentMetaDataMod();

    /**
     * Create a new segment metadata entry
     * \param isUpdateLog whether the segment is of type log
     * \param sid specific segment id to use
     * \return the new segment metadata data structure
     */
    SegmentMetaData* const createEntry(bool isUpdateLog = false, sid_t sid = INVALID_SEG);

    /**
     * Get the segment metadata entry
     * \param segmentId the id of segment
     * \return the segment metadata data structure for the specific id
     */
    SegmentMetaData* const getEntry(sid_t segmentId);

    /**
     * Check whether a segment exists
     * \param segmentId the id of the segment to check
     * \return whether the segment exists
     */
    bool existEntry(sid_t segmentId);

    /**
     * Remove the segment record
     * \param segmentId the id of the segment to remove
     * \return whether the segment exists for removal
     */
    bool removeEntry(sid_t segmentId);

    /**
     * Update the heap for GC
     * \param segmentId the id of segment accessed
     * \param delv the change in the number of valid bytes in the segment
     * \return the number of valid bytes in the segment
     */
    int updateHeap(sid_t segmentId, int delv = 0);

    /**
     * Get the segment with least number of valid bytes
     * \return a pair of segment id and segment metadata data structure
     */
    pair<sid_t,SegmentMetaData*> getminHeap();

    /**
     * Print the current min heap
     */
    void printHeap();

    /** the mapping of segment id and segment metadata data structure */
    nv_unordered_map<sid_t, SegmentMetaData*> m_metaMap;       // simple hack for accessing from other class

private:
    RWMutex m_metaOptMutex;                     /**< Lock for operations on metaMap and idMap */
    sid_t m_segmentIdCounter;                   // no need to be atomic, we have lock already
    sid_t m_logSegmentIdCounter;                // no need to be atomic, we have lock already
    int m_segmentSize;                          /**< Segment size */
    int* value;                                 /**< [segID] = free bytes in the segment */
    int* ps;                                    /**< [pos] = segID */
    int* heap;                                  /**< [segID] = pos */
    int  heaplen;                               /**< Size of heap in used */
};

#endif /* SEGMETAMOD_HH_ */
