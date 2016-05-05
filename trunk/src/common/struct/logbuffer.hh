#ifndef LOGBUFFER_HH_
#define LOGBUFFER_HH_

#include "common/define.hh"
#include "common/struct/segmentmetadata.hh"

/**
 * Basic structure for buffer
 */

// this struct is not copyable, since containing a mutex
// be careful with push_back()
// I am lucky to discover this within 10 mins!!!
struct LogBuffer {
    char* buf;                                     /**< Buffer data */
    int curOff;                                    /**< Buffer offset for appending new write*/
    SegmentMetaData* segmentMetaData;              /**< Segment metadata associated */
    RWMutex* logMutex;                             /**< Lock for operations on buffer */
    int chunkBitmap;                               /**< Chunk occupied, < 32 chunks */
    /** Data segments with updates in buffer */
    vector<SegmentMetaData*> updateSegments;
    /** Mapping of chunk in log disk (disk id) to chunks (chunk id) in data segment */
    vector<pair<chunk_id_t,chunk_id_t>> chunkMap;
#ifdef STRIPE_PLOG
    int dataSegmentId;
#endif
};

#endif /* LOGBUFFER_HH_ */
