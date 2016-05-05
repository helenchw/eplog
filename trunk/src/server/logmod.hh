#ifndef LOGMOD_HH_
#define LOGMOD_HH_

#include <tuple>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include "lib/threadpool/threadpool.hpp"
#include "common/define.hh"
#include "common/struct/logbuffer.hh"
#include "common/struct/filemetadata.hh"
#include "common/configmod.hh"
#include "server/raidmod.hh"
#include "server/segmetamod.hh"
#include "server/filemetamod.hh"
#include "server/syncmod.hh"
#include "server/permetamod.hh"

/**
 * Module for (log) buffer management
 */
class LogMod {
public:
    /**
     * Constructor
     * for double coding schemes
     */
    LogMod(RaidMod* raidMod, SegmentMetaDataMod* segmetaMod, 
            FileMetaDataMod* filemetaMod, SyncMod* syncMod, 
            CodeSetting codeSetting, PersistMetaMod* perMetaMod = NULL);

    /**
     * Destructor
     */
    virtual ~LogMod();

    /**
     * Write Data to a buffer
     * \param buf buffer containing the data to write
     * \param level (Deprecated)
     * \param fmd file metadata of the file which the data belongs to
     * \param fileOffLen pair of write offset and length in file
     * \param isGCWrite whether this write is for GC
     * \param ts timestamp
     * \param isUpdate whether this write is an update to file
     * \return whether log write is sucessful (always sucess)
     */
#ifndef BLOCK_ITF
    bool writeLog (char* buf, int level, FileMetaData* fmd, 
            foff_len_t fileOffLen, bool isGCWrite = false, LL ts = 0, 
            bool isUpdate = false);
#else
    bool writeLog (char* buf, int level, FileMetaData* fmd, 
            foff_len_t fileOffLen, bool isLastSeg = false, LL ts = 0, 
            bool isUpdate = false);
#endif

    /**
     * Read data from a buffer
     * \param buf buffer to receive the data read
     * \param segmentId segment id to read
     * \param segOffLen pair of write offset and length in segment 
     * \return whether log segment read is sucessful
     */
#ifndef BLOCK_ITF
    bool readLog (char* buf, sid_t segmentId, soff_len_t segOffLen);
#else
    bool readLog (char* buf, sid_t segmentId, soff_len_t segOffLen, 
            unordered_map<chunk_id_t, pair<int, chunk_id_t> > levels);
#endif

    /**
     * Get the segment ids of the segments in log buffers
     * \return no. of ids inserted
     */
    int getAliveSegmentIds(std::unordered_set<sid_t>& ids);

    /**
     * Get the segment ids of the segments in update log buffers
     * \return list of segment id in update log buffers
     */
#ifndef BLOCK_ITF
    int getAliveUpdateSegmentIds(std::unordered_set<sid_t>& ids);
#else
    int getAliveUpdateSegmentIds(sid_t sid, unordered_map<chunk_id_t, pair<int,chunk_id_t> >& levels);  // set(update buffer involved, chunk id)
#endif

    /**
     * Flush a number of buffer to disks
     * \param numLog number of update log buffers to flush
     * \param ts timestamp
     * \return number of update log buffers flushed
     */
    int flushUpdateToDisk(int numLog = -1, LL ts = 0);

    /**
     * Flush updates in chunk groups
     * \param forced whether all updates will be flushed even their size is smaller than a chunk group
     * \return whether any update is flushed from update log buffers
     */
    bool flushUpdateInChunkGroups(bool forced);

    /**
     * Flush all the log buffers to disk in a batch
     * \param ts timestamp
     * \return whether any new write is flushed from log buffers
     */
    bool flushToDiskInBatch(LL ts = 0);

    /**
     * Get size of chunks in a segment
     * \return chunk size
     */
    int getChunkSize(void) {
        return m_logSegmentSize / m_logBlockNum;
    }

    std::mutex m_flushMutex;                                /**< Lock for buffer flush operations */

private:
    const int m_logLevel;                                   /**< Number of log buffers */
    const int m_updateLogLevel;                             /**< Number of update log buffers */
    const int m_segmentSize;                                /**< Size of a data segment */
    const int m_logSegmentSize;                             /**< Size of a log segment */
    const int m_logBlockNum;                                /**< Number of data blocks in a log segment */
    const int m_logCodeBlockNum;                            /**< Number of code blocks in a log segment */
    const int m_chunkGroupSize;                             /**< Number of chunks in a group */

    RaidMod* const m_raidMod;                               /**< Pointer to RaidMod */
    SegmentMetaDataMod* const m_segmetaMod;                 /**< Pointer to SegmentMetaDataMod */
    FileMetaDataMod* const m_filemetaMod;                   /**< Pointer to FileMetaDataMod */
    SyncMod * const m_syncMod;                              /**< Pointer to SyncMod */
    PersistMetaMod* const m_perMetaMod;

    std::vector<LogBuffer> m_logbuffers;                    /**< New write buffers */
    std::vector<LogBuffer> m_updateLogbuffers;              /**< Update buffers */
    char** m_chunkGroupLogBuffer;                           /**< Chunk group buffers */

    std::vector<SegmentMetaData*> m_chunkGroupLogSegMeta;   /**< Segment metadata associated with the chunk group buffers */

    std::unordered_map<sid_t, int> m_segmentIdMap;          /**< Mapping of segmentId in the new write buffers */
    RWMutex m_segmentIdMapMutex;                            /**< Lock for segment ID mapping */
    std::unordered_map<sid_t, int> m_updateSegmentIdMap;    /**< SegmentId in the update buffers */
    RWMutex m_updateSegmentIdMapMutex;
#if defined(STRIPE_PLOG) || defined(BLOCK_ITF)
    std::unordered_map<sid_t, set<pair<int,chunk_id_t> > > m_updatedSegmentIdMap;  // dataSegmentId, set(update buffer involved, chunk id)
#else
    std::unordered_map<sid_t, set<int>> m_updatedSegmentIdMap;  // dataSegmentId, set(update buffer involved)
#endif
    RWMutex m_updatedSegmentIdMapMutex;

    int m_curLogLevel;                                      /**< Current appending new write buffer level */
    int m_lastFlushLog;                                     /**< Last flushed update buffer's level, i.e. most recent level */
#ifdef STRIPE_PLOG
    int m_curUpdateLogLevel;
#endif
    vector<int> m_chunkGroupLastFlush;                      /**< Last level flushed within a chunk group */
    vector<int> m_chunkGroupDataEnd;                        /**< Last level with valid data a chunk group */
    char* m_logMapping;                                     /**< Buffer for new write processing */

	boost::threadpool::pool m_tp;                           /**< Thread pool for parallel I/O */

    /**
     * Flush a given log buffer to disk
     * \param logbuf the log buffer data structure of the log buffer
     * \param ts timestamp
     * \param isUpdate whether the log buffer is for updates
     * \param logSegMeta a set of segment metadata to return if the log buffer
     * is divided into different segments when flushed
     * \return whether any new write is flushed from log buffers
     */
    bool flushToDisk(LogBuffer& logbuf, LL ts, bool isUpdate = false);

    /**
     * Flush all update log buffers to disk in chunk groups
     * \param level the number of buffers to form a chunk group
     * \param ts timestamp
     * \param forced whether all update are flush even if their size is smaller than a chunk group
     * \return whether any update is flushed from update log buffers
     */
    bool flushUpdateInChunkGroups(int level = -1, FileMetaData* curfmd = NULL, LL ts = 0, bool forced = false);

    /**
     * Update the segment metadata of data segments and update log buffers flushed
     * \param level the level of the log buffer among the buffers
     * \param logsegmetas the set of segment metadata return by flushToDisk
     */
    void updateMetaData(int level);
            
    /**
     * Add the mapping of a data segment associated with a log buffer
     * \param segId the segment id of the data segment
     * \param level the level of log buffer associated
     * \return whether the operation is successful (always success)
     */
#if defined(STRIPE_PLOG) ||  defined(BLOCK_ITF)
    bool addUpdateSegment(sid_t segId, int level, chunk_id_t chunk);
#else
    bool addUpdateSegment(sid_t segId, int level);
#endif

    /**
     * Remove the mapping of a data segment associated with a log buffer
     * \param segId the segment id of the data segment
     * \param level the level of log buffer associated
     * \return whether the segment mapping is removed
     */
#if defined(STRIPE_PLOG) || defined(BLOCK_ITF)
    bool removeUpdateSegment(sid_t segId, int level, chunk_id_t chunk);
#else
    bool removeUpdateSegment(sid_t segId, int level);
#endif

    /**
     * Check if a log segment is still in buffer
     * \param segId the segment id of the log segment
     * \return whether the segment is in update log buffer
     */
    bool isUpdateSegmentExist(sid_t segId);
};

#endif /* LOGMOD_HH_ */
