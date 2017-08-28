#ifndef __RAIDMOD_HH__
#define __RAIDMOD_HH__
#include <vector>
#include <list>
#include <atomic>
#include "common/lowlockqueue.hh"
#include "common/define.hh"
#include "common/enum.hh"
#include "common/struct/chunk.hh"
#include "common/struct/codesetting.hh"
#include "common/struct/segmentmetadata.hh"
#include "common/struct/raidsetting.hh"
#include "server/diskmod.hh"
#include "server/coding/coding.hh"
#include "server/segmetamod.hh"
#include "server/syncmod.hh"
#include "server/permetamod.hh"
#include "threadpool.hpp"

using namespace std;

/**
 * Module handling coding of data (and log) segments
 */

class RaidMod {
public:
    /**
     * Constructor
     * \param diskMod DiskMod
     * \param codeSetting coding scheme used for data segments, and log segments
     * \param gcQueue queue of segments to GC
     * \param segMetaMod SegmentMetaDataMod
     * \param syncMod SyncMod
     */
    RaidMod(DiskMod* diskMod, vector<CodeSetting> codeSetting,
            LowLockQueue<SegmentMetaData*>* gcQueue, SegmentMetaDataMod* segMetaMod,
            SyncMod* syncMod, PersistMetaMod* perMetaMod = NULL);

    /**
     * Directly write a data segment (as one block) to disks when MD is used,
     * otherwise, enocde and write a data segment to disks
     * \param segmentMetaData segment metadata data structure
     * \param buf buffer containing the data to write
     * \param ts timestamp
     */
    void writeSegment(SegmentMetaData* segmentMetaData, char* buf, LL ts = 0);

    /**
     * Encode and overwrite parts of a data segment in-place on disks
     * \param segmentMetaData segment metadata data structure
     * \param buf buffer containing the data to write
     * \param offLens list of (offset,len) pairs to overwrite
     * \param parityUpdate (Deprecated) 
     * \param delta (Deprecated)
     */
    void writePartialSegment(SegmentMetaData* segmentMetaData, char* buf, 
            vector<off_len_t> offLens, LL ts = 0, bool parityUpdate = false, 
            char* delta = NULL);

    /**
     * Encode and overwrite parts of a data segment in-place on disks (for multi-thread)
     * \param segmentMetaData segment metadata data structure
     * \param buf buffer containing the data to write
     * \param offLens list of (offset,len) pairs to overwrite
     * \param parityUpdate (Deprecated)
     * \param delta (Deprecated)
     */
    void writePartialSegment_mt(SegmentMetaData* segmentMetaData, char* buf, 
            vector<off_len_t> offLens, LL ts, bool parityUpdate, 
            char* delta, std::atomic<int>& cnt) {
		writePartialSegment(segmentMetaData, buf, offLens, ts, parityUpdate,
				delta);
		cnt--;
	}

    /**
     * Encode and write a log segment to disks
     * \param segmentMetaData segment metadata data structure
     * \param buf buffer containing the data to write
     * \param bitmap bitmap indicating which chunk to write
     * \param ts timestamp
     * \param dsid data segment id associated with the buffer
     */
    void writeLogSegment(SegmentMetaData* lsmd, char* buf, int bitmap, LL ts = 0);

    /**
     * Write an encoded but incomplete segment to disks
     * \param segmentMetaData segment metadata data structure
     * \param buf buffer containing the data to write
     * \param bitmap bitmap indicating which chunk to write
     * \param ts timestamp
     */
    void writeIncompleteSegment(SegmentMetaData* dsmd, char* buf, int bitmap, LL ts = 0);
    void writeIncompleteSegment_mt(SegmentMetaData* dsmd, char* buf, int bitmap, LL ts,
			std::atomic<int>& cnt){
		writeIncompleteSegment(dsmd, buf, bitmap, ts);
		cnt--;
	}

    /**
     * Encode and write a batch of segments in log buffers
     * \param level number of log buffers to write
     * \param lsmdV list of segment metadata data structure associated
     * \param buf buffer containing the data to write
     * \param ts timestamp
     */
    void writeBatchedSegments(int level, vector<SegmentMetaData*> lsmdV, char* buf, LL ts = 0);

#ifdef STRIPE_PLOG
    /**
     * Write a batch of parity to log disks 
     * \param level  number of parity to flush for each log disks (assume to be same for all disks)
     * \param lsmdV list of segment metadata data structure associated
     * \param buf buffer containing the parity to write
     * \param ts timestamp
     */
    void writeBatchedLogParity(int level, vector<SegmentMetaData*> lsmdV, char* buf, LL ts = 0);
#endif

    /**
     * Encode and write a batch of segments in update log buffers
     * \param level number of log buffers to write
     * \param lsmdV list of segment metadata data structure associated
     * \param buf buffer containing the data to write
     * \param chunkMap bitmap indicating which chunk to write
     * \param ts timestamp
     */
    void writeBatchedLogSegments(int level, vector<SegmentMetaData*> lsmdV, char** buf, int chunkMap, LL ts = 0);

    /**
     * Encode a segment 
     * \param buf buffer containing the data to write
     * \param isUpdate whether the segment is of type log
     * \param k number of data chunks returned
     * \return list of encoded chunks
     */
    vector<Chunk> encodeSegment(char* buf, bool isUpdate, int& k);

    /**
     * Encode a segment (for multi-thread) [DATA SEGMENT ONLY!]
     * \param buf buffer containing the data to write
     * \param chunks list of encoded chunks
     * \param cnt counter for multi-thread (to decrement)
     */
    void encodeSegment_mt(char* buf, vector<Chunk>& chunks, bool isUpdate, std::atomic_int& cnt);


    /**
     * (Decode and) read a segment, handle normal read, as well as degraded read 
     * \param segmentMetaData segment metadata data structure of the segment to read
     * \param buf buffer to receive the data read
     * \param offLen offset and length of the data to read
     * \param ts timestamp
     * \param commit whether commit should be done for degraded read
     */
    void readSegment(SegmentMetaData* segmentMetaData, char* buf, 
            off_len_t offLen, LL ts = 0, bool commit = true);

    /**
     * (Decode and) read a segment, handle normal read, as well as degraded read (for multi-thread)
     * \param segmentMetaData segment metadata data structure of the segment to read
     * \param buf buffer to receive the data read
     * \param offLen offset and length of the data to read
     * \param ts timestamp
     * \param commit whether commit should be done for degraded read
     * \param cnt counter for multi-thread (to decrement)
     */
    void readSegment_mt(SegmentMetaData* segmentMetaData, char* buf, 
            off_len_t offLen, LL ts, bool commit, std::atomic<int>& cnt) {
		readSegment(segmentMetaData, buf, offLen, ts, commit);
		cnt--;
	}

    /**
     * Commit a segment by segment id (Deprecated)
     * \param segmentId the id of data segment to commit
     * \param ts timestamp
     * \return whether all updates of the data segment is committed 
     */
    bool syncSegment(sid_t segmentId, LL ts = 0);

    /**
     * Commit all segment with updates
     * \param ts timestamp
     * \return whether all updates are committed (always true)
     */
    bool syncAllSegments(LL ts = 0);

	/**
	 * Recover failed disks to target disks, after degraded parity commit
	 * \param failed list of disks failed
	 * \param target list of disks to store recovered data
     * \param ts timestamp
     * \return amount of data recovered
	 */
	uint64_t recoverData(vector<disk_id_t> failed, vector<disk_id_t> target, LL ts = 0);

private:

    /**
     * (Decode and) read a log segment, used only when original data segment has updated chunks missing
     * \param segmentMetaData segment metadata data structure
     * \param buf buffer to receive data read
     * \param offLen pair of (offset, length) of data to read
     * \param ts timestamp
     */
    void readLogSegment(SegmentMetaData* segmentMetaData, char* buf, off_len_t offLen, LL ts = 0);

    /**
     * Read a chunk of a segment (for multi-thread)
     * \param diskId id of the disk where the chunk is on
     * \param lba list of addresses of blocks composing the chunk
     * \param buf buffer to receive data read
     * \param offLens list of pairs of (offset, length) of data to read
     * \param rcnt counter for multi-thread to update (decrement)
     * \param ts timestamp
     */
    void readChunkSymbols (disk_id_t diskId, lba_t lba, char* buf, 
            vector<off_len_t> offLens, std::atomic_int& rcnt, LL ts);

    /**
     * Read a chunk of a segment
     * \param diskId id of the disk where the chunk is on
     * \param lba list of addresses of blocks composing the chunk
     * \param buf buffer to receive data read
     * \param offLens list of pairs of (offset, length) of data to read
     * \param ts timestamp
     */
    void readChunkSymbols (disk_id_t diskId, lba_t lba, char* buf, vector<off_len_t> offLens, LL ts);

    /**
     * Obtain the total length of parts read within a chunk
     * \param offLen list of pairs of (offset, length) of data read
     * \return total length of all parts
     */
    int getChunkLength (vector<off_len_t> offLen) const;

    /**
     * batchRecovery
     * Recover failed chunks in a batch of data segments 
     * \param readbuf buffer for holding the chunks read
     * \param writebuf buffer for holding the chunks to write
     * \param seguf buffer for decoding
     * \param readbufMap mapping of address to chunks in readbuf
     * \param dataSegments metadata of data segments to recover
     * \param chunksRead metadata of the chunks read from each segment
     * \param failedChunks metadata of failed chunks in each segment
     * \param stripeChunkEnd ending position of chunks for each segment
     * \param batchSize max. number of segments in each batch
     * \param target the disk to store recovered data
     * \return number of bytes recovered
     */
    uint64_t batchRecovery(char *readbuf, char *writebuf, char* segbuf,
            map<disk_id_t, set<lba_t> > &readbufMap,
            vector<SegmentMetaData*> &dataSegments, vector<Chunk> &chunksRead,
            vector<chunk_id_t> &failedChunks, vector<pair<uint32_t,uint32_t> > stripeChunkEnd,
            uint32_t batchSize, vector<disk_id_t> target);

    /**
     * syncParityBlocks
     * Synchronize the parity block according to the scheme specified
     * \param dataSegment the segment metadata data structure of the data segment
     * \param chunks list of new chunks (including both data and parity)
     * \param oldChunks the list of old chunks (including both data and parity)
     * \param segbuf temporary buffer for bit-wise encoding
     * \param readbuf temporary buffer for bit-wise encoding
     * \param diskStatus list of status of disks involved
     * \param rcw whether parity should be updated using RRW
     * \param ts timestamp
     */
    void syncParityBlocks(SegmentMetaData* dataSegment, const vector<Chunk>& chunks, 
            const vector<Chunk>& oldChunks, char* segbuf, char* readbuf, 
            vector<bool> diskStatus, bool rcw, LL ts = 0);

    /**
     * Synchronize the parity block according to the scheme specified (for multi-thread)
     * \param dataSegment the segment metadata data structure of the data segment
     * \param chunks list of new chunks (including both data and parity)
     * \param oldChunks the list of old chunks (including both data and parity)
     * \param segbuf temporary buffer for bit-wise encoding
     * \param readbuf temporary buffer for bit-wise encoding
     * \param diskStatus list of status of disks involved
     * \param rcw whether parity should be updated using RRW
     * \param cnt counter for multi-thread (to decrement)
     * \param ts timestamp
     */
    void syncParityBlocks_mt(SegmentMetaData* dataSegment, char* segbuf, char* readbuf, 
            vector<bool> diskStatus, atomic<int>& cnt, LL ts = 0);

    /**
     * Commit updates for the set of data segments specified
     * \param dataSegUpdatedChunks the mapping of data segment id and chunks to commit
     * \param logSegmentIds the list of log segment ids associated
     * \param onlyRCW whether parity should be updated using RCW only
     * \param ts timestamp
     */
    int commitUpdate (map<sid_t, map<chunk_id_t, pair<Chunk,lba_t> > > dataSegUpdatedChunks, 
        vector<sid_t> logSegmentIds, bool onlyRCW = false, LL ts = 0);

    vector<Coding*> m_coding;                           /**< List of encoding schemes */
    vector<RaidSetting> m_raidSetting;                  /**< List of raidSetting for encoding */
    DiskMod* m_diskMod;                                 /**< Pointer to DiskMod */
    SegmentMetaDataMod* m_segMetaMod;                   /**< Pointer to SegmentMetaDataMod */
    SyncMod* m_syncMod;                                 /**< Pointer to SyncMod */
    PersistMetaMod* m_perMetaMod;                       /**< Pointer to PersistMetaMod */
	boost::threadpool::pool m_rtp;                      /**< Read thread pool */
	boost::threadpool::pool m_wtp;                      /**< Write thread pool */
	//boost::threadpool::pool m_ctp;                    /**< Encoding thread pool */
	char* m_segbuf;                                     /**< Segment-sized buffer */

    LowLockQueue<SegmentMetaData*>* m_gcQueue;          /**< Pointer to queue of segments to GC */
};

#endif
