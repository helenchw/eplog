#ifndef __SYNCMOD_HH__
#define __SYNCMOD_HH__

#include <mutex>
#include <map>
#include "common/struct/segmentmetadata.hh"

/**
 * Module for managing metadata of logs chunks
 */

class SyncMod {
public:

    // assume there are only n-k log disks, we assume the same lba refers to the same log segment across log disks
    // mapping add by: logMod, when flush to disk; mapping remove by: raidMod, sync all segments

    /** lock for operations on logDiskMapMutex */
    mutex m_logDiskMapMutex;
    /** mapping of starting log disk LBA to log segment info. and ending LBA */
    map<lba_t, sid_t> m_logDiskMap; // LBA -> log segment id
    /** min. block address of log disk */
	lba_t m_logDiskMapMin;
    /** max. block address of log disk */
	lba_t m_logDiskMapMax;

    /** 
     * Constructor
     */
    SyncMod() : m_logDiskMapMin(INVALID_LBA), m_logDiskMapMax(INVALID_LBA) {
    }

    /** 
     * Destructor
     */
    ~SyncMod() {
    }

    /**
     * Determine whether RRW or RMW should be used for a data segment commit
     * \param k the number of data chunk in the segment
     * \param diskStatus the list of status of disks involved
     * \param updatedChunks the list of id of chunks updated in the segment
     * \return whether RMW should be used
     */
    static bool useRMW (int k, const vector<bool>& diskStatus, 
            const vector<chunk_id_t>& updatedChunks) {
        // 1. if any of disks failed
        for (chunk_id_t chunkId : updatedChunks) {
            if (!diskStatus[chunkId]) {
                return false;
            }
        }
        // 2. update ratio (read bandwidth required)
        int rwRatio = updatedChunks.size() * 100 / k;
        return (rwRatio < 50);
    }

	/** 
     * Update the log disk address to log segment id mapping
	 * \param lba the log chunk lba on log disk
	 * \param sid the log segment id associated with the log chunk
     * \return whether the update is successful
	 */
	bool updateLogDiskMap(lba_t lba, sid_t sid) {
		assert(lba >= 0);
		assert(sid >= 0);
		if (lba < 0 || sid < 0) return false;

        lock_guard<mutex> lk (m_logDiskMapMutex);
		m_logDiskMap[lba] = sid;
		if (m_logDiskMapMin == INVALID_LBA || lba < m_logDiskMapMin) m_logDiskMapMin = lba;
		if (m_logDiskMapMax == INVALID_LBA || lba > m_logDiskMapMax) m_logDiskMapMax = lba;

		return true;
	}

	/**
     * Reset the log disk address to log segment id mapping (clear all mapping)
     * \return whether the mapping is cleared
	 */
	bool resetLogDiskMap() {
        lock_guard<mutex> lk (m_logDiskMapMutex);
		m_logDiskMap.clear();
		return true;
	}
    
    /**
     * Print the log disk mapping
     */
    void printLogDiskMap () {
        lock_guard<mutex> lk (m_logDiskMapMutex);
        if (m_logDiskMap.empty()) {
            fprintf(stderr, "Empty log disk mapping\n");
            return;
        }
        for (auto& kv : m_logDiskMap) {
            fprintf(stderr, "(logDiskMap) lba %d to log segment %d\n", 
                    kv.first, kv.second);
        }
    }
};
#endif /* __SYNCMOD_HH__ */
