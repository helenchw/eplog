#include <assert.h>
#include <algorithm>
#include <cstdlib>
#include <set>
#include <errno.h>
#include "common/debug.hh"
#include "common/enum.hh"
#include "server/diskmod.hh"
#include "common/configmod.hh"
#include "server/cachemod.hh"


// disk array should not change after initialization
DiskMod::DiskMod(const vector<DiskInfo> v_diskInfo) :
        m_numLogDisks(0), m_numDataDisks(0),
        m_logZoneSize(ConfigMod::getInstance().getLogZoneSize()),
        m_segmentPerLogZone(ConfigMod::getInstance().getLogZoneSegments()) {

	std::set<disk_id_t> diskIdSet;

    for (DiskInfo diskInfo : v_diskInfo) {
        assert(!m_diskInfo.count(diskInfo.diskId));
        assert(!m_usedLBA.count(diskInfo.diskId));
        assert(!m_diskMutex.count(diskInfo.diskId));

        m_diskInfo[diskInfo.diskId] = diskInfo;
        m_usedLBA[diskInfo.diskId] = new BitMap(diskInfo.numBlock);
        m_diskMutex[diskInfo.diskId] = new mutex();
        m_diskStatus[diskInfo.diskId] = true;
        if (diskInfo.isLogDisk) {
            m_numLogDisks++;
        } else {
            m_numDataDisks++;
			diskIdSet.insert(diskInfo.diskId);
		}
    }

	// construct ordered list of data disk id
	for (disk_id_t diskId : diskIdSet) {
		m_dataDiskIdVector.push_back(diskId);
	}

    m_numDisks = v_diskInfo.size();

	int numThread = ConfigMod::getInstance().getNumThread();
	m_stp.size_controller().resize(numThread);

#ifdef DISKLBA_OUT
    fp = fopen("disklba.out", "w");
#endif /* DISKLBA_OUT */
}

DiskMod::~DiskMod() {
    for (auto & kv : m_diskInfo) {
        kv.second.unmap();
    }
    for (auto & kv : m_usedLBA) {
        delete kv.second;
    }
    for (auto & kv : m_diskMutex) {
        delete kv.second;
    }

#ifdef DISKLBA_OUT
    fclose(fp);
#endif /* DISKLBA_OUT */
}

void DiskMod::listDisks() const {
    for (auto & kv : m_diskInfo) {
        debug("DiskId: %d Path:%s Cap:%lld\n", kv.first, kv.second.diskPath,
                kv.second.capacity);
    }
}

vector<disk_id_t> DiskMod::getDataDiskIds() const {
	return m_dataDiskIdVector;
}

// wrappers for multi-thread single-block write
void DiskMod::writeBlock_mt(disk_id_t diskId, const char* buf, lba_t& lba, std::atomic_int& cnt, LL ts) {
	lba = writeBlock(diskId, buf, ts);
	cnt--;
}

// wrappers for single-block write 
int DiskMod::writeBlock(disk_id_t diskId, const char* buf, LL ts) {
    lock_guard<mutex> lk(*m_diskMutex[diskId]);
    assert(m_diskMutex.count(diskId));
    assert(m_diskInfo.count(diskId));
    assert(m_usedLBA.count(diskId));

    lba_t lba = -1;

	lba_t &front= m_diskInfo[diskId].dataWriteFront;
	lba = m_usedLBA[diskId]->getFirstZeroAndFlip(front);

	// search from the begining of the disk (fill holes on a data disk) 
	if (lba < 0 && !m_diskInfo[diskId].isLogDisk) {
		lba = m_usedLBA[diskId]->getFirstZeroAndFlip(0);
	}

	// no free LBA is available
    if (lba < 0) {
        debug_error("Error LBA: %d disk full?\n", lba);
        exit(0);
    }

	// update the write front
	front = lba++;

    // write block
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    const ULL diskOffset = (ULL)lba * blockSize;

    debug("Write to disk %d at %llu \n", diskId, diskOffset);
#ifdef ACTUAL_DISK_IO
#ifdef USE_MMAP
    memcpy(m_diskInfo[diskId].data + diskOffset, buf, blockSize);
#else /* USE_MMAP */
    if (pwrite(m_diskInfo[diskId].fd, buf, blockSize, diskOffset) < 0) {
        debug_error ("pwrite error: %s\n", strerror (errno));
        exit(-1);
    }
#endif /* USE_MMAP */
#endif /* ACTUAL_DISK_IO */

#ifdef DISKLBA_OUT
    fprintf(fp, "%lld %d %lld %d %d\n",ts*1000, diskId, diskOffset / DISK_BLOCK_SIZE, blockSize / DISK_BLOCK_SIZE, 0);
#endif

    // deduct remaining space
    m_diskInfo[diskId].remaining -= blockSize;
    
    return lba;
}

void DiskMod::writeBlockToLba_mt(disk_id_t diskId, const char* buf, lba_t lba, int len, std::atomic_int& cnt, LL ts) {
    writeBlock(diskId, lba, len, (void*)buf, ts);
	cnt--;
}

// single-block write/overwrite with a given lba
int DiskMod::writeBlock(disk_id_t diskId, lba_t lba, int len, void* buf, LL ts) {
    assert(m_diskMutex.count(diskId));
    lock_guard<mutex> lk(*m_diskMutex[diskId]);

    assert(m_diskInfo.count(diskId));
    DiskInfo& diskInfo = m_diskInfo[diskId];

    // check if the given starting and ending LBA are out of range 
    int blockSize = ConfigMod::getInstance().getBlockSize();
    if (lba < 0 || lba > diskInfo.numBlock) {
        debug_error("Bad LBA: %d\n", lba);
        exit(-1);
    }

    if (len % blockSize) {
        debug_error("Bad length: %d\n", len);
        exit(-1);
    }
    
    // write the block
#ifdef ACTUAL_DISK_IO
    const ULL diskOffset = (ULL)lba * blockSize;
#ifdef USE_MMAP
    memcpy(diskInfo.data + diskOffset, buf, blockSize);
#else /* USE_MMAP */
    if (!m_diskInfo[diskId].isLogDisk && pwrite(diskInfo.fd, buf, len, diskOffset) < 0) {
        debug_error ("pwrite error: %s\n", strerror (errno));
        exit(-1);
    }
#endif /* USE_MMAP */
#endif /* ACTUAL_DISK_IO */

#ifdef DISKLBA_OUT
    fprintf(fp, "%lld %d %lld %d %d\n",ts*1000, diskId, diskOffset / DISK_BLOCK_SIZE, blockSize / DISK_BLOCK_SIZE, 0);
#endif
    return lba;
}

// wrapper for multi-thread partial block write
int DiskMod::writePartialBlock_mt(const disk_id_t diskId, const lba_t lba,
        ULL segmentOffset, int len, const char* buf, std::atomic_int& cnt, 
		LL ts) {
	writePartialBlock(diskId, lba, segmentOffset, len, buf, ts);
	cnt--;
    return lba;
}

// partial block write
int DiskMod::writePartialBlock(const disk_id_t diskId, const lba_t lba,
        ULL segmentOffset, int len, const char* buf, LL ts) {
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    ULL diskOffset = (ULL)lba * blockSize + segmentOffset % blockSize;

    assert(m_diskMutex.count(diskId));
    lock_guard<mutex> lk(*m_diskMutex[diskId]);

    assert(m_diskInfo.count(diskId));
    DiskInfo& diskInfo = m_diskInfo[diskId];

    if (lba < 0 || lba > diskInfo.numBlock) {
        debug_error("Bad LBA: %d\n", lba);
        exit(-1);
    }

    debug("Write to disk %d at %llu (for %d)\n", diskId, diskOffset, len);
#ifdef ACTUAL_DISK_IO
    // TODO : log-structure update (?)
#ifdef USE_MMAP
    memcpy(m_diskInfo[diskId].data + offset, buf, len);
#else
    if (pwrite(m_diskInfo[diskId].fd, buf, len, diskOffset) < 0) {
        debug_error ("pwrite error: %s\n", strerror (errno));
        exit(-1);
    }
#endif /* USE_MMAP */
#endif /* ACTUAL_DISK_IO */

#ifdef DISKLBA_OUT
    fprintf(fp, "%lld %d %lld %d %d\n",ts*1000, diskId, diskOffset / DISK_BLOCK_SIZE, len / DISK_BLOCK_SIZE, 0);
#endif

    return lba; 
}

// wrapper for multi-thread mutli-blocks write
void DiskMod::writeBlocks_mt(disk_id_t diskId, const char* buf, int len, 
        vector<lba_t>& lbaV, std::atomic_int& cnt, LL ts) {
    lba_t lba = writeBlocks(diskId, buf, len, ts);
    lbaV.clear();
    for (int i = 0; i < len; i++) { lbaV.push_back(lba+i); }
    cnt--;
}

void DiskMod::writeSeqBlocks_mt(disk_id_t diskId, const char* buf, int len, 
        lba_t& lba, std::atomic_int& cnt, LL ts) {
    lba = writeBlocks(diskId, buf, len, ts);
    cnt--;
}

int DiskMod::writeBlocks(disk_id_t diskId, const char* buf, int len, LL ts) {
    lock_guard<mutex> lk(*m_diskMutex[diskId]);
    assert(m_diskMutex.count(diskId));
    assert(m_diskInfo.count(diskId));
    assert(m_usedLBA.count(diskId));

    lba_t lba = -1;
	// find an area with enough space for writing multiple blocks
	// TODO: defragmentation ?
    const int blockSize = ConfigMod::getInstance().getBlockSize();
	lba_t& front = m_diskInfo[diskId].dataWriteFront;
	lba = m_usedLBA[diskId]->getFirstZerosAndFlip(front, len);

	// try to fill up holes on data disks
	//if (lba < 0 && !m_diskInfo[diskId].isLogDisk) {
	//	lba = m_usedLBA[diskId]->getFirstZerosAndFlip(0, len);
	//}

	// no free LBA available
    if (lba < 0) {
        debug_error("Error LBA: %d disk full?\n", lba);
        exit(0);
    }

	// update last next unused LBA (write front)
	front = lba + len;

    // write block
    const ULL diskOffset = (ULL)lba * blockSize;

    debug("Write to disk %d at %llu len %d [%x]\n", diskId, diskOffset, 
            blockSize * len, buf[1]);
#ifdef ACTUAL_DISK_IO
#ifdef USE_MMAP
    memcpy(m_diskInfo[diskId].data + diskOffset, buf, blockSize * len);
#else /* USE_MMAP */
    if (!m_diskInfo[diskId].isLogDisk && pwrite(m_diskInfo[diskId].fd, buf, blockSize * len, diskOffset) < 0) {
        debug_error ("pwrite error: %s\n", strerror (errno));
        exit(-1);
    }
#endif /* USE_MMAP */
#endif /* ACTUAL_DISK_IO */

#ifdef DISKLBA_OUT
    fprintf(fp, "%lld %d %lld %d %d\n",ts*1000, diskId, diskOffset / DISK_BLOCK_SIZE, blockSize * len / DISK_BLOCK_SIZE, 0);
#endif

    // deduct remaining space
    m_diskInfo[diskId].remaining -= len * blockSize;
    
    return lba;
}

int DiskMod::readBlock(disk_id_t diskId, lba_t lba, char* buf, off_len_t blockOffLen, LL ts) {
    return readBlock(diskId, lba, (void*) buf, blockOffLen, ts);
}

int DiskMod::readBlock(disk_id_t diskId, lba_t lba, void* buf, off_len_t blockOffLen, LL ts) {
    //lock_guard<mutex> lk(*m_diskMutex[diskId]);
    const int pageSize = ConfigMod::getInstance().getPageSize();
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    
    // round the starting offset and total length to page size
    if (blockOffLen.first % pageSize) {
        blockOffLen.second += (blockOffLen.first % pageSize);
        blockOffLen.first -= (blockOffLen.first % pageSize);
    }
    if (blockOffLen.second % pageSize) {
        blockOffLen.second += pageSize - (blockOffLen.second % pageSize);
    }

    assert(m_diskMutex.count(diskId));
    assert(lba >= 0);
    assert(blockOffLen.first % pageSize == 0);
    assert(blockOffLen.second % pageSize == 0);
    assert(m_diskInfo.count(diskId));
    assert(m_usedLBA.count(diskId));
    assert((ULL) lba * blockSize + blockOffLen.first + blockOffLen.second
            < m_diskInfo[diskId].capacity);

    // read block
#ifdef ACTUAL_DISK_IO
    const ULL diskOffset = (ULL)lba * blockSize + blockOffLen.first;
#ifdef USE_MMAP
    memcpy(buf, m_diskInfo[diskId].data + diskOffset, blockOffLen.second);
#else /* USE_MMAP */
    if (!CacheMod::getInstance().isInit() || !CacheMod::getInstance().getReadBlock(diskId, lba, (char*)buf)) {
        if (pread(m_diskInfo[diskId].fd, buf, blockOffLen.second, diskOffset) < 0 ) {
            debug_error ("pread error: %s, of = %llu, len = %d\n", strerror (errno), diskOffset, blockOffLen.second);
            exit(-1);
        }
    }
#ifdef READ_AHEAD
    posix_fadvise(m_diskInfo[diskId].fd, diskOffset + blockOffLen.second, 
            READ_AHEAD, POSIX_FADV_WILLNEED);
#endif /* READ_AHEAD */
#endif /* USE_MMAP */
#endif /* ACTUAL_DISK_IO */

#ifdef DISKLBA_OUT
    const ULL diskOffset = (ULL)lba * blockSize + blockOffLen.first;
    fprintf(fp, "%lld %d %lld %d %d\n", ts*1000, diskId, diskOffset / DISK_BLOCK_SIZE, blockOffLen.second / DISK_BLOCK_SIZE, 1);
#endif /* DISKLBA_OUT */

    return blockOffLen.second / pageSize; // no of pages
}

void DiskMod::readBlocks_mt(disk_id_t diskId, lba_t startLBA, lba_t endLBA, char* buf,
        std::atomic_int& cnt, LL ts) {
    readBlocks(diskId, startLBA, endLBA, buf, ts);
    cnt--;
}

void DiskMod::readBlocks_mt(disk_id_t diskId, set<lba_t> &LBASet, char* buf, std::atomic_int& cnt, LL ts) {
    ULL blockSize = ConfigMod::getInstance().getBlockSize();
    lba_t startLBA = INVALID_LBA, endLBA = INVALID_LBA;
    set<lba_t>::iterator lbaIt;
    uint32_t lbaRead = 0;
    for (lbaIt = LBASet.begin(); lbaIt != LBASet.end(); lbaIt++) {
        // check if the blocks can be read from cache
        for ( ; CacheMod::getInstance().isInit() && lbaIt != LBASet.end(); lbaIt++) {
            // see if get from cache
            lba_t offset = (startLBA != INVALID_LBA && endLBA != INVALID_LBA)? endLBA - startLBA + 1 : 0;
            if (!CacheMod::getInstance().getReadBlock(diskId, *lbaIt, buf + (lbaRead + offset) * blockSize)) {
                break;
            }
            if (startLBA != INVALID_LBA && endLBA != INVALID_LBA) {
                readBlocks(diskId, startLBA, endLBA, buf + lbaRead * blockSize, ts);
                lbaRead += endLBA - startLBA + 1;
                startLBA = INVALID_LBA;
                endLBA = INVALID_LBA;
            }
            lbaRead += 1;
        }
        if (lbaIt == LBASet.end())
            break;
        if (*lbaIt != endLBA + 1) {
            if (startLBA != INVALID_LBA && endLBA != INVALID_LBA) {
                readBlocks(diskId, startLBA, endLBA, buf + lbaRead * blockSize, ts);
                lbaRead += endLBA - startLBA + 1;
            } else {
                assert(lbaRead > 0 || lbaIt == LBASet.begin());
            }
            // reset the range of address to read
            startLBA = *lbaIt;
            endLBA = *lbaIt;
        } else {
            // expand the range of address to read
            endLBA = *lbaIt;
        }
    }
    if (startLBA != INVALID_LBA && endLBA != INVALID_LBA && lbaRead < LBASet.size()) {
        readBlocks(diskId, startLBA, endLBA, buf + lbaRead * blockSize, ts);
        lbaRead += endLBA - startLBA + 1;
    }
    assert(lbaRead == LBASet.size());
    cnt--;
}

void DiskMod::readBlocks_mt(disk_id_t diskId, lba_t startLBA, lba_t endLBA, char* buf,
        int& ready, std::mutex& lock, LL ts) {
    readBlocks(diskId, startLBA, endLBA, buf, ts);
    lock.lock();
    ready = true;
    lock.unlock();
}

int DiskMod::readBlocks(disk_id_t diskId, lba_t startLBA, lba_t endLBA, char* buf, LL ts) {
    //lock_guard<mutex> lk(*m_diskMutex[diskId]);
    const int pageSize = ConfigMod::getInstance().getPageSize();
    ULL blockSize = ConfigMod::getInstance().getBlockSize();
    ULL len = endLBA - startLBA + 1;

    assert(m_diskMutex.count(diskId));
    assert(startLBA >= 0 && endLBA >= 0);
    assert(len >= 0);

    assert(m_diskInfo.count(diskId));
    assert(m_usedLBA.count(diskId));

    assert(endLBA == 0 || endLBA * blockSize - 1 < (ULL) m_diskInfo[diskId].capacity);

#ifdef ACTUAL_DISK_IO
    if (pread(m_diskInfo[diskId].fd, buf, len * blockSize, startLBA * blockSize) < 0) {
        debug_error ("pread error: %s , ofs = %llu , len = %llu\n", strerror (errno), startLBA * blockSize, len * blockSize);
        exit(-1);
    }
#ifdef READ_AHEAD
    posix_fadvise(m_diskInfo[diskId].fd, (startLBA + len) * blockSize, 
            READ_AHEAD, POSIX_FADV_WILLNEED);
#endif /* READ_AHEAD */
#endif /* ACTUAL_DISK_IO */

#ifdef DISKLBA_OUT
    fprintf(fp, "%lld %d %lld %llu %d\n", ts*1000, diskId, startLBA * blockSize / DISK_BLOCK_SIZE, len * blockSize / DISK_BLOCK_SIZE, 1);
#endif /* DISKLBA_OUT */

    return len * blockSize / pageSize;
}

int DiskMod::setLBAFree(disk_id_t diskId, lba_t lba) {
    lock_guard<mutex> lk(*m_diskMutex[diskId]);
    assert(lba >= 0);
    assert(m_usedLBA.count(diskId));

    m_usedLBA[diskId]->clearBit(lba);

    // update remaining capacity 
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    m_diskInfo[diskId].remaining += blockSize;

    return lba;
}

int DiskMod::setLBAsFree(disk_id_t diskId, lba_t startLBA, int n) {
    lock_guard<mutex> lk(*m_diskMutex[diskId]);
    assert(startLBA >= 0 && n > 0);
    assert(m_usedLBA.count(diskId));

    m_usedLBA[diskId]->clearBitRange(startLBA, n);

	// update remaining capacity
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    if ((lba_t)n >= m_diskInfo[diskId].numBlock) {
        m_diskInfo[diskId].remaining = m_diskInfo[diskId].numBlock * blockSize;
    } else {
        m_diskInfo[diskId].remaining += n * blockSize;
    }

    return startLBA;
}

int DiskMod::getNumDisks(int type) const {
    switch(type) {
        case DATA:
            return m_numDataDisks;
        case LOG:
            return m_numLogDisks;
        default:
            return m_numDisks;
    }
}

vector<disk_id_t> DiskMod::selectDisks(int n, unsigned int hint) const {
    return selectDisks(n, false, hint, set<disk_id_t>());
}

vector<disk_id_t> DiskMod::selectDisks(int n, bool logDisks, 
        unsigned int hint, set<disk_id_t> filter) const {
    // TODO: return random for now
    assert ((int)m_diskInfo.size() >= n);

    vector<disk_id_t> tmpV;
    for (auto & kv : m_diskInfo) {
        // skip log disk for normal selection, 
        // skip non-log disk for log-disk selection
        if ((!logDisks && kv.second.isLogDisk) 
                || (logDisks && !kv.second.isLogDisk)) continue;
		// skip specific disks 
        if (filter.count(kv.first)) continue;
        tmpV.push_back(kv.first);
    }
	// ordered assignment
    //sort(tmpV.begin(), tmpV.end());
    if (logDisks) {
		// assume only minimal # of disks is used, no need to rotate
    } else if (hint >= 0) {
		// round robin
        rotate(tmpV.begin(), tmpV.begin() + (hint % tmpV.size()), tmpV.end());
    } else {
		// random assignment
        random_shuffle(tmpV.begin(), tmpV.end());
    }
    vector<disk_id_t> rtnV (tmpV.begin(), tmpV.begin() + n);
    return rtnV;
}

vector<bool> DiskMod::getDiskStatus (const vector<disk_id_t> diskIds) {
    lock_guard<mutex> lk(m_diskStatusMutex);
    vector<bool> status;
    int diskLogMode = ConfigMod::getInstance().getDiskLogMode();
    for (int i = 0; i < (int)diskIds.size(); i++) {
        int diskId = diskIds[i];

        // partial log segments for update
        if (diskLogMode != DISABLE && diskId == -1) {
            status.push_back(true);
            continue;
        }

        unordered_map<disk_id_t, bool>::const_iterator pos = m_diskStatus.find(diskId);
        if (pos == m_diskStatus.end()) {
            debug_error("Disk status not found: %d\n", diskId);
            exit (-1);
        } else {
            status.push_back(pos->second);
        }
    }
    return status;
}

void DiskMod::setDiskStatus (disk_id_t diskId, bool value) {
    lock_guard<mutex> lk(m_diskStatusMutex);
    assert (m_diskStatus.count(diskId));
    m_diskStatus[diskId] = value;
}

set<disk_id_t> DiskMod::getFailDisks() {
    lock_guard<mutex> lk(m_diskStatusMutex);
    set<disk_id_t> failDisks;
    for (auto& kv: m_diskStatus) {
        if (!kv.second) {
            failDisks.insert(kv.first);
        }
    }
    return failDisks;
}

void DiskMod::fsyncDisks(bool logDisks) {
    for (auto & kv : m_diskInfo) {
        //fsync(kv.second.fd);
        if (kv.second.isLogDisk && !logDisks) continue;
		m_stp.schedule(boost::bind(&DiskMod::fsyncDisk, this, 
				kv.first));
    }
    m_stp.wait();
}

void DiskMod::fsyncDisk(disk_id_t diskId) {
    lock_guard<mutex> lk(*m_diskMutex[diskId]);
	fsync(m_diskInfo[diskId].fd);
}

double DiskMod::getRemainingSpace(int type) const {
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    ULL capacity = 0;
    ULL remaining = 0;
    for (auto & kv : m_diskInfo) {
        // only count data disks
        if (type == DATA && kv.second.isLogDisk) {
            continue;
        }
        // only count log disks
        if (type == LOG && !kv.second.isLogDisk) {
            continue;
        }
        capacity += kv.second.capacity / blockSize;
        remaining += kv.second.remaining / blockSize;
    }
    return (double) remaining / capacity;
}

bool DiskMod::resetDisksWriteFront() {
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    for (auto & kv : m_diskInfo) {
        disk_id_t diskId = kv.second.diskId;
		m_diskMutex[diskId]->lock();
        if (!kv.second.isLogDisk) { 
            m_diskInfo[diskId].dataWriteFront = m_usedLBA[diskId]->getFirstZeroSince(0);
        } else {
            m_diskInfo[diskId].dataWriteFront = LOG_DISK_RAW_WF;
			int freeBlocks = m_diskInfo[diskId].numBlock - LOG_DISK_RAW_WF;
            m_usedLBA[diskId]->clearBitRange(LOG_DISK_RAW_WF, freeBlocks);
            m_diskInfo[diskId].remaining = (ULL) freeBlocks * blockSize;
        }
        debug("reset write front of disk %d to %d\n", diskId, kv.second.dataWriteFront);
        m_diskMutex[diskId]->unlock();
    }
    return true;
}

void DiskMod::printDisksWriteFront() {
    for (auto & kv : m_diskInfo) {
        disk_id_t& diskId = kv.second.diskId;
        fprintf(stderr, "disk %d, data %d, isLog %d\n", diskId,
                m_diskInfo[diskId].dataWriteFront,
                m_diskInfo[diskId].isLogDisk);
    }
}
