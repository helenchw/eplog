#include "cachemod.hh"
pair<uint32_t,uint32_t> CacheMod::initCacheMod(uint32_t readCacheSize, uint32_t writeCacheSize, 
            SegmentMetaDataMod *segMetaMod, DiskMod *diskMod) {
    m_segMetaMod = segMetaMod;
    m_diskMod = diskMod;
    readCacheSize = initReadCache(readCacheSize);
    writeCacheSize = 0;
    return pair<uint32_t, uint32_t>(readCacheSize, writeCacheSize);
}

uint32_t CacheMod::initReadCache(uint32_t cacheSize) {
    uint32_t blockSize = ConfigMod::getInstance().getBlockSize();
    uint32_t numPagePerBlock = ConfigMod::getInstance().getNumPagePerBlock();
    uint32_t roundedCacheSize = cacheSize / numPagePerBlock;
    int numThread = ConfigMod::getInstance().getNumThread();

    this->m_readCache.lock.lock();
    if (this->m_readCache.inUse.size() != roundedCacheSize) {
        if (this->m_readCache.cache != 0 && !this->m_readCache.inUse.empty())
            free(this->m_readCache.cache);
        this->m_readCache.cache = (char*) buf_calloc (blockSize * roundedCacheSize, sizeof(char));
    }
    if (m_readCache.cache == 0) {
        printf("Failed to allocate read cache of %u pages\n", cacheSize);
        this->m_readCache.lock.unlock();
        return 0;
    }
    this->m_readCache.size = roundedCacheSize;
    this->m_readCache.inUse.resize(roundedCacheSize);
    this->m_readCache.ready.ready.resize(roundedCacheSize);
    // reset cache in-use
    for (uint32_t i = 0; i < roundedCacheSize; i++) {
        this->m_readCache.inUse[i] = false;
        this->m_readCache.ready.ready[i] = false;
    }
    this->m_readCache.lastUsed = -1;
    this->m_readCache.blockMap.clear();
	this->m_tp.size_controller().resize(numThread * 2);
    this->m_readCache.lock.unlock();
    return roundedCacheSize * numPagePerBlock;
}

bool CacheMod::insertReadBlock(disk_id_t diskId, lba_t lba) {
    // stop if yet initialized
    if (!this->isInit()) {
        return false;
    }
    uint32_t blockSize = ConfigMod::getInstance().getBlockSize();
    pair<disk_id_t, lba_t> newBlock(diskId, lba);

    if (diskId == INVALID_DISK || lba == INVALID_LBA)
        return false;

    this->m_readCache.lock.lock();
    unordered_map<pair<disk_id_t, lba_t>, uint32_t>::iterator it = this->m_readCache.blockMap.find(newBlock);
    bool found = it != this->m_readCache.blockMap.end();
    uint32_t curCacheIndex = found ? it->second : this->incLastUsed();
    if (!found && this->m_readCache.inUse[curCacheIndex] == true) {
        auto oldBlock= this->m_readCache.blockReverseMap.find(curCacheIndex);
        assert(oldBlock!= this->m_readCache.blockReverseMap.end());
        // replace a current block
        this->m_readCache.blockMap.erase(oldBlock->second);
        this->m_readCache.blockReverseMap.erase(oldBlock->first);
    }
    // update the mapping
    this->m_readCache.blockMap.insert(pair< pair<disk_id_t, lba_t>, uint32_t>(newBlock, curCacheIndex));
    this->m_readCache.blockReverseMap.insert(pair<uint32_t, pair<disk_id_t, lba_t> >(curCacheIndex, newBlock));
    // reset the cache status
    this->m_readCache.inUse[curCacheIndex] = true;
    this->m_readCache.ready.lock.lock();
    // read the block if it is not in cache and is not found
    // TODO: also update read cache when block becomes dirty?
    if (!found) {
        this->m_readCache.ready.ready[curCacheIndex] = false;
        m_tp.schedule(boost::bind(&DiskMod::readBlocks_mt, this->m_diskMod,
                diskId, lba, lba, this->m_readCache.cache + curCacheIndex * blockSize,
                boost::ref(this->m_readCache.ready.ready[curCacheIndex]),
                boost::ref(this->m_readCache.ready.lock), 0
        ));
    }
    this->m_readCache.ready.lock.unlock();
    this->m_readCache.lock.unlock();
    return true;
}

bool CacheMod::getReadBlock(disk_id_t diskId, lba_t lba, char *buf) {
    if (!this->isInit()) {
        return false;
    }
    uint32_t blockSize = ConfigMod::getInstance().getBlockSize();
    pair<disk_id_t, lba_t> block(diskId, lba);
    bool success = false;

    this->m_readCache.lock.lock();
    unordered_map<pair<disk_id_t, lba_t>, uint32_t>::iterator it = this->m_readCache.blockMap.find(block);
    if (it != this->m_readCache.blockMap.end()) {
        this->m_readCache.ready.lock.lock();
        assert(it->second < this->m_readCache.size);
        if (this->m_readCache.ready.ready[it->second]) {
            memcpy(buf, this->m_readCache.cache + it->second * blockSize, blockSize);
            success = true;
        }
        this->m_readCache.ready.lock.unlock();
    }
    this->m_readCache.lock.unlock();
    return success;
}

bool CacheMod::removeReadBlock(disk_id_t diskId, lba_t lba) {
    pair<disk_id_t, lba_t> block(diskId, lba);
    this->m_readCache.lock.lock();
    printf("Not implemented\n");
    this->m_readCache.lock.unlock();
    return false;
}

bool CacheMod::clearReadCache(bool freeAll) {
    // TODO not yet implemented
    printf("Not implemented\n");
    return false;
}
