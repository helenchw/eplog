#ifndef __CACHEMOD_HH__
#define __CACHEMOD_HH__

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <pthread.h>
#include "lib/threadpool/threadpool.hpp"
#include "common/define.hh"
#include "common/configmod.hh"
#include "server/segmetamod.hh"
#include "server/diskmod.hh"

using namespace std;

namespace std
{
    template<> struct hash< pair<disk_id_t, lba_t> >
    {
        size_t operator()(pair<disk_id_t, lba_t> const& s) const
        {
            size_t const h1 ( std::hash<disk_id_t>()(s.first) );
            size_t const h2 ( std::hash<lba_t>()(s.second) );
            return h1 ^ (h2 << 1); // or use boost::hash_combine
        }
    };
}

class CacheMod {
public:
    static CacheMod& getInstance() {
        static CacheMod instance; // Guaranteed to be destroyed
        // Instantiated on first use
        return instance;
    }

    // init the cache module
    pair<uint32_t,uint32_t> initCacheMod(uint32_t readCacheSize, uint32_t writeCacheSize, 
            SegmentMetaDataMod *segMetaMod, DiskMod *diskMod);
    // return the actual amount of cache allocated (in page)
    uint32_t initReadCache(uint32_t cacheSize);
    // return true if cache is cleared
    bool clearReadCache(bool freeAll = false);
    // return true if cache is inserted
    bool insertReadBlock(disk_id_t diskId, lba_t lba);
    // return true if cache is found and copied
    bool getReadBlock(disk_id_t diskId, lba_t lba, char *buf);
    // return true if cache is removed 
    bool removeReadBlock(disk_id_t diskId, lba_t lba);
    // return true if init
    bool inline isInit() {
        return !(m_segMetaMod == 0 || m_diskMod == 0 || this->m_readCache.cache == 0);
    };

private:
    CacheMod() {}
    CacheMod(CacheMod const&); // Don't Implement
    void operator=(CacheMod const&); // Don't implement

    struct {
        char *cache; // read cache content;
        mutex lock; // read cache mutex for inserting or evicting cache
        unordered_map<pair<disk_id_t, lba_t>, uint32_t> blockMap; // map blocks to cache 
        unordered_map< uint32_t, pair<disk_id_t, lba_t> > blockReverseMap; // map cache to block
        vector<bool> inUse; // per block 
        uint32_t lastUsed;
        uint32_t size; // in blocks 
        struct {
            vector<int> ready; // whether pages is already read 
            mutex lock;
        } ready;
    } m_readCache;

    boost::threadpool::pool m_tp; // thread pool for I/O

    // assume already locked
    uint32_t inline incLastUsed(bool read = true) {
        if (read) {
            if (++this->m_readCache.lastUsed >= this->m_readCache.size) {
                this->m_readCache.lastUsed = 0;
            }
            return this->m_readCache.lastUsed;
        } else {
            return 0;
        }
    }

    // assume already locked
    void inline resetLastUsed(bool read = true) {
        if (read) {
            this->m_readCache.lastUsed = -1;
        }
    }

    SegmentMetaDataMod *m_segMetaMod;
    DiskMod *m_diskMod;

};

#endif
