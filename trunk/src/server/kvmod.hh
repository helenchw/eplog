#ifndef KVMOD_HH_
#define KVMOD_HH_

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <libcuckoo/cuckoohash_map.hh>
#include "../common/define.hh"
#include "../common/enum.hh"
#include "../common/configmod.hh"
#include "../common/struct/keyvalue.hh"
#include "../common/struct/arc.hh"
#include "../common/struct/cuckoohash.hh"
#include "storagemod.hh"
#include "permetamod.hh"

#ifdef BLOCK_ITF

class KVMod {
public:
#ifdef KV_ON_MD
    KVMod(int md);
#else //ifdef KV_ON_MD
    KVMod(StorageMod *storageMod);
#endif //ifdef KV_ON_MD
    ~KVMod();

    bool set(char *key, const uint8_t keySize, char *value, const uint32_t valueSize, bool force = false);
    bool update(char *key, const uint8_t keySize, char *value, const uint32_t valueSize, bool force = false);
    bool get(char *key, const uint8_t keySize, char **value, uint32_t &valueSize);

    void reportCounts();

private:
#ifdef KV_ON_MD
    int m_md;
#else //ifdef KV_ON_MD
    /**
     * m_storageMod
     * internal pointer to the StorageMod (block interface)
     */
    StorageMod *m_storageMod;
#endif //ifdef KV_ON_MD

    // in-memory hash table for lookup
    std::unordered_set<Data<uint8_t> > _existingKeys;     // existing keys (pointers to memory allocated locally for keys)
    CuckooHash _valueLocMap;  // pointers to value location

    cuckoohash_map<Data<uint8_t>, std::pair<uint64_t, uint32_t>, std::hash<Data<uint8_t> >, std::equal_to<Data<uint8_t> > > _valueLocCache;    // cache of frequently accessed values
    ArcList *_arcKeyList;

    uint64_t _valueNextOffset;
    std::mutex _valueNextOffsetLock;
    char *_valueBuffer;
    struct {
        size_t arcReinsert;
        size_t arcInsert;
        size_t arcEvict;
        size_t setCount;
        size_t hitCount;
        size_t missCount;
    } _counter;

    void touchKeyInCache(Data<uint8_t> key, std::pair<uint64_t, uint32_t> offLen, bool isUpdate = false);
};

#endif //ifdef BLOCK_ITF

#endif //define KVMOD_HH_
