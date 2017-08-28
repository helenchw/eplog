#include "../common/define.hh"
#include "kvmod.hh"
#include <string.h>

#ifdef BLOCK_ITF

#ifdef KV_ON_MD
KVMod::KVMod(int md) {
    m_md = md;
    _valueLocMap.m_md = md;
#else //ifdef KV_ON_MD
KVMod::KVMod(StorageMod *storageMod) {
    m_storageMod = storageMod;
    _valueLocMap.m_storageMod = storageMod;
#endif //ifdef KV_ON_MD

    _valueNextOffset = 0; // Todo: read from CP
    const int blockSize = ConfigMod::getInstance().getBlockSize();
    _valueBuffer = (char*) buf_malloc (sizeof(char) * blockSize);
    if (ConfigMod::getInstance().getKeyCacheSize() > 0) {
        _arcKeyList = new ArcList(ConfigMod::getInstance().getKeyCacheSize());
    } else {
        _arcKeyList = 0;
    }
    _counter = {0, 0, 0, 0, 0, 0};
}

KVMod::~KVMod() {
    reportCounts();
    free(_valueBuffer);
    for (Data<uint8_t> k : _existingKeys) {
        k.release();
    }
}

bool KVMod::update(char *key, uint8_t keySize, char *value, uint32_t valueSize, bool force) {

    //printf("UPDATE key (%u) %.*s of len %u\n", keySize, keySize, key, valueSize);
    if (key == 0 || keySize == 0 || value == 0 || valueSize == 0)
        return false;

    Data<uint8_t> k;
    Data<uint32_t> v;
    k.shadow_set(key, keySize);
    v.shadow_set(value, valueSize);
    std::pair<uint64_t, uint32_t> oldOffLen;

    //printf("UPDATE key (%u) %.*s of len %u\n", keySize, keySize, key, valueSize);

    if (_valueLocCache.find(k, oldOffLen)) {
        _counter.hitCount++;
    } else if ((oldOffLen.first = _valueLocMap.find(key, keySize)) != INVALID_ADDR) {
        _counter.missCount++;
        // key not in cache, update fix-sized value
        /* disk offset and length to read */
        uint32_t metaSize = sizeof(uint8_t) + sizeof(uint32_t);

        /* read metadata from device */
#ifdef KV_ON_MD 
#ifdef DISK_DIRECT_IO
        uint64_t alignedOffset = oldOffLen.first / DISK_BLKSIZE * DISK_BLKSIZE;
        uint32_t readSize = (oldOffLen.first - alignedOffset + metaSize + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
        char *metaBuf = (char*) buf_malloc (readSize);
        int ret = pread(m_md, metaBuf, readSize, alignedOffset);
        memmove(metaBuf, metaBuf + oldOffLen.first - alignedOffset, metaSize);
#else //ifdef DISK_DIRECT_IO
        char *metaBuf = (char*) buf_malloc (metaSize);
        int ret = pread(m_md, metaBuf, metaSize, oldOffLen.first);
#endif //ifdef DISK_DIRECT_IO
        if (ret < 0) {
            printf("Cannot read value at %lu, length %u (%s)!\n", oldOffLen.first, metaSize, strerror(errno));
            assert(0);
            exit(-1);
        } 
#else /* ifdef KV_ON_MD */ 
        char *metaBuf = (char*) buf_malloc (metaSize);
        m_storageMod->read(metaBuf, oldOffLen.first, metaSize);
#endif /* ifdef KV_ON_MD */

        uint32_t oldValueSize = 0;
        memcpy((char*) &oldValueSize, metaBuf + sizeof(uint8_t), sizeof(uint32_t));

        // if value size changes, append as new kv 
        if (oldValueSize != valueSize) {
            assert(0);
            return set(key, keySize, value, valueSize);
        }
        
        oldOffLen = { oldOffLen.first + sizeof(uint8_t) + sizeof(uint32_t) + keySize, valueSize };

        delete metaBuf;
    } else {
        printf("Key not found for UPDATE (%lu, %u)\n", oldOffLen.first, oldOffLen.second);
        assert(0);
        return set(key, keySize, value, valueSize);
    }

    char *writeBuf = 0;
    uint64_t padding = sizeof(uint8_t) + sizeof(uint32_t) + keySize;
#ifdef KV_ON_MD
    uint32_t writeLength = 0;
    uint64_t writeOffset = 0;
#ifdef DISK_DIRECT_IO
    writeOffset = (oldOffLen.first - padding) / DISK_BLKSIZE * DISK_BLKSIZE;
    writeLength = (oldOffLen.first - padding - writeOffset + oldOffLen.second + padding + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
    // unaligned write, need buffer to align it first
    writeBuf = (char*) buf_malloc (writeLength);
    if ((oldOffLen.first - padding) % DISK_BLKSIZE > 0 || (oldOffLen.first + oldOffLen.second) % DISK_BLKSIZE > 0) {
        if (pread(m_md, writeBuf, writeLength, writeOffset) < 0) {
            printf("Cannot read before write at %lu %u (%s)\n", writeOffset, writeLength, strerror(errno));
            assert(0);
        }
    }
#else //ifdef DISK_DIRECT_IO
    writeOffset = oldOffLen.first - padding;
    writeLength = oldOffLen.second + padding;
    writeBuf = (char*) buf_malloc (writeLength);
#endif //ifdef DISK_DIRECT_IO
    memcpy(writeBuf + oldOffLen.first - padding - writeOffset, (char*) &keySize, sizeof(uint8_t));
    memcpy(writeBuf + oldOffLen.first - padding - writeOffset + sizeof(uint8_t), (char*) &valueSize, sizeof(uint32_t));
    memcpy(writeBuf + oldOffLen.first - padding - writeOffset + sizeof(uint8_t) + sizeof(uint32_t), key, keySize);
    memcpy(writeBuf + oldOffLen.first - writeOffset, v.getData(), oldOffLen.second);
    if (pwrite(m_md, writeBuf, writeLength, writeOffset) < 0) {
        printf("Cannot write value\n");
        assert(0);
        exit(-1);
    }
#else //ifdef KV_ON_MD
    //printf("write using read location %lu %u\n", oldOffLen.first, oldOffLen.second);
    writeBuf = (char*) buf_malloc (sizeof(uint8_t) + sizeof(uint32_t) + keySize + valueSize);
    memcpy(writeBuf, (char*) &keySize, sizeof(uint8_t));
    memcpy(writeBuf + sizeof(uint8_t), (char*) &valueSize, sizeof(uint32_t));
    memcpy(writeBuf + sizeof(uint8_t) + sizeof(uint32_t), key, keySize);
    memcpy(writeBuf + sizeof(uint8_t) + sizeof(uint32_t) + keySize, value, valueSize);
    if (m_storageMod->write(writeBuf, oldOffLen.first - padding, oldOffLen.second + padding) == false) {
        return false;
    }
#endif //ifdef KV_ON_MD
    free(writeBuf);

    if (!ConfigMod::getInstance().isCacheAllKeys())
        touchKeyInCache(k, oldOffLen, true);

    //printf("update value at %lu\n", oldOffLen.first);

    return true;
}

bool KVMod::set(char *key, uint8_t keySize, char *value, uint32_t valueSize, bool force) {

    //printf("SET key (%u) %.*s of len %u\n", keySize, keySize, key, valueSize);
    if (key == 0 || keySize == 0 || value == 0 || valueSize == 0)
        return false;

    Data<uint8_t> k;
    Data<uint32_t>  v;
    k.shadow_set(key, keySize);
    v.shadow_set(value, valueSize);
    std::pair<uint64_t, uint32_t> oldOffLen;

    const int blockSize = ConfigMod::getInstance().getBlockSize();
    //printf("SET key (%u) %.*s of len %u\n", keySize, keySize, key, valueSize);

    std::lock_guard<std::mutex> lk (_valueNextOffsetLock);

    // new write
    // counter for debug
    _counter.setCount++;

    // starting offset for writting object
//#if defined(DISK_DIRECT_IO) && defined(KV_ON_MD)
//    // aligned the value to 512B offset for easy write
//    uint64_t padding = sizeof(uint8_t) + sizeof(uint32_t) + keySize;
//    _valueNextOffset = (_valueNextOffset + padding + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE - padding;
//#endif
    uint64_t offset = _valueNextOffset;

    //printf("write kv to %lld value %x key %x\n", _valueNextOffset, value[0]);

#ifdef KV_ON_MD

#define WRITE_LOOP(_DATA_BUF_, _DATA_LEN_) \
    while (remains > 0) { \
        writeSize = remains > blockSize - (offset % blockSize) ? blockSize - (offset % blockSize) : remains; \
        memcpy(_valueBuffer + (offset % blockSize), _DATA_BUF_ + _DATA_LEN_ - remains, writeSize); \
        if ((offset + writeSize) / blockSize != _valueNextOffset / blockSize) { \
            if (pwrite(m_md, _valueBuffer, blockSize, offset / blockSize * blockSize) < 0) { \
                printf("Cannot flush value buffer (%s)\n", strerror(errno)); \
                assert(0); \
                exit(-1); \
            } \
        } \
        remains -= writeSize; \
        offset += writeSize; \
    }

#else /*ifdef KV_ON_MD*/

#define WRITE_LOOP(_DATA_BUF_, _DATA_LEN_) \
    while (remains > 0) { \
        writeSize = remains > blockSize - (offset % blockSize) ? blockSize - (offset % blockSize) : remains; \
        memcpy(_valueBuffer + (offset % blockSize), _DATA_BUF_ + _DATA_LEN_ - remains, writeSize); \
        if ((offset + writeSize) / blockSize != _valueNextOffset / blockSize) { \
            if (m_storageMod->write(_valueBuffer, offset / blockSize * blockSize, blockSize) == false) { \
                return false; \
            } \
        } \
        remains -= writeSize; \
        offset += writeSize; \
    }

#endif /*ifdef KV_ON_MD */ 

    // copy the metadata first
    uint32_t remains = sizeof(uint8_t), writeSize = 0;
    WRITE_LOOP((char*) &keySize, sizeof(uint8_t));
    remains = sizeof(uint32_t);
    WRITE_LOOP((char*) &valueSize, sizeof(uint32_t));
    // copy the key
    remains = keySize;
    WRITE_LOOP(key, keySize);
    // copy the value 
    remains = valueSize;
    WRITE_LOOP(value, valueSize);

#undef WRITE_LOOP

    // update metadata (location of value)
    std::pair<uint64_t, uint32_t> offLen (_valueNextOffset + sizeof(uint8_t) + sizeof(uint32_t) + keySize, v.getSize());
    //printf("key to %lld length %u\n", offLen.first, offLen.second);
    if (!ConfigMod::getInstance().isCacheAllKeys()) {
        // only keep metadata offset if key may not be cached
        _valueLocMap.insert(key, keySize, _valueNextOffset, valueSize);
    }
    // update cache 
    touchKeyInCache(k, offLen);
    _valueNextOffset += sizeof(uint8_t) + sizeof(uint32_t) + k.getSize() + v.getSize();

    // force all data out of buffer
    if (_valueNextOffset % blockSize != 0 && force) {
        memset(_valueBuffer + (_valueNextOffset % blockSize), 0, blockSize - _valueNextOffset % blockSize);
#ifdef KV_ON_MD
        if (pwrite(m_md, _valueBuffer, blockSize, _valueNextOffset / blockSize * blockSize) < 0) {
            printf("Cannot flush value buffer\n");
            assert(0);
            exit(-1);
        }
#else //ifdef KV_ON_MD
        if (m_storageMod->write(_valueBuffer, _valueNextOffset / blockSize * blockSize, blockSize) == false) {
            return false;
        }
#endif //ifdef KV_ON_MD
        _valueNextOffset += blockSize - _valueNextOffset % blockSize;
    }

    return true;
}

bool KVMod::get(char *key, uint8_t keySize, char **value, uint32_t &valueSize) {

    if (key == 0 || keySize == 0 || value == 0)
        return false;

    Data<uint8_t> k;
    k.shadow_set(key, keySize);

    std::pair<uint64_t, uint32_t> offLen;
    uint64_t metaOffset = 0;

    const int blockSize = ConfigMod::getInstance().getBlockSize();

    if (_valueLocCache.find(k, offLen) == false) {
        _counter.missCount++;
        if ((metaOffset = _valueLocMap.find(key, keySize)) == INVALID_ADDR) {
            printf("Key not found\n");
            return false;
        } else {
            if (metaOffset < _valueNextOffset / blockSize * blockSize) {
                //offLen.first = m_perMetaMod->getValue(keySize, offLen.second, metaOffset);
                // get metadata from disk to know the value size
                /* disk offset and length to read */
                uint32_t readLength = sizeof(uint8_t) + sizeof(uint32_t);
#ifdef DISK_DIRECT_IO
                char *metaBuf = (char*) buf_malloc (2 * DISK_BLKSIZE);
#else // ifdef DISK_DIRECT_IO
                char *metaBuf = (char*) buf_malloc (readLength);
#endif // ifdef DISK_DIRECT_IO

                offLen = { metaOffset, readLength };
                /* read metadata from device */
#ifdef KV_ON_MD 
#ifdef DISK_DIRECT_IO
                uint64_t alignedOffset = offLen.first / DISK_BLKSIZE * DISK_BLKSIZE;
                uint32_t readSize = (offLen.first - alignedOffset + offLen.second + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
                int ret = pread(m_md, metaBuf, readSize, alignedOffset);
                memmove(metaBuf, metaBuf + offLen.first - alignedOffset, offLen.second);
#else //ifdef DISK_DIRECT_IO
                int ret = pread(m_md, metaBuf, offLen.second, offLen.first);
#endif //ifdef DISK_DIRECT_IO
                if (ret < 0) {
                    printf("Cannot read metadata at %lu!\n", offLen.first);
                    assert(0);
                    exit(-1);
                } 
#else /* ifdef KV_ON_MD */ 
                m_storageMod->read(metaBuf, offLen.first, offLen.second);
#endif /* ifdef KV_ON_MD */

                offLen.first += sizeof(uint8_t) + sizeof(uint32_t) + keySize;
                memcpy((char*) &offLen.second, metaBuf + sizeof(uint8_t), sizeof(uint32_t));
                delete metaBuf;
                touchKeyInCache(k, offLen, true);
            }
        }
    } else {
        _counter.hitCount++;
    }


#ifdef DISK_DIRECT_IO
    uint64_t alignedOffset = offLen.first / DISK_BLKSIZE * DISK_BLKSIZE;
    uint32_t readSize = (offLen.second + (offLen.first - alignedOffset) + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
    *value = (char*) buf_malloc (sizeof(char) * readSize);
#else //ifdef DISK_DIRECT_IO
    *value = (char*) buf_malloc (sizeof(char) * offLen.second);
#endif //ifdef DISK_DIRECT_IO
    if (*value == 0) {
        printf("failed to malloc\n");
        return false;
    }

    valueSize = offLen.second;

#ifdef KV_ON_MD
#ifdef DISK_DIRECT_IO
    int ret = pread(m_md, *value, readSize, alignedOffset);
    memmove(*value, *value + (offLen.first - alignedOffset), offLen.second);
#else //ifdef DISK_DIRECT_IO
    int ret = pread(m_md, *value, offLen.second, offLen.first);
#endif //ifdef DISK_DIRECT_IO
    if (ret < 0) {
        printf("Cannot read value at %lu length %u (%s)!\n", offLen.first, offLen.second, strerror(errno));
        assert(0);
        exit(-1);
    }
#else //ifdef KV_ON_MD
    m_storageMod->read(*value, offLen.first, offLen.second);
#endif //ifdef KV_ON_MD
    //printf("value = %p size %u from offset %lu %x %x\n", *value, valueSize, offLen.first, (*value)[0], (*value)[1]);

    return true;
}

void KVMod::reportCounts() {
    printf("ARC Insert = %lu Reinsert = %lu Evict = %lu\n", _counter.arcInsert, _counter.arcReinsert, _counter.arcEvict);
    if (_arcKeyList) _arcKeyList->print();
    printf("Set count = %lu; Hit = %lu, Missed = %lu\n", _counter.setCount, _counter.hitCount, _counter.missCount);
    //printf("Next offset = %lu\n", _valueNextOffset);
}

void KVMod::touchKeyInCache(Data<uint8_t> k, std::pair<uint64_t, uint32_t> offLen, bool isUpdate) {
    // Todo cache if needed
    std::pair<Data<uint8_t>, std::vector<std::pair<Data<uint8_t>, bool> > > arcRet;
    Data<uint8_t> tmp;

    //if (ConfigMod::getInstance().isCacheAllKeys() && isUpdate) {
    if (ConfigMod::getInstance().isCacheAllKeys() && !isUpdate) {
        // cache all, no need to maintain ARC
        k.dup();
        _existingKeys.insert(k);
        if (_valueLocCache.insert(k, offLen) == false) {
            assert(0);
            printf("Failed to insert key\n");
        }
    } else {
        // apply the ARC algorithm
        if (_arcKeyList) arcRet = _arcKeyList->insert(k);
    }

    // insert the new key / re-insert a key into cache
    if (arcRet.first.isValid()) {
        //LL metaOffset;
        assert(arcRet.first == k);
        //if (arcRet.first == k) {
            // the key to touch is just inserted into the ARC list
            bool insertRet = _valueLocCache.insert(arcRet.first, offLen);
            assert(insertRet);
            _counter.arcInsert++;
        //} else {
            //// insert / reinsert a key into cache
            //if (!tmp.isValid()) {
            //    printf("Key does not exist for re-insert!!\n");
            //    assert(0);
            //}
            //// insert / reinsert a key into cache
            //if (_valueLocMap.find(tmp, metaOffset) == false) {
            //    printf("Key does not exist for re-insert!!\n");
            //    assert(0);
            //    return;
            //}
            //assert(!_valueLocCache.find(tmp, offLen));

            //offLen.first = m_perMetaMod->getValue(arcRet.first.getSize(), offLen.second, metaOffset);

            //if (_valueLocCache.insert(tmp, offLen) == false) {
            //    assert(0);
            //}
            //_counter.arcReinsert++;
        //}
    }
    // evict cache items
    if (_arcKeyList && !arcRet.second.empty()) {
        Data<uint8_t> keyCopy;
        for (auto &d : arcRet.second) {
            // remove from cache
            _valueLocCache.erase(d.first);
            std::unordered_set<Data<uint8_t> >::iterator keyCopyIt = _existingKeys.find(d.first);
            if (keyCopyIt != _existingKeys.end()) 
                keyCopy = *keyCopyIt;
            _existingKeys.erase(d.first);
            // release the key not longer exists in ARC list
            if (d.second) {
                d.first.release();
                keyCopy.release();
            }
            // release the key in cache
            _counter.arcEvict++;
        }
    }
}

#endif //ifdef BLOCK_ITF
