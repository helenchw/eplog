#include "storagemod.hh"
#include "syncmod.hh"
#include <ctime>
#include <algorithm>
#include <unordered_set>
#include <mutex>

StorageMod::StorageMod(SegmentMetaDataMod* segMetaMod, 
        FileMetaDataMod* fileMetaMod, LogMod* logMod, RaidMod* raidMod, 
        PersistMetaMod* perMetaMod) :
        m_segMetaMod(segMetaMod), m_fileMetaMod(fileMetaMod), m_logMod(logMod), 
                m_raidMod(raidMod), m_perMetaMod(perMetaMod),
                m_logLevel(ConfigMod::getInstance().getLogLevel()), 
                m_updateLogLevel(ConfigMod::getInstance().getUpdateLogLevel()), 
                m_segmentSize(ConfigMod::getInstance().getSegmentSize()) {
}

StorageMod::~StorageMod() {
    // TODO Auto-generated destructor stub
}

#ifndef BLOCK_ITF
int StorageMod::read(const char* path, char* buf, LL offset, int length, LL ts) {
    FileMetaData* fmd;
    unordered_set<sid_t> logSegmentIds;
    getFileMetaData(path,&fmd);
    assert (fmd != nullptr);

    // lock shared first to avoid findCover change
    fmd->fileMutex.lock_shared();

    m_logMod->getAliveSegmentIds(logSegmentIds);
    m_logMod->getAliveUpdateSegmentIds(logSegmentIds);
    FileLocIt lit, rit;
    tie (lit, rit) = fmd->findCover(offset, length);
    LL curOffset = offset;
    int remainLen = length;
    for (FileLocIt it = lit; it != rit && remainLen > 0; it++) {
        const sid_t segmentId = it->segmentMetaData->segmentId;
        const LL itOffset = it->fileOffset;
        const int itLength = it->locLength;
        //debug ("remainLen = %d, itLength = %d, itOffset = %d, curOffset = %d, offset = %d, length = %d\n", remainLen, itLength, itOffset, curOffset, offset, length);
        debug ("%d B remains, read segment %d from %lld to %lld at buf %lld\n", remainLen, segmentId, itOffset, itOffset+itLength, curOffset-offset);
        int len = (int)std::min((ULL)remainLen, (ULL)itLength - (ULL)(curOffset - itOffset));
        bool readDisk = false;
        // 1. update buffer , 2. new write buffer , 3. on disk
        if ((m_updateLogLevel > 0 || m_logLevel > 0) && logSegmentIds.count(segmentId)) {
            // data in update log buffer / or new write buffer
            readDisk = !m_logMod->readLog(buf+(curOffset-offset), segmentId,
              make_pair(it->segmentOffset+(curOffset-itOffset), len));
        } else {
            // data flushed on disks
            readDisk = true;
        }
        if (readDisk) {
            m_raidMod->readSegment(it->segmentMetaData, buf+(curOffset-offset),
                    make_pair(it->segmentOffset+(curOffset-itOffset), len), ts);
        }
        curOffset += len;
        remainLen -= len;
    }
    fmd->fileMutex.unlock_shared();
    return length;
}
#else /*ifndef BLOCK_ITF */
int StorageMod::read(char* buf, LL offset, int length, LL ts) {

    int segmentSize = ConfigMod::getInstance().getSegmentSize();
    int chunkSize = m_logMod->getChunkSize();

    sid_t startSegmentId = offset / segmentSize;
    sid_t endSegmentId = (offset + length + segmentSize - 1) / segmentSize; // exclusive

    LL volOff = offset;
    int remainLen = length;

    debug ("read off %llu len %d, start %d end %d\n", volOff, length, startSegmentId, endSegmentId);
    for (sid_t sid = startSegmentId; sid < endSegmentId; sid++) {
        if (m_segMetaMod->m_metaMap.count(sid) < 1) 
            continue;
        SegmentMetaData * smd = m_segMetaMod->m_metaMap[sid];
        int off = volOff % segmentSize;
        int len = min(segmentSize - off, remainLen);
        bool readDisk = true;

        unordered_set<sid_t> logSegmentIds;
        m_logMod->getAliveSegmentIds(logSegmentIds);
        unordered_map<chunk_id_t, pair<int, chunk_id_t> > levels;
        m_logMod->getAliveUpdateSegmentIds(sid, levels);

        if (logSegmentIds.count(sid)) {
        // see if content is in new buffer
            debug ("read new log %d off %d len %d to buf %llu\n", sid, off, len, volOff - offset);
            readDisk = !m_logMod->readLog(buf+(volOff-offset), sid,
              make_pair(off, len), levels);
        } else if (levels.size() > 0) {
        // see if (part of the ) content is in update buffer
            int startChunkId = off / chunkSize;
            int endChunkId = (off + len + chunkSize -1) / chunkSize; // exclusive
            for (chunk_id_t cid = startChunkId; cid < endChunkId; cid++) {
                off = volOff % segmentSize;
                int clen = min(chunkSize, len - ((int)(volOff % segmentSize) - off % segmentSize));
                readDisk = true;
                // in buffer
                if (levels.count(cid) > 0) {
                    debug ("in log read log %d off %d len %d\n", sid, off, clen);
                    readDisk = !m_logMod->readLog(buf+(volOff-offset), sid,
                            make_pair(off, clen), levels);
                }
                // on disk
                if (readDisk) {
                    debug ("in log read disk %d off %d len %d\n", sid, off, clen);
                    m_raidMod->readSegment(smd, buf + (volOff - offset),
                            make_pair(off, clen), ts);
                }
                volOff += clen;
            }
            readDisk = false;
        }
        // read all from disk
        if (readDisk) {
            debug ("disk %d off %d len %d to buf %llu vol %llu off %llu\n", 
                    sid, off, len, volOff - offset, volOff, offset);
            m_raidMod->readSegment(smd, buf + (volOff - offset),
                    make_pair(off, len), ts);
        }
        // update read offset and len
        volOff = ((LL) sid + 1) * segmentSize;
        remainLen -= len;
    }

    return length - remainLen;
}
#endif

#ifndef BLOCK_ITF
int StorageMod::write(const char* path, char* buf, LL offset, int length, LL ts) {
    FileMetaData* fmd;
    // distinguish update from new writes 
    int diskLogMode = ConfigMod::getInstance().getDiskLogMode();
    bool isUpdate = getFileMetaData(path,&fmd); 
    debug ("fmd %p isUpdate %d diskLogMod %d\n", fmd, (int)isUpdate, diskLogMode);
    assert (fmd != nullptr);
    FileLocIt lit, rit, it;

    int level = 0;

    // lock flush mutex, as we may flush after write
    // TODO: concurrent writers
    std::lock_guard<std::mutex> lk (m_logMod->m_flushMutex);

    // Priority:
    // 1. Full stripe writes
    // 2a. Logging (NVRAM) for new writes
    // 2b. Logging (HDDs) for updates
    int remainLen = length;
    SegmentMetaData *smd = nullptr;
    while (remainLen > 0) {
        debug("start writing %d remains\n", remainLen);

        // append to the end of file
        if (offset >= fmd->fileSize) { isUpdate = false; }

        if (isUpdate && diskLogMode != DISABLE) {
            LL curLen = remainLen;
            bool toUpdateBuf = false;       // looks redundant??
            if (remainLen > m_segmentSize) {
                curLen = m_segmentSize;   
            }

            // handle a mixture of update and new bytes
            LL newbytes = offset + curLen - fmd->fileSize;
            if (newbytes > 0) {
                curLen = curLen - newbytes;
                toUpdateBuf = true;
            } else {
                toUpdateBuf = true;
            }

            tie (lit, rit) = fmd->findCover(offset,curLen);      // find segments involved
#ifdef UPDATE_DATA_IN_PLACE
            it = lit; it++;
            assert(it == rit || ++it == rit); // modfication can span 1 or 2 segments only

            for (it = lit; it != rit ; it++) {
                smd = m_segMetaMod->getEntry(it->segmentMetaData->segmentId);
                debug("Loc len:%d, off:%d, fOff:%lld\n", it->locLength, it->segmentOffset, it->fileOffset);
                debug ("%d [%d] log to seg %d\n", (int)isUpdate, diskLogMode, smd->segmentId);

                vector<off_len_t> offLens;
                int len = min(curLen,int(it->locLength-(offset-it->fileOffset)));
                offLens.push_back(make_pair(offset-it->fileOffset,len));
                // Update (partial) segments
                m_raidMod->writePartialSegment(smd, buf, offLens, ts, false); 

                switch(diskLogMode) {
                    default:
                        cerr << "Unknown disk log mode!!" << endl;
                        break;
                }
                offset += len;
                remainLen -= len;
            }
#else
            fmd->fileMutex.lock();
            // buffer encode log writes
            m_logMod->writeLog(buf, level, fmd, make_pair (offset, curLen), false, ts, toUpdateBuf);
            fmd->fileMutex.unlock();

            buf += curLen;
            offset += curLen;
            remainLen -= curLen;
#endif /* UPDATE_DATA_IN_PLACE */
        } else {
            if (remainLen >= m_segmentSize && m_logLevel < 1) {
                fmd->fileMutex.lock();
                smd = m_segMetaMod->createEntry();
                smd->fileSet.insert(fmd);
                smd->lookupVector.push_back (make_pair(0, fmd));
                smd->fileOffsetMap[0] = offset;
                // add fmd to lookup vector
                m_raidMod->writeSegment(smd, buf, ts);
                fmd->fileSize = offset + m_segmentSize;
                m_fileMetaMod->updateFileLocs(fmd, offset, m_segmentSize, smd, 0);
                fmd->fileMutex.unlock();
                remainLen -= m_segmentSize;
                buf += m_segmentSize;
                offset += m_segmentSize;
                debug ("direct write to segment %d fileset size = %zu\n", smd->segmentId, smd->fileSet.size());
            } else { // m_logLevel > 1 || ( m_logLevel < 1 && remainLen < m_segmentSize)
                int curLen = min(remainLen, m_segmentSize);
                fmd->fileMutex.lock();
                m_logMod->writeLog(buf, level, fmd, make_pair (offset, curLen), false, ts);
                fmd->fileSize = offset + curLen;
                fmd->fileMutex.unlock();
                remainLen -= curLen;
                buf += curLen;
                offset += curLen;
            //    break;
            }
        }
    
    }

    // mapping of a file changes only when its file size changes
    if (m_perMetaMod && !isUpdate) { m_perMetaMod->addDirtyFile(fmd->fileId); }
    return length;
}
#else /** BLOCK_ITF **/
/** block interface */
int StorageMod::write(char* buf, LL offset, int length, LL ts) {

    std::lock_guard<std::mutex> lk (m_logMod->m_flushMutex);

    int segmentSize = ConfigMod::getInstance().getSegmentSize();
    int chunkSize = m_logMod->getChunkSize();
    sid_t startSegmentId = offset / segmentSize;
    sid_t endSegmentId = (offset + length + segmentSize - 1) / segmentSize; // exclusive

    LL volOff = offset;
    int curLen = min(length, (int) (segmentSize - volOff % segmentSize));
    int remainLen = length;

    for (sid_t i = startSegmentId; i < endSegmentId; i++) {

        SegmentMetaData* smd = nullptr;

        chunk_id_t startChunk = (volOff % segmentSize) / chunkSize;
        chunk_id_t endChunk = (volOff % segmentSize + curLen -1) / chunkSize;

        bool isUpdate = m_segMetaMod->m_metaMap.count(i);
        if (isUpdate) { 
            smd = m_segMetaMod->m_metaMap[i];
            if (smd->locations.size() > (unsigned) startChunk) {
                isUpdate &= (smd->locations[startChunk].first != INVALID_DISK);
            } else {
                isUpdate = false;
            }
        }
        //printf("chunk offset i %d %llu len %d end %d update? %d\n", i, offset, length, endChunk, isUpdate);

        if (isUpdate && ((int) smd->locations.size() < endChunk || 
                (smd->locations[startChunk].first != INVALID_DISK && 
                smd->locations[endChunk].first == INVALID_DISK)) ) {
            // partial update + partial new write -> update first
            for (unsigned int j = 0; j < smd->locations.size(); j++) {
                if (smd->locations[j].first != INVALID_DISK) continue;
                endChunk = j-1;
                break;
            }
            assert(endChunk != INVALID_CHUNK);
            int updateLen = endChunk * chunkSize - volOff % segmentSize;
            m_logMod->writeLog(buf + (length - remainLen), 0, nullptr, make_pair(volOff, updateLen), (i+1 == endSegmentId), ts, true);
            remainLen -= updateLen;
            curLen -= updateLen;
            volOff += updateLen;
            isUpdate = false;
        }

        if (curLen > 0) {
            // leave the job of determining which buffer to use to logMod
            //printf("writeLog %llu len %d, sid start %d end %d cur %d, is %d\n", offset, length, startSegmentId, endSegmentId, i, isUpdate);
            m_logMod->writeLog(buf + (length - remainLen), 0, nullptr, make_pair(volOff, curLen), (i+1 == endSegmentId), ts, isUpdate);
        }

        remainLen -= curLen;
        volOff = ((LL) i + 1) * segmentSize;
        curLen = min(remainLen, (int) (segmentSize - volOff % segmentSize));
    }
    
    return length;
}
#endif /** BLOCK_ITF **/

bool StorageMod::syncAllOnDiskLog() {
    return m_raidMod->syncAllSegments();
}

// return TRUE if "IsUpdate", FALSE if "IsNewWrite"
bool StorageMod::getFileMetaData (const char* path, FileMetaData** fileMeta) {
    *fileMeta = nullptr;
    bool isUpdate = false;
    if (m_fileMetaMod->existEntry(path)) {
        /* if isExists, then isUpdate */
        isUpdate = m_fileMetaMod->getEntry(path, fileMeta);
    } else {
        /* if isCreated, then !isUpdate */
        isUpdate = !m_fileMetaMod->createEntry(path, fileMeta);
    }
    return isUpdate;
}

int StorageMod::flushUpdateLog(LL ts) {
    std::lock_guard<std::mutex> lk (m_logMod->m_flushMutex);
    return m_logMod->flushUpdateInChunkGroups(true);
}

