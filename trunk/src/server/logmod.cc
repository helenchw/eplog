#include "server/logmod.hh"

#include <unordered_set>
#include <unordered_map>
#include <atomic>
#include <assert.h>
#include "common/debug.hh"
#include "coding/coding.hh"

LogMod::LogMod(RaidMod* raidMod, SegmentMetaDataMod* segmetaMod, 
        FileMetaDataMod* filemetaMod, SyncMod* syncMod, 
        CodeSetting codeSetting, PersistMetaMod* perMetaMod):
    m_logLevel(ConfigMod::getInstance().getLogLevel()),
    m_updateLogLevel(ConfigMod::getInstance().getUpdateLogLevel()),
    m_segmentSize(ConfigMod::getInstance().getSegmentSize()),
    m_logSegmentSize(ConfigMod::getInstance().getLogSegmentSize()),
    m_logBlockNum(codeSetting.k), 
    m_logCodeBlockNum(codeSetting.n - codeSetting.k),
    m_chunkGroupSize(ConfigMod::getInstance().getChunkGroupSize()),
    m_raidMod(raidMod), m_segmetaMod(segmetaMod), m_filemetaMod(filemetaMod),
    m_syncMod(syncMod), m_perMetaMod(perMetaMod), m_curLogLevel(0), 
    m_lastFlushLog(-1) 
{
    assert(m_logSegmentSize % m_logBlockNum == 0);


    // special case: no new write buffer: cache parity in memory, 
    // flush to log disk after each update
    if (m_logLevel < 1) {

        // cache parity to avoid RMW, just MW
        m_logbuffers.resize(2);
        m_logbuffers[0].buf = (char*) buf_malloc (m_segmentSize * sizeof(char));
        m_logbuffers[0].curOff = 0;
        m_logbuffers[0].logMutex = new RWMutex();
        m_logbuffers[0].chunkBitmap = 0;
#ifndef BLOCK_ITF
        m_logbuffers[0].segmentMetaData = m_segmetaMod->createEntry();
        m_segmentIdMap[m_logbuffers[0].segmentMetaData->segmentId] = 0;
#endif

        // buffer segment for encoding
        m_logbuffers[1].buf = (char*) buf_malloc (m_segmentSize * sizeof(char) * 2);
        m_logbuffers[1].logMutex = new RWMutex();

    } else {

        m_logbuffers.resize(m_logLevel);
        // use global mapping
        m_logMapping = (char*) buf_malloc (m_segmentSize * 2 * sizeof (char) * m_logLevel);
    }

    for (int bufid = 0; bufid < m_logLevel; bufid++)
    {
        m_logbuffers[bufid].buf = m_logMapping + m_segmentSize * 2 * bufid;
        m_logbuffers[bufid].curOff = 0;
        m_logbuffers[bufid].logMutex = new RWMutex();
        m_logbuffers[bufid].chunkBitmap = 0;
#ifndef BLOCK_ITF
        m_logbuffers[bufid].segmentMetaData = m_segmetaMod->createEntry();
        m_segmentIdMap[m_logbuffers[bufid].segmentMetaData->segmentId] = bufid;
#endif
    }

    // special case: no update buffer: generate log parity for each write request
    if (m_updateLogLevel > 0) {
        m_updateLogbuffers.resize(m_updateLogLevel);
    } else {
        // reserve a global segment for encode
        m_updateLogbuffers.resize(1);
        m_updateLogbuffers[0].buf = (char*) buf_calloc (m_logSegmentSize, sizeof(char));
        m_updateLogbuffers[0].logMutex = new RWMutex();
        m_updateLogbuffers[0].segmentMetaData = m_segmetaMod->createEntry(true);
        m_updateLogbuffers[0].curOff = 0;
        m_updateLogbuffers[0].chunkBitmap = 0;
        m_updateSegmentIdMap[m_updateLogbuffers[0].segmentMetaData->segmentId] = 0;
        m_updateLogbuffers[0].updateSegments.resize(m_logBlockNum);
        m_updateLogbuffers[0].chunkMap.resize(m_logBlockNum);
    }

    for (int bufid = 0; bufid < m_updateLogLevel; bufid++) 
    {
        m_updateLogbuffers[bufid].buf = (char*)buf_calloc(m_logSegmentSize, sizeof(char));
        m_updateLogbuffers[bufid].curOff = 0;
        m_updateLogbuffers[bufid].segmentMetaData = m_segmetaMod->createEntry(true);
        m_updateLogbuffers[bufid].logMutex = new RWMutex();
        m_updateLogbuffers[bufid].chunkBitmap = 0;
        m_updateSegmentIdMap[m_updateLogbuffers[bufid].segmentMetaData->segmentId] = bufid;

        m_updateLogbuffers[bufid].updateSegments.resize(m_logBlockNum);
        m_updateLogbuffers[bufid].chunkMap.resize(m_logBlockNum);
    }

    m_chunkGroupLogSegMeta.resize(m_chunkGroupSize);
    m_chunkGroupLastFlush.resize(m_logBlockNum);
    m_chunkGroupDataEnd.resize(m_logBlockNum);
    int chunkSize = m_logSegmentSize / m_logBlockNum;
    m_chunkGroupLogBuffer = 
            (char**) buf_malloc (sizeof(char*) * (m_logBlockNum + m_logCodeBlockNum));

    for (int cg = 0; cg < m_chunkGroupSize; cg++) {
        m_chunkGroupLogSegMeta[cg] = m_segmetaMod->createEntry(true);
    }

    for (int i = 0; i < m_logBlockNum; i++) {
        m_chunkGroupLogBuffer[i] = (char*) buf_malloc (sizeof(char) * chunkSize * m_chunkGroupSize);
        m_chunkGroupLastFlush[i] = -1;
        m_chunkGroupDataEnd[i] = -1;
    }

    for (int i = m_logBlockNum; i < m_logBlockNum + m_logCodeBlockNum; i++) {
        m_chunkGroupLogBuffer[i] = (char*) buf_malloc (sizeof(char) * chunkSize * m_chunkGroupSize);
    }

#ifdef STRIPE_PLOG
    m_curUpdateLogLevel = 0;
#endif
    int numThread = ConfigMod::getInstance().getNumThread();
	m_tp.size_controller().resize(numThread * 2);
}

LogMod::~LogMod() {
    for (int bufid = 0; bufid < m_logLevel; bufid++)
    {
        free(m_logbuffers[bufid].buf);
    }
    for (int bufid = 0; bufid < m_updateLogLevel; bufid++) 
    {
        free(m_updateLogbuffers[bufid].buf);
    }
}

#ifndef BLOCK_ITF
// TODO: Save filemetadata info here
bool LogMod::writeLog (char* buf, int level, FileMetaData* fmd, 
        foff_len_t fileOffLen, bool isGCWrite, LL ts, bool isUpdate) {
    // Get reference and init return value
    int buflen = fileOffLen.second;
    int chunkSize = m_logSegmentSize / m_logBlockNum;
    debug ("isGC = %d fileid = %d fileLen %lld, fileOfflen = %d isUpdate = %d\n", 
            isGCWrite, fmd->fileId, fileOffLen.first, buflen, isUpdate);

    
    if (isUpdate) { // update log buffer

        // disk-oriented buffer
        sid_t segmentId = INVALID_SEG; 
        chunk_id_t chunkId = INVALID_CHUNK; 
        disk_id_t diskId = INVALID_DISK;
        FileLocIt lit, rit;
        int i = 0, rounds= 0; 
		LL curOff = 0;

        // "block-wise" logging
        int len = chunkSize;
        bool isPartialUpdate = false;

        for (curOff = fileOffLen.first; (ULL)curOff - fileOffLen.first < (ULL)buflen; curOff += len) {
            // assume full chunk update
            len = chunkSize;
            isPartialUpdate = false;
            int bufCurOff = curOff - fileOffLen.first;

            // segments affected when this block is updated
            tie (lit, rit) = fmd->findCover(curOff, chunkSize);

            // accross two chunks (&& new write are aligned to chunkSize) -> partial chunk update 
            if (lit != --rit) {
                len = lit->locLength - (curOff - lit->fileOffset);
                isPartialUpdate = true;
            } 
            // relax update size
            if (buflen < len) { 
                len = (int) buflen;
                isPartialUpdate = true;
            }
            // constraint on chunk-by-chunk update
            LL segOfs = lit->segmentOffset + (curOff - lit->fileOffset);
            if (len > (chunkSize - segOfs % chunkSize)) {
                len = chunkSize - segOfs % chunkSize;
                isPartialUpdate = true;
            }

            chunkId = (lit->segmentOffset+(curOff-lit->fileOffset)) / chunkSize;
            segmentId = lit->segmentMetaData->segmentId;

            // identify if the segment belongs to new write log buffer
            bool skipUpdateBuffer = false;
            bool newWriteBuffer = false;
            m_segmentIdMapMutex.lock_shared();
            if (m_segmentIdMap.count(segmentId) && m_logLevel > 0) {
                LogBuffer &logbuf = m_logbuffers[m_segmentIdMap[segmentId]];

                logbuf.logMutex->lock();
                int logbufOff = lit->segmentOffset + (curOff - lit->fileOffset);
                debug("overwrite new write buffer segment %d from %d [%c]\n", 
                        segmentId, logbufOff, buf[bufCurOff]);
                // directly overwrite the new write log buffer
                memcpy(logbuf.buf + logbufOff, buf + bufCurOff, len);
                logbuf.logMutex->unlock();

                skipUpdateBuffer = true;
                newWriteBuffer = true;
            }
            m_segmentIdMapMutex.unlock_shared();

            // overwrite update in update buffer if alread exists
            if (lit->segmentMetaData->locations.empty() && !skipUpdateBuffer) {
                m_updateSegmentIdMapMutex.lock_shared();
                int i =  m_updateSegmentIdMap[segmentId];
                // chunk ofs in log segment + byte ofs in original data chunk
                int logbufOff = (lit->segmentOffset + int(curOff - lit->fileOffset)) % chunkSize;
                m_updateSegmentIdMapMutex.unlock_shared();

                LogBuffer& logbuf = m_updateLogbuffers[i];
                logbuf.logMutex->lock();
                diskId = (lit->segmentOffset + (curOff - lit->fileOffset) ) / chunkSize;
                chunkId = logbuf.chunkMap[diskId].first;

                debug("Overwritten update in buf[%d] from %d len %d [%c]>[%c]\n", 
                        i, logbufOff, len, logbuf.buf[logbufOff],
                        buf[bufCurOff]);
                memcpy(logbuf.buf + diskId * chunkSize + logbufOff, buf+bufCurOff, len);
                logbuf.logMutex->unlock();

                skipUpdateBuffer = true;
            }

            debug("writing %lld\n", curOff - fileOffLen.first);

            // case 1. find a free segment; case 2. flush a segment for a free segment
            // TODO : active flush log buffer policies, current policy only flushes before write
            for (rounds = 0; rounds < 2; rounds++) {

                assert(chunkId >= 0);

                if (rounds) { 
                    // already check if flush is necessary for m_updatelogLevel<0
                    if (skipUpdateBuffer) break;
                    // since fileLocs may be changed due to update flush, need to update iterators
                    // segments affected when this block is updated
                    tie (lit, rit) = fmd->findCover(curOff, chunkSize);
                    // even iterator (ref) changes, update type should not changes,
                    // since ref. here is always to segment on disk
					if (buflen < chunkSize) {
                        assert(isPartialUpdate);
                    } else if (lit != --rit) {
						assert(isPartialUpdate);
                        assert(len == lit->locLength - (curOff - lit->fileOffset));
                    }
                    chunkId = (lit->segmentOffset+(curOff-lit->fileOffset)) / chunkSize;
                    segmentId = lit->segmentMetaData->segmentId;
                }

                // ??
                if (!skipUpdateBuffer && 
                        lit->segmentMetaData->locations.size() > (uint32_t) chunkId) {
                    diskId = lit->segmentMetaData->locations[chunkId].first;
                }

                // indicate if the device buffer need to be flushed before data writes
                bool flushBeforeWrite = false;

                if (m_updateLogLevel > 0) {
#ifndef STRIPE_PLOG
                    if (m_chunkGroupDataEnd[diskId] == -1) {
                        i = (m_chunkGroupLastFlush[diskId] + 1) % m_updateLogLevel;
                    } else {
                        i = (m_chunkGroupDataEnd[diskId] + 1) % m_updateLogLevel;
                        // check if the target is already occupied
                        if ((m_updateLogbuffers[i].chunkBitmap & (0x1 << diskId)) != 0) {
                        //if (i == (m_chunkGroupLastFlush[diskId] + 1) % m_updateLogLevel) {
                            flushBeforeWrite = true;
                        }
                    }
#else
                    i = m_curUpdateLogLevel;
#endif
                } else { 
                    i = 0;
                    // check if the target is already occupied
#ifndef STRIPE_PLOG
                    if ((m_updateLogbuffers[i].chunkBitmap & (0x1 << diskId)) != 0) 
                        flushBeforeWrite = true;
#else
                    if (m_updateLogbuffers[i].curOff >= m_segmentSize)
                        flushBeforeWrite = true;
#endif
                }

                // write to the buffer slot as indicated
                if (!skipUpdateBuffer && !flushBeforeWrite) {

                    debug("Start at buf %d for disk %d\n", i, diskId);
                    LogBuffer& logbuf = m_updateLogbuffers[i];
                    // lit->segmentMetaData is always referred to a segment on-disk
                    chunkId = (lit->segmentOffset+(curOff-lit->fileOffset)) / chunkSize;
                    diskId = lit->segmentMetaData->locations[chunkId].first;

                    debug("get diskId %d for seg(%d,%d)\n", diskId, segmentId, chunkId);

#ifndef STRIPE_PLOG
                    // if chunk pos is empty and not to the same disk, can write parallel
                    assert((logbuf.chunkBitmap & (0x1 << diskId)) == 0);
#endif

                    // get buf reference and lock
                    SegmentMetaData* lsmd = logbuf.segmentMetaData;
                    SegmentMetaData* dsmd = nullptr;

                    logbuf.logMutex->lock();

                    if (lit->segmentMetaData->locations.empty()) {
                        // segment still inside buffer, copy the reference to the original segment
                        //dsmd = m_updateLogbuffers[level].updateSegments[diskId];
                        assert(0);
                    } else {
                        dsmd = lit->segmentMetaData;
                    }

                    // read chunk (i) from buffer [overwritten??], or (ii) from disk, if it's partial update
                    if (isPartialUpdate) {
                        int dsid = dsmd->segmentId;
                        if (m_segmentIdMap.count(dsid) || m_updateSegmentIdMap.count(dsid)) {
                            readLog(logbuf.buf + diskId * chunkSize, dsid, 
                                    make_pair(diskId * chunkSize, chunkSize));
                        } else {
                            m_raidMod->readSegment(dsmd, logbuf.buf + diskId * chunkSize, 
                                    make_pair(chunkId * chunkSize, chunkSize), ts, false);
                        }
                    }

                    LL segOfs = lit->segmentOffset + (curOff - lit->fileOffset);
#ifdef STRIPE_PLOG
                    diskId = logbuf.curOff / chunkSize;
#endif
                    int logbufOff = diskId * chunkSize + (segOfs % chunkSize);

                    debug("[before] %d map: %x , lookupVector: %lu; level: %d to add: %d\n", 
                            i, logbuf.chunkBitmap, lsmd->lookupVector.size(), level, diskId);
                    assert(len > 0);
                    assert(logbufOff + len <= m_logSegmentSize);
                    memcpy(logbuf.buf + logbufOff, buf + bufCurOff, len);
                    // update chunk bitmap
                    logbuf.chunkBitmap |= (0x1 << diskId);
                    
                    logbuf.updateSegments[diskId] = dsmd;

                    assert(logbuf.updateSegments.size() >= (uint32_t)diskId);
                    assert(logbuf.updateSegments[diskId] != nullptr);
                    logbuf.chunkMap[diskId] = make_pair(chunkId,lsmd->lookupVector.size());

                    LL alignSegOfs = curOff - (segOfs % chunkSize);
                    // update the segment metadata
                    lsmd->lookupVector.push_back(make_pair(diskId * chunkSize,fmd));

                    lsmd->fileOffsetMap[diskId*chunkSize] = alignSegOfs;
                    lsmd->fileSet.insert(fmd);
                    logbuf.logMutex->unlock();

                    // back reference from original seg --> log buffer seg
#ifndef STRIPE_PLOG
                    addUpdateSegment(logbuf.updateSegments[diskId]->segmentId,i);
#else
                    addUpdateSegment(logbuf.updateSegments[diskId]->segmentId,i,diskId);
#endif

                    m_filemetaMod->updateFileLocs(fmd, alignSegOfs, chunkSize, lsmd, diskId*chunkSize, true);
                    // increase the "effective size" of buffer
                    logbuf.curOff += chunkSize;

                    m_chunkGroupDataEnd[diskId] = i;
                    debug("Set Data End disk %d to %d\n", diskId, i);

                    debug("Logging update from %lld to %lld into buf[%d] map:%x seg: %d replace %d %d\n", 
                            alignSegOfs,
                            alignSegOfs+chunkSize, i, logbuf.chunkBitmap, logbuf.segmentMetaData->segmentId,
                            logbuf.updateSegments[diskId]->segmentId, logbuf.chunkMap[diskId].first);

                    // skip the flush due to fullness
                    rounds = 3;
#ifndef STRIPE_PLOG
                } // (!skipUpdateBuffer && !flushBeforeWrite)

                // cannot find a segment candidate, flush the least recent logbuf,
                // or, if this is the last chunk and update buffer size = 0
                if ((!rounds && !skipUpdateBuffer) || 
#else
                if ((m_curUpdateLogLevel == m_updateLogLevel - 1 && 
                        m_updateLogbuffers[m_curUpdateLogLevel].curOff >= m_logSegmentSize) || 
#endif
					flushBeforeWrite || (m_updateLogLevel < 1 && (curOff - fileOffLen.first + chunkSize >= buflen))) {

                    bool success = false;
#ifndef STRIPE_PLOG
                    //printf("flush curOff %llu buflen %d\n", curOff, buflen);
                    if (m_updateLogLevel < 1) {
                        LogBuffer& logbuf = m_updateLogbuffers[0];
                        success = flushToDisk(logbuf, ts, true);
                        updateMetaData(0);
                        memset(logbuf.buf, 0, chunkSize * m_logBlockNum);

                        // assign a new segment metadata for the buffer
                        m_updateSegmentIdMapMutex.lock();
                        m_updateSegmentIdMap.erase(logbuf.segmentMetaData->segmentId);
                        logbuf.segmentMetaData = m_segmetaMod->createEntry(true);
                        m_updateSegmentIdMap[logbuf.segmentMetaData->segmentId] = 0;
                        m_updateSegmentIdMapMutex.unlock();

                    } else {
#endif
                        success = flushUpdateInChunkGroups(-1, fmd, ts);
#ifndef STRIPE_PLOG
                    }
#endif
                    assert(newWriteBuffer || success);

#ifdef STRIPE_PLOG
                    m_curUpdateLogLevel = 0;
                } else if (m_updateLogLevel && 
                        m_updateLogbuffers[m_curUpdateLogLevel].curOff >= m_logSegmentSize) {
                    m_curUpdateLogLevel = (m_curUpdateLogLevel + 1) % m_updateLogLevel;
#endif
                } else {
                    break;
                }
            }

        } // block-wise update

    } else { // new write buffer

        level = m_curLogLevel;
        if (m_logLevel < 1) {
            level = 0; 
        } else { // batch mode
            // TODO : select the level of buffer to append
        }

        LogBuffer& logbuf = m_logbuffers[level];
        // get buf reference and lock
        logbuf.logMutex->lock();

        // first segmentId, first segment saved bytes, second segmentId
        SegmentMetaData *firSmd = nullptr, *secSmd = nullptr;
        soff_len_t firOffLen, secOffLen;

        // flush condition: last log available && overflow
        if (logbuf.curOff + buflen < m_segmentSize) {
            
            if (m_logLevel > 0)  {
                // append new data to log buffer
                memcpy(logbuf.buf + logbuf.curOff, buf, buflen);

            } else {
                LogBuffer& segbuf = m_logbuffers[1];
                int k = 0;
                int bitmap = 0;
                segbuf.logMutex->lock();
                // generate the new partial parity
                memset(segbuf.buf, 0, m_segmentSize);
                memcpy(segbuf.buf + logbuf.curOff, buf, buflen);
                // use coding function exported from raidmod
                vector<Chunk> chunks = m_raidMod->encodeSegment(segbuf.buf, false, k);
                assert(chunks.size() == (uint32_t) k + m_logCodeBlockNum);
                // XOR the changes to parities
                for (int i = k; (uint32_t) i < chunks.size(); i++) {
                    Coding::bitwiseXor(&logbuf.buf[i-k], chunks[i].buf, &logbuf.buf[i-k], chunkSize);
                    // mark parity as dirty (to be written to disk)
                    bitmap |= (0x1 << i);
                }
                memcpy(segbuf.buf + k * chunkSize, logbuf.buf, m_segmentSize - k * chunkSize);
                // mark dirty data (to be written to disk)
                for (int i = logbuf.curOff / chunkSize; i < (logbuf.curOff + buflen) / chunkSize; i++) {
                    bitmap |= (0x1 << i);
                }
                // Write to disk (both data and new parity)
                m_raidMod->writeIncompleteSegment(logbuf.segmentMetaData, segbuf.buf, bitmap, ts);
                 
                for (Chunk chunk : chunks) {
                    free(chunk.buf);
                }
                segbuf.logMutex->unlock();
            }

            // update return value
            firSmd = logbuf.segmentMetaData;
            firOffLen = make_pair(logbuf.curOff, buflen);

            // add fmd to lookup vector
            logbuf.segmentMetaData->lookupVector.push_back(make_pair(logbuf.curOff, fmd));
            logbuf.segmentMetaData->fileOffsetMap[logbuf.curOff] = fileOffLen.first;

            // update log buffer offset 
            logbuf.curOff += buflen;

            // add fmd to segment file set
            logbuf.segmentMetaData->fileSet.insert(fmd);

        } else {
            // may need to flush
            // allocate a new entry of smd
            SegmentMetaData* newEntry;
            if (level == m_logLevel - 1 || m_logLevel < 1) { // flush all buffer
                newEntry = m_segmetaMod->createEntry();
            } else { // move to next batch buffer
                newEntry = m_logbuffers[level+1].segmentMetaData;
                // TODO : update global current append buffer level
            }
            
            // add fmd to both segment metadata
            logbuf.segmentMetaData->fileSet.insert(fmd);
            // add fmd to lookup vector
            logbuf.segmentMetaData->lookupVector.push_back(make_pair(logbuf.curOff, fmd));
            logbuf.segmentMetaData->fileOffsetMap[logbuf.curOff] = fileOffLen.first;
            // add fmd to new entry only if it contains data, to avoid boundary case
            debug("level %d curOff %d + buflen %d\n", level, logbuf.curOff, buflen);
            if (logbuf.curOff + buflen > m_segmentSize) {
                newEntry->fileSet.insert(fmd);
                newEntry->lookupVector.push_back(make_pair(0, fmd));
                newEntry->fileOffsetMap[0] = fileOffLen.first + (m_segmentSize - logbuf.curOff);
            }
            
            if (m_logLevel > 0)  {
                // append new data to log buffer
                memcpy(logbuf.buf + logbuf.curOff, buf, buflen);
            } else {
                // direct write to disk
                LogBuffer& segbuf = m_logbuffers[1];
                int k = 0;
                int bitmap = 0;
                segbuf.logMutex->lock();
                // generate the new partial parity
                memset(segbuf.buf, 0, m_segmentSize);
                memcpy(segbuf.buf + logbuf.curOff, buf, buflen);
                // use coding function exported from raidmod
                vector<Chunk> chunks = m_raidMod->encodeSegment(segbuf.buf, false, k);
                assert(chunks.size() == (uint32_t) k + m_logCodeBlockNum);
                // XOR the changes to parities
                for (int i = k; (uint32_t) i < chunks.size(); i++) {
                    Coding::bitwiseXor(&logbuf.buf[i-k], chunks[i].buf, &logbuf.buf[i-k], chunkSize);
                }
                memcpy(segbuf.buf + k * chunkSize, logbuf.buf, m_segmentSize - k * chunkSize);
                // Write to disk (both data and new parity)
                for (int i = logbuf.curOff / chunkSize; i < (logbuf.curOff + buflen) / chunkSize; i++) {
                    bitmap |= (0x1 << i);
                }
                m_raidMod->writeIncompleteSegment(logbuf.segmentMetaData, segbuf.buf, bitmap, ts);

                for (Chunk chunk : chunks) {
                    free(chunk.buf);
                }
                segbuf.logMutex->unlock();
            }

            // update return value
            firSmd  = logbuf.segmentMetaData;
            firOffLen = make_pair(logbuf.curOff, m_segmentSize - logbuf.curOff);
            debug("buffer level %d curOff %d + buflen %d\n", level, logbuf.curOff, buflen);
            if (logbuf.curOff + buflen > m_segmentSize) {
                secSmd = newEntry;
                secOffLen = make_pair(0, logbuf.curOff + buflen - m_segmentSize);
            } 
            // update logbuffer
            logbuf.curOff += buflen;

            // for buffer-enabled situations 
            if (m_logLevel > 0 && level == m_logLevel - 1) {
                // flush to disk
                // to avoid deadlock: hold all logbuffers, then the segmentIdMap
                //for (int i = 0; i < m_logLevel; i++) {
                //    //debug ("Now flushing level %d segment to disk with id = %d\n", 
                //    //        i, m_logbuffers[i].segmentMetaData->segmentId);
                //    //bool success = flushToDisk(m_logbuffers[i], ts);
                //    //assert(success);
                //    //debug ("Flushed level %d segment to disk with id = %d\n", 
                //    //        i, m_logbuffers[i].segmentMetaData->segmentId);
                //}

                bool success = flushToDiskInBatch(ts);
                assert(success);

                for (int i = 0; i < m_logLevel; i++) {
                    // avoid double lock on current buffer level
                    if (i != level) m_logbuffers[i].logMutex->lock();

                    // update segment map with unique lock
                    m_segmentIdMapMutex.lock();

                    SegmentMetaData* newData = i? m_segmetaMod->createEntry() : newEntry;
                    LogBuffer& curlogbuf = m_logbuffers[i];
                    m_segmentIdMap.erase(curlogbuf.segmentMetaData->segmentId);
                    m_segmentIdMap[newData->segmentId] = i;
                    // update associated segment metadata 
                    curlogbuf.segmentMetaData = newData;

                    m_segmentIdMapMutex.unlock();

                    // update the data-append offset of buffers
                    m_logbuffers[i].curOff -= m_segmentSize;
                    debug("reset level %d off %d\n", i, m_logbuffers[i].curOff);
                    // avoid double unlock on current buffer level
                    if (i != level) m_logbuffers[i].logMutex->unlock();
                }

                //for (int i = 0; i < m_logLevel; i++) {
                //}
            }

            // for buffer-disabled situations
            if (m_logLevel < 1) {
                // update segment map with unique lock
                m_segmentIdMapMutex.lock();
                // only update the segment metadata associated with parity-buffer
                m_segmentIdMap.erase(logbuf.segmentMetaData->segmentId);
                assert(newEntry != nullptr);
                m_segmentIdMap[newEntry->segmentId] = level;
                m_segmentIdMapMutex.unlock();
                // update associated segment metadata 
                logbuf.segmentMetaData = newEntry;
                // update the data-append position of buffers
                logbuf.curOff -= m_segmentSize;
            }

            // for buffer-enabled situations 
            if (m_logLevel > 0) {
                // update data remains in log buffer
                if (level == m_logLevel -1) {
                    if (m_logLevel > 1) { // multiple buffers, and flushed, remains goes to first buffer
                        LogBuffer& nextlogbuf = m_logbuffers[0];
                        nextlogbuf.logMutex->lock();
                        memcpy(nextlogbuf.buf, logbuf.buf + m_segmentSize, logbuf.curOff);
                        nextlogbuf.curOff += logbuf.curOff;
                        logbuf.curOff = 0;
                        m_curLogLevel = 0;
                        nextlogbuf.logMutex->unlock();
                    } else { // only 1 buffer, remains moves to the front of current buffer
                        memmove(logbuf.buf, logbuf.buf + m_segmentSize, logbuf.curOff);
                    }
                } else { // multiple buffers, not flushed, remains goes to next level buffer
                    LogBuffer& nextlogbuf = m_logbuffers[level+1];
                    if (logbuf.curOff > m_segmentSize) {
                        nextlogbuf.logMutex->lock();
                        memcpy(nextlogbuf.buf, logbuf.buf + m_segmentSize, logbuf.curOff - m_segmentSize);
                        nextlogbuf.curOff += logbuf.curOff - m_segmentSize;
                        nextlogbuf.logMutex->unlock();
                        logbuf.curOff = m_segmentSize;
                    }
                    m_curLogLevel = level+1;
                }
            } else { // for buffer-disabled situations

                if (logbuf.curOff > 0) {
                    // write the remaining part of data to disk
                    LogBuffer& segbuf = m_logbuffers[1];
                    int k = 0;
                    int bitmap = 0;
                    segbuf.logMutex->lock();
                    // generate the new partial parity
                    memset(segbuf.buf, 0, m_segmentSize);
                    memmove(segbuf.buf, segbuf.buf + m_segmentSize, logbuf.curOff);
                    // use coding function exported from raidmod
                    vector<Chunk> chunks = m_raidMod->encodeSegment(segbuf.buf, false, k);
                    assert(chunks.size() == (uint32_t) k + m_logCodeBlockNum);
                    // XOR the changes to parities
                    for (int i = k; (uint32_t) i < chunks.size(); i++) {
                        Coding::bitwiseXor(&logbuf.buf[i-k], chunks[i].buf, &logbuf.buf[i-k], chunkSize);
                    }
                    memcpy(segbuf.buf + k * chunkSize, logbuf.buf, m_segmentSize - k * chunkSize);
                    // Write to disk (both data and new parity)
                    for (int i = 0; i < logbuf.curOff / chunkSize; i++) {
                        bitmap |= (0x1 << i);
                    }
                    m_raidMod->writeIncompleteSegment(logbuf.segmentMetaData, segbuf.buf, bitmap, ts);

                    for (Chunk chunk : chunks) {
                        free(chunk.buf);
                    }
                    segbuf.logMutex->unlock();
                }
            }
        }

        if (firSmd != nullptr) {
            debug ("update fileId %d from offset %lld with %d bytes with segment id %d from offset %d fileset size = %zu\n",
                    fmd->fileId, fileOffLen.first, firOffLen.second, firSmd->segmentId, firOffLen.first, firSmd->fileSet.size());
            m_filemetaMod->updateFileLocs(fmd, fileOffLen.first, firOffLen.second, firSmd, firOffLen.first);
        }
        if (secSmd != nullptr) {
            debug ("update fileId %d from offset %lld with %d bytes with segment id %d from offset %d fileset size = %zu\n",
                    fmd->fileId, fileOffLen.first+firOffLen.second, secOffLen.second, secSmd->segmentId, secOffLen.first, secSmd->fileSet.size());
            m_filemetaMod->updateFileLocs(fmd, fileOffLen.first + firOffLen.second, secOffLen.second,
                                          secSmd, secOffLen.first);
        }

        logbuf.logMutex->unlock();
    }


    return true;
}
#else /** BLOCK_ITF **/
bool LogMod::writeLog (char* buf, int level, FileMetaData* fmd, 
        foff_len_t fileOffLen, bool isLastSeg, LL ts, bool isUpdate) {

    int chunkSize = m_logSegmentSize / m_logBlockNum;
    int startSegOff = fileOffLen.first % m_segmentSize;
    int endSegOff = (fileOffLen.first + fileOffLen.second - 1) % m_segmentSize;
    sid_t dsid = fileOffLen.first / m_segmentSize;

    int bufCurOff = 0;
    // for every chunk involved in this segment
    for (chunk_id_t cid = startSegOff / chunkSize; cid <= endSegOff / chunkSize; cid++) {
        int startChunkOff = max(startSegOff - cid * chunkSize, 0);
        int endChunkOff = min(chunkSize - 1, endSegOff - cid * chunkSize);
        int len = endChunkOff - startChunkOff + 1;

        if (isUpdate) {
            SegmentMetaData* dsmd = m_segmetaMod->m_metaMap[dsid];
            assert(dsmd->locations.size() >= (unsigned) (endSegOff + chunkSize -1) / chunkSize);

            int diskId = dsmd->locations[cid].first;

            bool newWriteBuffer = false;
            bool skipUpdateBuffer = false;
            bool isPartialUpdate = false;

            // overwrite content in new write buffer?
            m_segmentIdMapMutex.lock_shared();
            if (m_segmentIdMap.count(dsid) && m_logLevel > 0) {
                LogBuffer &logbuf = m_logbuffers[m_segmentIdMap[dsid]];

                logbuf.logMutex->lock();
                int logbufOff = cid * chunkSize + startChunkOff;
                debug("overwrite new write buffer segment %d from %d [%c]\n", 
                        dsid, logbufOff, buf[bufCurOff]);
                // directly overwrite the new write log buffer
                memcpy(logbuf.buf + logbufOff, buf + bufCurOff, len);
                logbuf.logMutex->unlock();

                skipUpdateBuffer = true;
                newWriteBuffer = true;
            }
            m_segmentIdMapMutex.unlock_shared();

            // overwrite content in update buffer?
            m_updatedSegmentIdMapMutex.lock_shared();
            if (m_updatedSegmentIdMap.count(dsid) && m_updateLogLevel > 0) {
                for (pair<int, chunk_id_t> loc : m_updatedSegmentIdMap[dsid]) {
                    int lv = loc.first;
                    int diskId = loc.second;
                    LogBuffer &logbuf = m_updateLogbuffers[lv];

                    logbuf.logMutex->lock();
                    if (logbuf.updateSegments[diskId]->segmentId == dsid &&
                            logbuf.chunkMap[diskId].first == cid) {
                        assert(logbuf.updateSegments[diskId] != nullptr);
                        int logbufOff = diskId * chunkSize + startChunkOff;
                        memcpy(logbuf.buf + logbufOff, buf + bufCurOff, len);
                        logbuf.logMutex->unlock();
                        skipUpdateBuffer = true;
                        debug("match in update buf dsid %d, chunk %d on %d lv %d\n", dsid, cid, diskId, lv);
                        break;
                    }
                    logbuf.logMutex->unlock();
                }
            }
            m_updatedSegmentIdMapMutex.unlock_shared();

            if (len < chunkSize)
                isPartialUpdate = true;

            // write to update buffer if needed
            int rounds, i;
            for (rounds = 0; rounds < 2; rounds++) {

                if (rounds && skipUpdateBuffer) break;

                bool flushBeforeWrite = false;

                // determine the next available slot 
                if (m_updateLogLevel > 0) {
#ifndef STRIPE_PLOG
                    if (m_chunkGroupDataEnd[diskId] == -1) {
                        i = (m_chunkGroupLastFlush[diskId] + 1) % m_updateLogLevel;
                    } else {
                        i = (m_chunkGroupDataEnd[diskId] + 1) % m_updateLogLevel;
                        // check if the target is already occupied
                        if ((m_updateLogbuffers[i].chunkBitmap & (0x1 << diskId)) != 0) {
                            flushBeforeWrite = true;
                        }
                    }
#else
                    i = m_curUpdateLogLevel;
#endif
                } else { 
                    i = 0;
                    // check if the target is already occupied
#ifndef STRIPE_PLOG
                    if ((m_updateLogbuffers[i].chunkBitmap & (0x1 << diskId)) != 0) 
                        flushBeforeWrite = true;
#else
                    if (m_updateLogbuffers[i].curOff >= m_segmentSize)
                        flushBeforeWrite = true;
#endif
                }

                // write to update buffer
                if (!skipUpdateBuffer && !flushBeforeWrite) {

                    debug("Start at buf %d for disk %d\n", i, diskId);
                    LogBuffer& logbuf = m_updateLogbuffers[i];
                    logbuf.logMutex->lock();
#ifndef STRIPE_PLOG
                    // if chunk pos is empty and not to the same disk, can write parallel
                    assert((logbuf.chunkBitmap & (0x1 << diskId)) == 0);
#endif
                    // read before partial update
                    if (isPartialUpdate) {
                        if (m_segmentIdMap.count(dsid) || m_updateSegmentIdMap.count(dsid)) {
                            unordered_map<chunk_id_t, pair<int, chunk_id_t> > levels;
                            getAliveUpdateSegmentIds(dsid, levels);
                            readLog(logbuf.buf + diskId * chunkSize, dsid, 
                                    make_pair(diskId * chunkSize, chunkSize), levels);
                        } else {
                            m_raidMod->readSegment(dsmd, logbuf.buf + diskId * chunkSize, 
                                    make_pair(cid * chunkSize, chunkSize), ts, false);
                        }
                    }

                    // write starts
#ifdef STRIPE_PLOG
                    diskId = logbuf.curOff / chunkSize;
#endif
                    int logbufOff = diskId * chunkSize + startChunkOff;
                    assert(logbufOff + len <= m_logSegmentSize);
                    memcpy(logbuf.buf + logbufOff, buf + bufCurOff, len);

                    logbuf.chunkMap[diskId].first = cid;
                    logbuf.updateSegments[diskId] = dsmd;
                    logbuf.chunkBitmap |= (0x1 << diskId);
                    logbuf.curOff += chunkSize;
                    logbuf.logMutex->unlock();

                    addUpdateSegment(logbuf.updateSegments[diskId]->segmentId,i,diskId);
                    
                    // indicate this slot is occupied
                    m_chunkGroupDataEnd[diskId] = i;

                    debug("Set Data End disk %d to %d\n", diskId, i);

                    debug("Logging update from %lld to %lld into buf[%d] map:%x seg: %d replace %d %d\n", 
                            (LL)startSegOff, (LL)startSegOff+chunkSize, i, 
                            logbuf.chunkBitmap, logbuf.segmentMetaData->segmentId,
                            dsid, logbuf.chunkMap[diskId].first);

                    // skip the flush due to fullness
                    rounds = 3;

                } // (!skipUpdateBuffer && !flushBeforeWrite)
                
#ifndef STRIPE_PLOG
                // cannot find a segment candidate, flush the least recent logbuf,
                // or, if this is the last chunk and update buffer size = 0
                if ((!rounds && !skipUpdateBuffer) || 
#else
                if ((m_curUpdateLogLevel == m_updateLogLevel - 1 && 
                        m_updateLogbuffers[m_curUpdateLogLevel].curOff >= m_logSegmentSize) || 
#endif
					flushBeforeWrite || (m_updateLogLevel < 1 && (isLastSeg && cid+1 > endSegOff / chunkSize))) {

                    bool success = false;
                    //printf("flush isLast=%d, flushB4Write=%d, round=%d\n", isLastSeg, flushBeforeWrite, rounds);
#ifndef STRIPE_PLOG
                    if (m_updateLogLevel < 1) {
                        LogBuffer& logbuf = m_updateLogbuffers[0];
                        success = flushToDisk(logbuf, ts, true);
                        updateMetaData(0);
                        memset(logbuf.buf, 0, chunkSize * m_logBlockNum);

                        // assign a new segment metadata for the buffer
                        m_updateSegmentIdMapMutex.lock();
                        m_updateSegmentIdMap.erase(logbuf.segmentMetaData->segmentId);
                        logbuf.segmentMetaData = m_segmetaMod->createEntry(true);
                        m_updateSegmentIdMap[logbuf.segmentMetaData->segmentId] = 0;
                        m_updateSegmentIdMapMutex.unlock();

                    } else {
#endif
                        success = flushUpdateInChunkGroups(-1, fmd, ts);
#ifndef STRIPE_PLOG
                    }
#endif
                    assert(newWriteBuffer || success);

#ifdef STRIPE_PLOG
                    m_curUpdateLogLevel = 0;
                } else if (m_updateLogLevel && 
                        m_updateLogbuffers[m_curUpdateLogLevel].curOff >= m_logSegmentSize) {
                    m_curUpdateLogLevel = (m_curUpdateLogLevel + 1) % m_updateLogLevel;
#endif
                } else {
                    break;
                }
            } // block-wise update
        } else {
            // new writes
            level = m_curLogLevel;
            if (m_logLevel < 1) {
                level = 0; 
            } else { // batch mode
                // TODO : select the level of buffer to append
            }

            LogBuffer& logbuf = m_logbuffers[level];
            // get buf reference and lock
            logbuf.logMutex->lock();

            sid_t dsid  = fileOffLen.first / m_segmentSize;
            if (logbuf.segmentMetaData == nullptr) {
                bool exists = m_segmetaMod->m_metaMap.count(dsid);
                if (exists) {
                    logbuf.segmentMetaData = m_segmetaMod->m_metaMap[dsid];
                } else {
                    logbuf.segmentMetaData = m_segmetaMod->createEntry(false, dsid);
                }
                m_segmentIdMap[dsid] = level;
            }

            // first segmentId, first segment saved bytes, second segmentId
            soff_len_t firOffLen, secOffLen;
            if (m_logLevel > 0)  {
                // append new data to log buffer
                memcpy(logbuf.buf + logbuf.curOff, buf, len);
                if (logbuf.segmentMetaData->segmentId != dsid) printf("%d ; %d off %llu dsid %d\n", logbuf.segmentMetaData->segmentId, dsid, fileOffLen.first, dsid);
                assert(logbuf.segmentMetaData->segmentId == dsid);

            } else {
                // sync the incomplete segment to disk 
                LogBuffer& segbuf = m_logbuffers[1];
                int k = 0;
                int bitmap = 0;
                segbuf.logMutex->lock();
                // generate the new partial parity
                memset(segbuf.buf, 0, m_segmentSize);
                memcpy(segbuf.buf + logbuf.curOff, buf + bufCurOff, len);
                // use coding function exported from raidmod
                vector<Chunk> chunks = m_raidMod->encodeSegment(segbuf.buf, false, k);
                assert(chunks.size() == (uint32_t) k + m_logCodeBlockNum);
                // XOR the changes to parities
                for (int i = k; (uint32_t) i < chunks.size(); i++) {
                    Coding::bitwiseXor(&logbuf.buf[i-k], chunks[i].buf, &logbuf.buf[i-k], chunkSize);
                    // mark parity as dirty (to be written to disk)
                    bitmap |= (0x1 << i);
                }
                memcpy(segbuf.buf + k * chunkSize, logbuf.buf, m_segmentSize - k * chunkSize);
                // mark dirty data (to be written to disk)
                for (int i = logbuf.curOff / chunkSize; i < (logbuf.curOff + len) / chunkSize; i++) {
                    bitmap |= (0x1 << i);
                }
                // Write to disk (both data and new parity)
                m_raidMod->writeIncompleteSegment(logbuf.segmentMetaData, segbuf.buf, bitmap, ts);
                 
                for (Chunk chunk : chunks) {
                    free(chunk.buf);
                }
                segbuf.logMutex->unlock();
            }

            logbuf.curOff += len;

            if (logbuf.curOff < m_segmentSize) {
                logbuf.logMutex->unlock();
                bufCurOff += len;
                continue; 
            }

            // allocate a new entry of smd
            //SegmentMetaData* newEntry;
            if (level == m_logLevel - 1 || m_logLevel < 1) { // flush all buffer
                //newEntry = m_segmetaMod->createEntry();
            } else { // move to next batch buffer
                //newEntry = m_logbuffers[level+1].segmentMetaData;
                // TODO : update global current append buffer level
            }

            if (m_logLevel > 0) {
                // update data remains in log buffer
                if (level == m_logLevel -1) {

                    // overflow in current buffer level
                    bool success = flushToDiskInBatch(ts);
                    assert(success);
                    for (int i = 0; i < m_logLevel; i++) {
                        // avoid double lock on current buffer level
                        if (i != level) m_logbuffers[i].logMutex->lock();

                        // update segment map with unique lock
                        m_segmentIdMapMutex.lock();

                        //SegmentMetaData* newData = i? m_segmetaMod->createEntry() : newEntry;
                        LogBuffer& curlogbuf = m_logbuffers[i];
                        m_segmentIdMap.erase(curlogbuf.segmentMetaData->segmentId);
                        if (i == 0 && logbuf.curOff > m_segmentSize) {
                            // update associated segment metadata 
                            SegmentMetaData * newSeg = m_segmetaMod->createEntry();
                            curlogbuf.segmentMetaData = newSeg;
                            m_segmentIdMap[newSeg->segmentId] = i;
                        } else {
                            curlogbuf.segmentMetaData = nullptr;
                        }

                        m_segmentIdMapMutex.unlock();

                        // update the data-append offset of buffers
                        m_logbuffers[i].curOff -= m_segmentSize;
                        debug("reset level %d off %d\n", i, m_logbuffers[i].curOff);
                        // avoid double unlock on current buffer level
                        if (i != level) m_logbuffers[i].logMutex->unlock();
                    }
                    if (logbuf.curOff > 0) {
                        if (m_logLevel > 1) { // multiple buffers, and flushed, remains goes to first buffer
                            LogBuffer& nextlogbuf = m_logbuffers[0];
                            nextlogbuf.logMutex->lock();
                            memcpy(nextlogbuf.buf, logbuf.buf + m_segmentSize, logbuf.curOff);
                            nextlogbuf.curOff += logbuf.curOff;
                            logbuf.curOff = 0;
                            nextlogbuf.logMutex->unlock();
                        } else { // only 1 buffer, remains moves to the front of current buffer
                            memmove(logbuf.buf, logbuf.buf + m_segmentSize, logbuf.curOff);
                        }
                    }
                    m_curLogLevel = 0;
                } else { // multiple buffers, not flushed, remains goes to next level buffer
                    LogBuffer& nextlogbuf = m_logbuffers[level+1];

                    if (logbuf.curOff > m_segmentSize) {
                        SegmentMetaData * newSeg = m_segmetaMod->createEntry(false,dsid+1);
                        nextlogbuf.segmentMetaData = newSeg;
                        m_segmentIdMap[newSeg->segmentId] = level + 1;
                        nextlogbuf.logMutex->lock();
                        memcpy(nextlogbuf.buf, logbuf.buf + m_segmentSize, logbuf.curOff - m_segmentSize);
                        nextlogbuf.curOff += logbuf.curOff - m_segmentSize;
                        nextlogbuf.logMutex->unlock();
                        logbuf.curOff = m_segmentSize;
                    }
                    m_curLogLevel = level+1;
                }
            } else {
                // update segment map with unique lock
                m_segmentIdMapMutex.lock();
                // only update the segment metadata associated with parity-buffer
                m_segmentIdMap.erase(logbuf.segmentMetaData->segmentId);
                //assert(newEntry != nullptr);
                //m_segmentIdMap[newEntry->segmentId] = level;
                m_segmentIdMapMutex.unlock();

                // update associated segment metadata 
                //logbuf.segmentMetaData = newEntry;
                logbuf.segmentMetaData = nullptr;
                // update the data-append position of buffers
                logbuf.curOff -= m_segmentSize;

                if (logbuf.curOff > 0) {
                    // write the remaining part of data to disk
                    LogBuffer& segbuf = m_logbuffers[1];
                    int k = 0;
                    int bitmap = 0;
                    segbuf.logMutex->lock();
                    // generate the new partial parity
                    memset(segbuf.buf, 0, m_segmentSize);
                    memmove(segbuf.buf, segbuf.buf + m_segmentSize, logbuf.curOff);
                    // use coding function exported from raidmod
                    vector<Chunk> chunks = m_raidMod->encodeSegment(segbuf.buf, false, k);
                    assert(chunks.size() == (uint32_t) k + m_logCodeBlockNum);
                    // XOR the changes to parities
                    for (int i = k; (uint32_t) i < chunks.size(); i++) {
                        Coding::bitwiseXor(&logbuf.buf[i-k], chunks[i].buf, &logbuf.buf[i-k], chunkSize);
                    }
                    memcpy(segbuf.buf + k * chunkSize, logbuf.buf, m_segmentSize - k * chunkSize);
                    // Write to disk (both data and new parity)
                    for (int i = 0; i < logbuf.curOff / chunkSize; i++) {
                        bitmap |= (0x1 << i);
                    }
                    m_raidMod->writeIncompleteSegment(logbuf.segmentMetaData, segbuf.buf, bitmap, ts);

                    for (Chunk chunk : chunks) {
                        free(chunk.buf);
                    }
                    segbuf.logMutex->unlock();
                }
            }

            logbuf.logMutex->unlock();
        }

        // update offset to be processed
        bufCurOff += len;
    }

    return true;
}
#endif  /** BLOCK_ITF **/

#ifndef BLOCK_ITF
bool LogMod::readLog (char* buf, sid_t segmentId, soff_len_t segOffLen) 
#else
bool LogMod::readLog (char* buf, sid_t segmentId, soff_len_t segOffLen, 
        unordered_map<chunk_id_t, pair<int, chunk_id_t> > levels)
#endif
{
    m_segmentIdMapMutex.lock_shared();
#ifndef BLOCK_ITF
    m_updateSegmentIdMapMutex.lock_shared();
#else
    m_updatedSegmentIdMapMutex.lock_shared();
#endif

    bool isUpdate = false;
        
#ifndef BLOCK_ITF
    if (m_segmentIdMap.count(segmentId) == 0 && m_updateSegmentIdMap.count(segmentId) == 0) {
        m_updateSegmentIdMapMutex.unlock_shared();
#else

    assert(m_segmetaMod->m_metaMap.count(segmentId));
    int chunkSize = m_logSegmentSize / m_logBlockNum;
    chunk_id_t cid = segOffLen.first / chunkSize;
    assert((segOffLen.first + segOffLen.second-1) / chunkSize == cid);

    if (m_segmentIdMap.count(segmentId) == 0 && levels.count(cid) < 1) {
        m_updatedSegmentIdMapMutex.unlock_shared();
#endif
        m_segmentIdMapMutex.unlock_shared();
        debug ("SegmentId %d is not in NVRAM\n", segmentId);
        return false;
    }

    isUpdate = (m_segmentIdMap.count(segmentId))? false : true;

    LogBuffer *logbuf = nullptr;
    if (isUpdate) {
#ifndef BLOCK_ITF
        assert(m_updateSegmentIdMap[segmentId] >= 0 
                && m_updateSegmentIdMap[segmentId] < m_updateLogLevel);
        logbuf = &m_updateLogbuffers[m_updateSegmentIdMap[segmentId]];
#else
        logbuf = &m_updateLogbuffers[levels[cid].first];
        assert(logbuf->updateSegments[levels[cid].second] && 
                logbuf->updateSegments[levels[cid].second]->segmentId == segmentId);
#endif
    } else {
        assert(m_segmentIdMap[segmentId] >= 0 && m_segmentIdMap[segmentId] < m_logLevel);
        logbuf = &m_logbuffers[m_segmentIdMap[segmentId]];
    }

    debug("read segment %d from %d to %d [%s] %d\n", segmentId, segOffLen.first, 
            segOffLen.first+segOffLen.second, isUpdate?"UP":"NW",  
            isUpdate?m_updateSegmentIdMap[segmentId]:m_segmentIdMap[segmentId]);

#ifndef BLOCK_ITF
    m_updateSegmentIdMapMutex.unlock_shared();
#else
    m_updatedSegmentIdMapMutex.unlock_shared();
#endif
    m_segmentIdMapMutex.unlock_shared();

    // segment in the nvram and start lock
    logbuf->logMutex->lock_shared();
    // double check, the segment maybe flushed already
#ifndef BLOCK_ITF
    if (logbuf->segmentMetaData->segmentId != segmentId) {
#else
    if ((isUpdate && logbuf->updateSegments[levels[cid].second]->segmentId != segmentId) ||
            (!isUpdate && logbuf->segmentMetaData->segmentId != segmentId)) {
        if (isUpdate) 
            debug ("SegmentId %d is already flushed to disk, buf is now %d\n", 
                    segmentId, logbuf->updateSegments[levels[cid].second]->segmentId);
        else
#endif
        debug ("SegmentId %d is already flushed to disk, buf is now %d\n", 
                segmentId, logbuf->segmentMetaData->segmentId);
        logbuf->logMutex->unlock_shared();
        return false;
    }

    if (isUpdate) {
        // check bitmap (usage) for position-dependent buffer
#ifndef BLOCK_ITF
        int chunkSize = m_logSegmentSize / m_logBlockNum;
        disk_id_t diskId = segOffLen.first / chunkSize;
#else
        disk_id_t diskId = levels[cid].second;
#endif
        assert((logbuf->chunkBitmap & (0x1 << diskId)) != 0);
        memcpy(buf, logbuf->buf + diskId * chunkSize + segOffLen.first % chunkSize, segOffLen.second);
    } else {
        // check effective log length for append-only buffer
        assert(logbuf->curOff >= segOffLen.first + segOffLen.second);
        memcpy(buf, logbuf->buf + segOffLen.first, segOffLen.second);
    }

    logbuf->logMutex->unlock_shared();

    return true;
}
/* return no. of ids inserted */
int LogMod::getAliveSegmentIds(unordered_set<sid_t>& ids)
{
    m_segmentIdMapMutex.lock_shared();
    for (auto& entry: m_segmentIdMap) {
        ids.insert(entry.first);
    }
    m_segmentIdMapMutex.unlock_shared();
    return m_segmentIdMap.size();
}

#ifndef BLOCK_ITF
int LogMod::getAliveUpdateSegmentIds(unordered_set<sid_t>& ids)
{
    m_updateSegmentIdMapMutex.lock_shared();
    for (auto& entry: m_updateSegmentIdMap) {
        ids.insert(entry.first);
    }
    m_updateSegmentIdMapMutex.unlock_shared();
    return m_updateSegmentIdMap.size();
}
#else /* ifndef BLOCK_ITF */
int LogMod::getAliveUpdateSegmentIds(sid_t sid, unordered_map<chunk_id_t, pair<int, chunk_id_t> >& levels) {
    set<pair<int, chunk_id_t> > sid_levels;
    levels.clear();
    m_updatedSegmentIdMapMutex.lock_shared();
    if (m_updatedSegmentIdMap.count(sid) > 0) {
        sid_levels = m_updatedSegmentIdMap[sid];
        // chunkId, <buf lv, diskId>
        for (pair<int, chunk_id_t> lv : sid_levels) {
            chunk_id_t cid = m_updateLogbuffers[lv.first].chunkMap[lv.second].first;
            levels[cid] = lv;
        }
    }
    m_updatedSegmentIdMapMutex.unlock_shared();
    return levels.size();
}
#endif

void LogMod::updateMetaData(int level) {

#ifndef BLOCK_ITF
    int chunkSize = m_logSegmentSize / m_logBlockNum;
#endif

    LogBuffer &logbuf = m_updateLogbuffers[level];
    unordered_set<sid_t> logBlockSegMetaMap;
    SegmentMetaData* lsmd = logbuf.segmentMetaData;
    assert(lsmd != NULL);
    sid_t lsid = lsmd->segmentId;
    //logBlockSegMeta.segmentId = logbuf.segmentMetaData->segmentId;

    // update the metadata of updated segment
    for (int i = 0; i < m_logBlockNum; i++) {
        // check if the block contains data alive
        if ((logbuf.chunkBitmap & (0x1 << i)) == 0) continue;

        SegmentMetaData* dsmd = logbuf.updateSegments[i];
#ifndef BLOCK_ITF
        LL fileOff; int segOff; FileMetaData* curFmd;
        tie(segOff, curFmd) = lsmd->lookupVector[logbuf.chunkMap[i].second];
		fileOff = lsmd->fileOffsetMap[segOff];
#endif /* ifndef BLOCK_ITF */
        int chunkId = logbuf.chunkMap[i].first;
        debug("segment %d on disk %d is chunk %d\n", lsmd->segmentId, i, chunkId);

        // same disk, store old lba
        // TODO : lock the segment before modification
        // mark current data block as log
        dsmd->logLocations[chunkId].push_back( make_pair(
                dsmd->locations[chunkId].second,dsmd->curLogId[chunkId]));
        // update new data block lba
        dsmd->locations[chunkId].second = lsmd->locations[i].second;
        // mark the log segment associated with the new data block
        dsmd->curLogId[chunkId] = lsid;

        // revert mapping from log segment to data segment + chunk
        lsmd->logLocations[i].push_back(make_pair(dsmd->segmentId, chunkId));

#ifndef BLOCK_ITF
        bool upToDate = true;
        // check on-disk segment
        int pageSize = ConfigMod::getInstance().getPageSize();
        upToDate = dsmd->bitmap.getBit(chunkId * chunkSize / pageSize);

        // only update reference if this chunk is the lastest
        if (upToDate) 
            m_filemetaMod->updateFileLocs(curFmd, fileOff, chunkSize, dsmd, chunkId*chunkSize, true);
#endif /* ifndef BLOCK_ITF */

        // update the back reference
#if defined(STRIPE_PLOG) || defined(BLOCK_ITF)
        removeUpdateSegment(logbuf.updateSegments[i]->segmentId, level, i);
#else
        removeUpdateSegment(logbuf.updateSegments[i]->segmentId, level);
#endif
        
#ifndef BLOCK_ITF
        debug("segment %d logLocation[%d]: %lu; on disk %d lba from %d to %d; fileOff: %lld\n", 
                dsmd->segmentId, chunkId, dsmd->logLocations[chunkId].size(), dsmd->locations[chunkId].first,
                dsmd->logLocations[chunkId].back().first, dsmd->locations[chunkId].second, fileOff);
#else
        debug("segment %d logLocation[%d]: %lu; on disk %d lba from %d to %d", 
                dsmd->segmentId, chunkId, dsmd->logLocations[chunkId].size(), dsmd->locations[chunkId].first,
                dsmd->logLocations[chunkId].back().first, dsmd->locations[chunkId].second);
#endif /* ifndef BLOCK_ITF */
    }

    // insert entry for encoded log blocks (for sync)
    if (m_syncMod != nullptr && lsmd->locations.size() > (uint32_t) m_logBlockNum) {
        lba_t startLBA = lsmd->locations[m_logBlockNum].second;
        m_syncMod->updateLogDiskMap(startLBA, lsid);
        //m_syncMod->printLogDiskMap();
    }

	// no need to sort if no longer need..
    //sort(logbuf.segmentMetaData->lookupVector.begin(), logbuf.segmentMetaData->lookupVector.end());

    // reset the log buffer flushed
    logbuf.chunkBitmap = 0;
    logbuf.curOff = 0;
}

bool LogMod::flushToDisk(LogBuffer& logbuf, LL ts, bool isUpdate)
{
    if (logbuf.curOff < m_segmentSize && !isUpdate) {
        debug("No need to flush buffer, size %d is not full\n", logbuf.curOff);
        return false;
    }
    if (isUpdate) {
        //m_raidMod->writeLogSegment(logbuf.segmentMetaData, logbuf.buf, 
        //        logbuf.chunkBitmap, ts);

        // mark dirty data segments
        if (m_perMetaMod) { 
            for (chunk_id_t i = 0; i < m_logBlockNum; i++) {
                if ((logbuf.chunkBitmap & (0x1 << i)) == 0) continue;
                int dsid = logbuf.updateSegments[i]->segmentId;
                m_perMetaMod->addDirtySeg(dsid);
            }
        }

        // write the log segment
        m_raidMod->writeLogSegment(logbuf.segmentMetaData, logbuf.buf, 
                logbuf.chunkBitmap, ts);
    } else {
        // write new segment
        m_raidMod->writeSegment(logbuf.segmentMetaData, logbuf.buf, ts);
    }
    return true;
}

// ONLY support new writes
bool LogMod::flushToDiskInBatch(LL ts) {
    //assert(m_logLevel > 0);
    if (m_logLevel <= 0) {
        return true;
    }
    vector<SegmentMetaData*> logSegV;
    int totalLv = 0;
    for (int i = 0; i < m_logLevel; i++) {
        if (m_logbuffers[i].curOff <= 0) break;
        logSegV.push_back(m_logbuffers[i].segmentMetaData);
        totalLv++;
    }

    if (logSegV.empty()) return true;

    m_raidMod->writeBatchedSegments(totalLv, logSegV, m_logMapping, ts);
    return true;
}

int LogMod::flushUpdateToDisk(int numLog, LL ts) {
    int logFlushed = 0;
    const int chunkSize = m_logSegmentSize / m_logBlockNum;

    if (numLog > m_updateLogLevel || numLog < 0) {
        numLog = m_updateLogLevel;
    } else if (m_updateLogLevel < 1) {
        numLog = 1;
    }

    for (int i = 0; i < numLog; i++) {
        
        if (m_updateLogLevel > 0) {
            m_lastFlushLog = (m_lastFlushLog+1) % m_updateLogLevel;
        } else {
            m_lastFlushLog = 0;
        }

        LogBuffer &logbuf = m_updateLogbuffers[m_lastFlushLog];
        if (logbuf.chunkBitmap != 0) {
            logbuf.logMutex->lock();
            if (flushToDisk(logbuf, ts, true)) {
                // update log stripe and data stripe metadata
                updateMetaData(m_lastFlushLog);

                memset(logbuf.buf, 0, chunkSize * m_logBlockNum);

                // assign a new segment metadata for the buffer
                m_updateSegmentIdMapMutex.lock();
                m_updateSegmentIdMap.erase(logbuf.segmentMetaData->segmentId);
                logbuf.segmentMetaData = m_segmetaMod->createEntry(true);
                m_updateSegmentIdMap[logbuf.segmentMetaData->segmentId] = m_lastFlushLog;
                m_updateSegmentIdMapMutex.unlock();

                logFlushed++;
            }
            logbuf.logMutex->unlock();
        }
    }

    return logFlushed;
}

bool LogMod::flushUpdateInChunkGroups(bool forced) {
    if (forced) {
        // force all update out of the update buffer
        bool ret = flushUpdateInChunkGroups();
        while (ret && flushUpdateInChunkGroups());
        if (!ret) { ret = flushUpdateInChunkGroups(-1, NULL, 0, true); }
        while (ret && flushUpdateInChunkGroups(-1, NULL, 0, true));
        return ret;
    } else {
        return flushUpdateInChunkGroups();
    }
}

#ifndef STRIPE_PLOG
// return true if at least one log segment is flushed
bool LogMod::flushUpdateInChunkGroups(int level, FileMetaData* curfmd, LL ts, bool forced) {
    unordered_map<disk_id_t, pair<int, int> > disksToFlush;     // diskId, (start, end)
    set<int> levelsLocked;                      // buffer levels
    unordered_map<int, set<disk_id_t> > levelDiskMap;           // level, set(diskId)

    if (m_updateLogLevel <= 0) { return false; }
    assert(m_updateLogLevel >= m_chunkGroupSize);
    const int chunkGroupSize = (!forced)? m_chunkGroupSize : 1;
    const int chunkSize = m_logSegmentSize / m_logBlockNum;
#ifndef BLOCK_ITF
    const int pageSize = ConfigMod::getInstance().getPageSize();
#endif /* BLOCK_ITF */

    int chunkBitmap = 0;
    unordered_set<int> logBlockSegMetaMap;    // syncMod, logdisk->dataSegment

    for (int i = 0; i < m_logBlockNum; i++) {
        int start = m_chunkGroupLastFlush[i];
        int end = m_chunkGroupDataEnd[i];   // inclusive
        if ((!m_updateLogLevel ||
                (end - start + m_updateLogLevel) % m_updateLogLevel >= chunkGroupSize || 
                (end - start) % m_updateLogLevel == 0)
                && end != -1) {

            if (m_updateLogLevel) {
                start = (m_chunkGroupLastFlush[i] + 1) % m_updateLogLevel;
            } else {
                start = 0;
            }
            // flush 1 chunk group at a time
            int endLevel = 1;
            if (m_updateLogLevel) endLevel = (start+chunkGroupSize) % m_updateLogLevel;
            assert(!m_updateLogLevel || (end - endLevel + m_updateLogLevel + 1) % 
                    m_updateLogLevel >= 0);
            debug("group flush test on disk %d start %d end %d\n", i, start, endLevel);
            disksToFlush[i] = make_pair(start, endLevel);  // exclusive

            // find the log buffer levels to lock
            do {
                levelsLocked.insert(start);
                start = (m_updateLogLevel)? (start + 1) % m_updateLogLevel : start + 1;
            } while (start != endLevel);

            // add the disk to bit map
            chunkBitmap |= (0x1 << i);
        }
    }

    // none of the disks accumulates enough chunks for flush
    if (disksToFlush.empty()) { return false; }

    // gain the locks of log buffers
    for (int lv : levelsLocked) {
        if (lv == level) { continue; }
        debug("group flush lock buf[%d] vs %d\n", lv, level);
        m_updateLogbuffers[lv].logMutex->lock_shared();
        m_updateLogbuffers[lv].logMutex->lock_upgrade();
    }

    // operate on the buffer
    for (int i = 0; i < chunkGroupSize; i++) {
        int cg = i;
        char** cglogbuf = m_chunkGroupLogBuffer;
        int start = 0, end = 0;
        for (int j = 0; j < m_logBlockNum; j++) {
            disk_id_t diskId = j;

            // disk w/o grouped data to flush 
            if (!disksToFlush.count(diskId)) {
                // init segment meta
                //assert(cglsmd->locations.size() > (uint32_t) diskId);
                //cglsmd->locations.push_back(make_pair(-1, vector<int>()) );
                memset(cglogbuf[diskId] + cg * chunkSize, 0, chunkSize);
                continue;
            }

            // disk w/ data to flush
            tie(start, end) = disksToFlush[diskId];
            int bufLv = (m_updateLogLevel)?
                    (start + cg + m_updateLogLevel) % m_updateLogLevel: 
                    (start + cg + m_updateLogLevel);
            LogBuffer& logbuf = m_updateLogbuffers[bufLv];

            // copy the segment metadata
            //assert(cglsmd->locations.size() > (uint32_t) diskId);
            //cglsmd->locations.push_back(make_pair(diskId, vector<int>()) );

            debug("group flush disk %d from start %d to %d-1, bitmap [%x]\n", 
                    diskId, start, end, logbuf.chunkBitmap);
            // reference to the  the data in buffer
            assert((logbuf.chunkBitmap & (0x1 << diskId)) != 0);
            //cglogbuf[j][i] = logbuf.buf + j * chunkSize;
            memcpy(cglogbuf[diskId] + cg * chunkSize, logbuf.buf + diskId * chunkSize, 
                    chunkSize);

            // record chunkGroup level-based 
            levelDiskMap[bufLv].insert(diskId);
        }
    }

    debug("group flush WriteBatchSegment in size %d, bitmap %x\n", 
            chunkGroupSize, chunkBitmap);

    // flush the data in parallel
    m_raidMod->writeBatchedLogSegments(chunkGroupSize, m_chunkGroupLogSegMeta,
            m_chunkGroupLogBuffer, chunkBitmap, ts);

    // downgrade the locks of log buffers
    for (int lv : levelsLocked) {
        if (lv == level) { continue; }
        debug("group flush lock buf[%d] vs %d\n", lv, level);
        m_updateLogbuffers[lv].logMutex->unlock_upgrade();
    }

    // update metadata reference from files
    // clean up metadata and data in update log buffers as well
    for (int lv : levelsLocked) {
        debug("group flush update buf[%d] %lu\n", lv, levelDiskMap.size());
        assert(levelDiskMap.count(lv));
        set<disk_id_t>& disks = levelDiskMap[lv];
        LogBuffer& logbuf = m_updateLogbuffers[lv];
#ifndef BLOCK_ITF
        SegmentMetaData* lsmd = logbuf.segmentMetaData;
        map<int, LL>::iterator fileOff; int segOff; FileMetaData* fmd;
#endif /* ifndef BLOCK_ITF */

        for (disk_id_t diskId : disks) {
            debug("group flush update meta in buf[%d], for disk %d\n", 
                    lv, diskId);
            SegmentMetaData* dsmd = logbuf.updateSegments[diskId];
            chunk_id_t& chunkId = logbuf.chunkMap[diskId].first;
            int startLv = disksToFlush[diskId].first;
            // buffer used by other disks, but not this disk
            int curLv = (m_updateLogLevel)? 
                    (lv - startLv + m_updateLogLevel) % m_updateLogLevel:
                    (lv - startLv + m_updateLogLevel);
            assert(curLv >= 0 && curLv < chunkGroupSize);
            SegmentMetaData* cglsmd = m_chunkGroupLogSegMeta[curLv];
            int cglsid = cglsmd->segmentId;

            // update the data segment LBA reference
            dsmd->logLocations[chunkId].push_back( make_pair(
                    dsmd->locations[chunkId].second,dsmd->curLogId[chunkId]));
            dsmd->locations[chunkId].second = cglsmd->locations[diskId].second;
            assert(dsmd->locations[chunkId].second != INVALID_LBA);
            dsmd->curLogId[chunkId] = cglsid;

            // log disk -> data segment mapping
            cglsmd->logLocations[diskId].push_back(make_pair(dsmd->segmentId, chunkId));
            //logBlockSegMetaMap.insert(cglsid);

            if (m_perMetaMod) { m_perMetaMod->addDirtySeg(dsmd->segmentId); }

            // update file reference back to data segment
#ifndef BLOCK_ITF
            tie(segOff, fmd) = lsmd->lookupVector[logbuf.chunkMap[diskId].second];
            fileOff = lsmd->fileOffsetMap.find(segOff);
            // always lock file before logbuffer, avoid deadlock with concurrent read 
            if (fmd != curfmd) {
                fmd->fileMutex.lock(); 
            }
#endif /* ifndef BLOCK_ITF */
            logbuf.logMutex->lock_upgrade();
#ifndef BLOCK_ITF
            bool upToDate = dsmd->bitmap.getBit(chunkId * chunkSize / pageSize);
            if (upToDate) {
                m_filemetaMod->updateFileLocs(fmd, fileOff->second, chunkSize, dsmd, 
                        chunkId * chunkSize, true);
            }
            // clear the reference
            lsmd->fileOffsetMap.erase(fileOff);
            lsmd->lookupVector[logbuf.chunkMap[diskId].second] = make_pair(-1, nullptr);
#endif /* ifndef BLOCK_ITF */
            logbuf.chunkMap[diskId] = make_pair(INVALID_CHUNK, INVALID_CHUNK);;
#ifdef BLOCK_ITF
            // remove reference from data segment to update buffer level
            removeUpdateSegment(dsmd->segmentId, lv, diskId);
#endif
            logbuf.updateSegments[diskId] = nullptr;

            // clean up the update buffer slot
            assert((logbuf.chunkBitmap & (0x1 << diskId)) != 0);
            logbuf.chunkBitmap ^= (0x1 << diskId);
            logbuf.curOff -= chunkSize;
            memset(logbuf.buf + diskId * chunkSize, 0, chunkSize);
            logbuf.logMutex->unlock_upgrade();
#ifndef BLOCK_ITF
            if (fmd != curfmd) { fmd->fileMutex.unlock(); }
#endif /* ifndef BLOCK_ITF */
        }
    }

    // reset last flushed level
    for (auto& entry : disksToFlush) {
        int endLv = entry.second.second;
        debug("Reset disk %d last flush from %d to %d\n", 
               entry.first,  m_chunkGroupLastFlush[entry.first], endLv-1);

        // 'endLv' is exclusive, so, move back by 1 buffer level
        int nLastFlush = (m_updateLogLevel)?
                (endLv - 1 + m_updateLogLevel) % m_updateLogLevel:
                (endLv - 1 + m_updateLogLevel);
        m_chunkGroupLastFlush[entry.first] = nLastFlush;

        // the last data chunk in buffer is also flushed
        if (m_chunkGroupDataEnd[entry.first] == nLastFlush) {
            m_chunkGroupDataEnd[entry.first] = -1;
        }
    }
    
    if (m_syncMod != nullptr) {
        //for (auto& entry : logBlockSegMetaMap) {
        for (int i = 0; i < chunkGroupSize; i++) {
            int entry = m_chunkGroupLogSegMeta[i]->segmentId;
            SegmentMetaData* lsmd = m_segmetaMod->m_metaMap[entry];
            assert(lsmd->locations[m_logBlockNum].second != INVALID_LBA);
            lba_t startLBA = lsmd->locations[m_logBlockNum].second;
            m_syncMod->updateLogDiskMap(startLBA, entry);
        }
    }
    
    // refresh segment meta of chunkGroupBuffer
    int pin = 0;
    // move the unused segment id forward
    for (; pin < m_chunkGroupSize - chunkGroupSize; pin++) {
        m_chunkGroupLogSegMeta[pin] = m_chunkGroupLogSegMeta[chunkGroupSize+pin];
    }
    // new segment id for future segments
    for (; pin < m_chunkGroupSize; pin++) {
        m_chunkGroupLogSegMeta[pin] = m_segmetaMod->createEntry(true);
    }

    // release the locks
    for (int lv : levelsLocked) {
        if (lv == level) { continue; }
        m_updateLogbuffers[lv].logMutex->unlock_shared();
    }

    return true;
}
#else
bool LogMod::flushUpdateInChunkGroups(int level, FileMetaData* curfmd, LL ts, bool forced) {
    int chunkSize = m_logSegmentSize / m_logBlockNum;
    int n = m_logBlockNum;
    int m = m_logCodeBlockNum;
    int k = m_segmentSize / chunkSize;

    if (m_updatedSegmentIdMap.empty()) return false;

    char* obuf = (char*) buf_malloc (sizeof(char) * m_segmentSize);
    char* nbuf = (char*) buf_malloc (sizeof(char) * m_segmentSize * m_updatedSegmentIdMap.size());
    char* pbuf = (char*) buf_malloc (sizeof(char) * chunkSize * m * m_updatedSegmentIdMap.size());
    vector<off_len_t> updateOfsV;
    vector<pair<FileMetaData*, LL> > updateFileOfsV;
    vector<SegmentMetaData*> lsmdV;


    for (int i = 0; i < m_updateLogLevel; i++) {
        if (i == level) continue;
        m_updateLogbuffers[i].logMutex->lock();
    }

    // for each data segment involved
    int curbuf  = 0;
    std::atomic<int> wcnt;
    wcnt = 0;
    for (unordered_map<sid_t, set<pair<int,chunk_id_t> > >::iterator it = m_updatedSegmentIdMap.begin(); 
        it != m_updatedSegmentIdMap.end(); it++) {

        memset(obuf, 0, m_segmentSize);
        memset(nbuf + curbuf * m_segmentSize, 0, m_segmentSize);
        SegmentMetaData* dsmd = m_segmetaMod->m_metaMap[it->first];
        SegmentMetaData* lsmd = m_segmetaMod->createEntry(true);
#ifndef BLOCK_ITF
        int segOff = 0;
        FileMetaData * fmd = nullptr;
        updateFileOfsV.clear();
#endif
        updateOfsV.clear();
        // read old chunks, xor data to temp buffer
		std::atomic<int> rcnt;
		rcnt = 0;
        for (set<pair<int,chunk_id_t> >::iterator sit = it->second.begin(); 
                sit != it->second.end(); sit++) {
            int logLv = (*sit).first;
            chunk_id_t logChunkId = (*sit).second;

            LogBuffer& logbuf = m_updateLogbuffers[logLv];

            chunk_id_t chunkId = logbuf.chunkMap[logChunkId].first;
#ifndef BLOCK_ITF
            SegmentMetaData* lbsmd = logbuf.segmentMetaData;
            tie(segOff, fmd) = lbsmd->lookupVector[logbuf.chunkMap[logChunkId].second];
            LL fileOff = lbsmd->fileOffsetMap.find(segOff)->second;
#endif
            assert(dsmd == logbuf.updateSegments[logChunkId]);

            updateOfsV.push_back(make_pair(chunkId * chunkSize, chunkSize));
#ifndef BLOCK_ITF
            updateFileOfsV.push_back(make_pair(fmd, fileOff));
#endif

            // TODO : read in parallel
            //m_raidMod->readSegment(dsmd, obuf + chunkId * chunkSize, 
            //        make_pair(chunkId * chunkSize, chunkSize), false);
			rcnt++;
            m_tp.schedule(boost::bind(&RaidMod::readSegment_mt, m_raidMod, 
					dsmd, obuf + chunkId * chunkSize, 
                    make_pair(chunkId * chunkSize, chunkSize), ts, false,
					boost::ref(rcnt)));
            memcpy(nbuf + (curbuf * n + chunkId) * chunkSize, 
                    logbuf.buf + logChunkId * chunkSize, chunkSize);

            // clear up
            logbuf.chunkMap[logChunkId] = make_pair(INVALID_CHUNK,INVALID_CHUNK);;
            logbuf.updateSegments[logChunkId] = nullptr;
            logbuf.curOff -= chunkSize;

        }
		while (rcnt > 0);

        // encode for new parity
        Coding::bitwiseXor(nbuf + curbuf * m_segmentSize, obuf, 
                nbuf + curbuf * m_segmentSize, m_segmentSize);
        vector<Chunk> nchunks = m_raidMod->encodeSegment(nbuf + curbuf * m_segmentSize, false, k);
        //vector<Chunk> ochunks = m_raidMod->encodeSegment(obuf, false, k);
        
        lsmd->locations.resize(k+m);

        for (int i = 0; i < k+m; i++) {
            if (i >= k) {
				lsmd->locations[i] = make_pair(INVALID_DISK,INVALID_CHUNK);
                memcpy(pbuf + (curbuf + (i-k) * m_updatedSegmentIdMap.size()) * chunkSize, nchunks[i].buf, chunkSize);
                //memcpy(obuf + i * chunkSize, ochunks[i].buf, chunkSize);
            }
            free(nchunks[i].buf);
            //free(ochunks[i].buf);
        }

        // xor the difference
        //Coding::bitwiseXor(nbuf + k * chunkSize, obuf + k * chunkSize, 
        //        nbuf + k * chunkSize, chunkSize * m);

        // write back to devices, data to original block, parity to log devices
		//wcnt += 2;
		wcnt += 1;
        // data
        m_tp.schedule(boost::bind(&RaidMod::writePartialSegment_mt,
				m_raidMod, dsmd, nbuf + curbuf * m_segmentSize, updateOfsV, ts, 
                false, nullptr, boost::ref(wcnt)));
        // parity
        //m_tp.schedule(boost::bind(&RaidMod::writeIncompleteSegment_mt,
		//		m_raidMod, lsmd, nbuf + curbuf * n * chunkSize, 0, ts, 
        //        boost::ref(wcnt)));
        // mark the data segment associated with the log segment
        lsmd->locations[0].second = dsmd->segmentId;

#ifndef BLOCK_ITF
        // update file reference back to data segment
        for (unsigned int i = 0; i < updateFileOfsV.size(); i++) {
            FileMetaData* fmd = updateFileOfsV[i].first;
            LL fileOff = updateFileOfsV[i].second;
            // assume the mapping is always align to chunkSize
            chunk_id_t chunkId = updateOfsV[i].first / chunkSize;
            if (fmd != curfmd) {
                fmd->fileMutex.lock(); 
            }

            m_filemetaMod->updateFileLocs(fmd, fileOff, chunkSize, dsmd, 
                    chunkId * chunkSize, true);

            if (fmd != curfmd) {
                fmd->fileMutex.unlock(); 
            }
        }
#endif
        lsmdV.push_back(lsmd);
    }
    m_raidMod->writeBatchedLogParity(lsmdV.size(), lsmdV, pbuf, ts);
    while (wcnt > 0);

    // mark the used LBA for log device
    if (m_syncMod != nullptr) {
        for (auto lsmd : lsmdV) {
            m_syncMod->updateLogDiskMap(lsmd->locations[k].second, lsmd->segmentId);
        }
    }

    // clear update all buffers data and meta
    for (int i = 0; i < m_updateLogLevel; i++) {
        LogBuffer& logbuf = m_updateLogbuffers[i];
        logbuf.curOff = 0;
        logbuf.segmentMetaData->fileOffsetMap.clear();
    }
    m_updatedSegmentIdMap.clear();

    for (int i = 0; i < m_updateLogLevel; i++) {
        if (i == level) continue;
        m_updateLogbuffers[i].logMutex->unlock();
    }

    free(obuf);
    free(nbuf);
    free(pbuf);
    return true;
}
#endif
// always success
#if defined(STRIPE_PLOG) ||  defined(BLOCK_ITF)
bool LogMod::addUpdateSegment(sid_t sid, int level, chunk_id_t chunk) {
    m_updatedSegmentIdMapMutex.lock();
    m_updatedSegmentIdMap[sid].insert(make_pair(level, chunk));
    m_updatedSegmentIdMapMutex.unlock();
    return true;
}
#else
bool LogMod::addUpdateSegment(sid_t sid, int level) {

    m_updatedSegmentIdMapMutex.lock();
    m_updatedSegmentIdMap[sid].insert(level);
    m_updatedSegmentIdMapMutex.unlock();
    return true;
}
#endif

// remove key as well ??
#if defined(STRIPE_PLOG) || defined(BLOCK_ITF)
bool LogMod::removeUpdateSegment(sid_t sid, int level, chunk_id_t chunk) {
    int ret = false;

    m_updatedSegmentIdMapMutex.lock();
    m_updatedSegmentIdMap[sid].erase(make_pair(level, chunk));
    if (m_updatedSegmentIdMap[sid].empty()) {
        m_updatedSegmentIdMap.erase(sid);
        ret = true;
    }
    m_updatedSegmentIdMapMutex.unlock();
    return ret;
}
#else
bool LogMod::removeUpdateSegment(sid_t sid, int level) {
    int ret = false;

    m_updatedSegmentIdMapMutex.lock();
    m_updatedSegmentIdMap[sid].erase(level);
    if (m_updatedSegmentIdMap[sid].empty()) {
        m_updatedSegmentIdMap.erase(sid);
        ret = true;
    }
    m_updatedSegmentIdMapMutex.unlock();
    return ret;
}
#endif

bool LogMod::isUpdateSegmentExist(sid_t sid) {
    return m_updateSegmentIdMap.count(sid); 
}
