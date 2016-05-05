#include "permetamod.hh"
#include "raidmod.hh"

PersistMetaMod::PersistMetaMod (FileMetaDataMod *fileMetaMod, 
        SegmentMetaDataMod* segMetaMod, SyncMod* syncMod, 
        DiskMod *diskmod, DiskMod* dataDiskMod, 
        CodeSetting dataCodeSetting):
    m_fileMetaMod(fileMetaMod), m_segMetaMod(segMetaMod), 
    m_diskMod(diskmod), m_dataDiskMod(dataDiskMod),
    m_pageSize(ConfigMod::getInstance().getPageSize()),
    m_blockSize(ConfigMod::getInstance().getBlockSize()),
    m_dataCodeSetting(dataCodeSetting){


    // init the buffer for write
    m_chunkbuf = (char*) buf_calloc (sizeof(char), m_blockSize * m_chunkbufSize);
    m_readbuf = (char*) buf_calloc (sizeof(char), m_blockSize * m_readbufSize);

    // cannot find the original metadata zone 
    if (!readSuperBlock()) {
        initSuperBlock();
    }

    resetLogHead();

}

PersistMetaMod::~PersistMetaMod() {
    //createSnapshot();
    free(m_chunkbuf);
    free(m_readbuf);
}

/********************/
/**** Public API ****/
/********************/

bool PersistMetaMod::createSnapshot() { 
    std::lock_guard<std::mutex> lk(m_metaOp);

    if (!readSuperBlock()) {
        initSuperBlock();
    }

    lba_t headerlba = m_sb.checkpoint + m_sb.checkpointSize * ((m_chkpt.version+1) % 2);
    lba_t curlba = headerlba + 1;

    // TODO: lock metadata map during checkpointing
    m_chkpt.fileMetaStart = curlba;
    if (!writeAllFileMeta(curlba)) {
        fprintf(stderr, "fatal error: failed to write file metadata during checkpointing!\n");
        return false;
    }

    m_chkpt.segmentMetaStart = curlba;
    if (!writeAllDataSegmentMeta(curlba)) {
        fprintf(stderr, "fatal error: failed to write data stripe metadata during checkpointing!\n");
        return false;
    }

    m_chkpt.logSegmentMetaStart = curlba;
    if (!writeAllLogSegmentMeta(curlba)) {
        fprintf(stderr, "fatal error: failed to write log stripe metadata during checkpointing!\n");
        return false;
    }

    m_chkpt.end = curlba;
    if (!writeCheckpointHeader(headerlba)) {
        fprintf(stderr, "fatal error: failed to write file metadata during checkpointing!\n");
        return false;
    }

    // TODO: release the previous checkpoint
    // clear all dirty flags
    m_dirtyFiles.clear();
    m_dirtySegs.clear();
    m_dirtyLogSegs.clear();
    resetLogHead();

    return true;
}

bool PersistMetaMod::flushMetaLog() {
    std::lock_guard<std::mutex> lk(m_metaOp);
    int curOfs = 0;
    debug("Flush Meta Log ST, files: %ld seg: %ld log seg: %ld head: %u\n", m_dirtyFiles.size(), m_dirtySegs.size(), m_dirtyLogSegs.size(), m_logHead);

    if (!writeDirtyFiles(curOfs)) {
        fprintf(stderr, "fatal error: failed to log metadata of modified files");
        return false;
    }

    if (!writeDirtySegments(curOfs)) {
        fprintf(stderr, "fatal error: failed to log metadata of modified data stripes");
        return false;
    }


    if (!writeDirtyLogSegments(curOfs)) {
        fprintf(stderr, "fatal error: failed to log metadata of modified log stripes");
        return false;
    }

    // force all data in buffer to disk
    forceBlockToDiskLog(curOfs, m_logHead);

    debug("Flush Meta Log ED, files: %ld seg: %ld log seg: %ld head: %u\n", m_dirtyFiles.size(), m_dirtySegs.size(), m_dirtyLogSegs.size(), m_logHead);

    return true;
}

bool PersistMetaMod::addDirtyFile(file_id_t fid) {
    m_dirtyFiles.insert(fid);
    return true;
}

bool PersistMetaMod::addDirtySeg(sid_t sid) {
    m_dirtySegs.insert(sid);
    return true;
}

bool PersistMetaMod::addDirtyLogSeg(sid_t lid) {
    m_dirtyLogSegs.insert(lid);
    return true;
}

/********************/
/**** Checkpoint ****/
/********************/

bool PersistMetaMod::writeCheckpointHeader(lba_t& curlba) {
    m_chkpt.version += 1;
    
    memset(m_chunkbuf, 0, m_blockSize);
    memcpy(m_chunkbuf, &m_chkpt, sizeof(struct CheckpointHeader));
    m_diskMod->writeBlock(0, curlba, m_blockSize, m_chunkbuf);
    return true;
}

bool PersistMetaMod::writeAllFileMeta(lba_t& curlba) {
    nv_unordered_map<file_id_t, FileMetaData*>& fmm = m_fileMetaMod->m_metaMap;
    int curOfs = 0;
    // record init size
    unsigned int fmmSize = fmm.size();
    m_chkpt.fileNum = fmmSize;

    for (auto meta : fmm) {
        // file id
        writeToBufCP(curOfs, curlba, meta.first);
        // file size
        writeToBufCP(curOfs, curlba, meta.second->fileSize);
        // file loc count
        writeToBufCP(curOfs, curlba, meta.second->fileLocs.size());
        
        // file loc
        for (FileLocation floc : meta.second->fileLocs) {
            // file offset
            writeToBufCP(curOfs, curlba, &floc.fileOffset);
            // length
            writeToBufCP(curOfs, curlba,  &floc.locLength);
            // segment id
            writeToBufCP(curOfs, curlba, floc.segmentMetaData->segmentId);
            // segment offset
            writeToBufCP(curOfs, curlba, floc.segmentOffset);
        }
    }
    // force all data in buffer to disk
    forceBlockToDiskCP(curOfs, curlba);

    if (fmmSize != fmm.size()) {
        fprintf(stderr, "Wanring: Total no. of file changed during checkpointing\n");
        assert(fmmSize == fmm.size());
        return false;
    }
    return true;
}

bool PersistMetaMod::writeAllDataSegmentMeta(lba_t& curlba) {
    nv_unordered_map<sid_t, SegmentMetaData*>& dsm = m_segMetaMod->m_metaMap;

    int curOfs = 0;
    unsigned int dsmSize = 0;

    for (auto meta : dsm) {
        if (meta.second->locations.size() == 0) continue;
        if (meta.second->isUpdateLog) continue;
        // segment id
        writeToBufCP(curOfs, curlba, meta.first);
        // chunks
        assert(m_sb.segmentSize == meta.second->locations.size());
        dsmSize += 1;
        for (unsigned int i = 0; i < m_sb.segmentSize; i++) {
            // device id
            writeToBufCP(curOfs, curlba, (unsigned char) meta.second->locations[i].first);
            // addr list size
            unsigned short addrListSize = 1;
            unsigned int hasLog = meta.second->logLocations.count(i);
            if (hasLog) {
                addrListSize += meta.second->logLocations[i].size();
            }
            writeToBufCP(curOfs, curlba, addrListSize);
            // addr
            if (hasLog) {
                for (auto rec : meta.second->logLocations[i]) {
                    writeToBufCP(curOfs, curlba, rec.first);
                }
            }
            writeToBufCP(curOfs, curlba, meta.second->locations[i].second);
        }
        
    }

    // force all data in buffer to disk
    forceBlockToDiskCP(curOfs, curlba);

    m_chkpt.segmentNum = dsmSize;

    return true;
}

bool PersistMetaMod::writeAllLogSegmentMeta(lba_t& curlba) {
    nv_unordered_map<sid_t, SegmentMetaData*>& dsm = m_segMetaMod->m_metaMap;

    int curOfs = 0;
    unsigned int lsmSize = 0;

    for (auto meta : dsm) {
        if (meta.second->locations.size() == 0) continue;
        if (!meta.second->isUpdateLog) continue;
        lsmSize += 1;
        // segment id
        writeToBufCP(curOfs, curlba, meta.first);
        // chunk 
        unsigned int chunkVSize = meta.second->locations.size();
        unsigned char logChunkNum = 0;
        for (unsigned int i = 0; i < chunkVSize; i++) {
            if (meta.second->locations[i].first != INVALID_DISK) logChunkNum ++;
        }
        writeToBufCP(curOfs, curlba, logChunkNum);
        for (unsigned int i = 0; i < chunkVSize; i++) {
            if (meta.second->locations[i].first == INVALID_DISK) continue;
            // log chunk id
            writeToBufCP(curOfs, curlba, i);
            if (i < m_sb.segmentSize) {
                assert(meta.second->logLocations.count(i));
                assert(meta.second->logLocations[i].size());
                // data segment id, data segment chunk id
                writeToBufCP(curOfs, curlba, meta.second->logLocations[i][0].first);
                writeToBufCP(curOfs, curlba, (unsigned char) meta.second->logLocations[i][0].second);
            } else {
                // log disk id, log chunk addr
                writeToBufCP(curOfs, curlba, (unsigned char) meta.second->locations[i].first);
                writeToBufCP(curOfs, curlba, meta.second->locations[i].second);
            }
        }
    }

    // force all data in buffer to disk
    forceBlockToDiskCP(curOfs, curlba);

    m_chkpt.logSegmentNum = lsmSize;

    return true;
}


/********************/
/**** SuperBlock ****/
/********************/

bool PersistMetaMod::readSuperBlock() {
    off_len_t blockOffLen(0,m_blockSize);
    m_diskMod->readBlock(0, 0, m_readbuf, blockOffLen);
    struct SuperBlock* sbt = (struct SuperBlock *) m_readbuf;
    sbt->magic = 123;
    if (sbt->magic != APPLOG_MAGIC ||
            sbt->pageSize != (unsigned) m_pageSize ||
            sbt->blockSize != (unsigned) m_blockSize) {
        return false;
    }
    // overwrite current superblock in cache
    memcpy(&m_sb, m_readbuf, sizeof(struct SuperBlock));
    return true;
}

bool PersistMetaMod::writeSuperBlock() {
    memset(m_chunkbuf, 0, m_blockSize);
    memcpy(m_chunkbuf, &m_sb, sizeof(struct SuperBlock));
    m_diskMod->writeBlock(0, 0, m_blockSize, m_chunkbuf);
    return true;
}

bool PersistMetaMod::initSuperBlock() {
    m_pageSize = ConfigMod::getInstance().getPageSize();
    m_blockSize = ConfigMod::getInstance().getBlockSize();
    m_curZone = 0;

    m_sb.magic = APPLOG_MAGIC;
    m_sb.pageSize = m_pageSize;
    m_sb.blockSize = m_blockSize;
    m_sb.checkpoint = 1;
    m_sb.checkpointSize = 1024 * 512; // TODO: calculate from data disks
    m_sb.logzoneSize = 1024 * 512;
    m_sb.segmentSize = m_dataCodeSetting.n;
    return writeSuperBlock();
}

/********************************************/
/*** Logging (Incremental Checkpointing) ****/
/********************************************/

void PersistMetaMod::resetLogHead() {
    // init log area head (assume clean start)
    m_logHead = m_sb.checkpoint + m_sb.checkpointSize * 2;
}

bool PersistMetaMod::writeDirtyFiles(int& curOfs) {

    if (m_dirtyFiles.empty()) { return true; }

    nv_unordered_map<unsigned int, FileMetaData*>& fmm = m_fileMetaMod->m_metaMap;
    //int curOfs = 0;
    lba_t &curlba = m_logHead;

    unsigned char logType = APPLOG_LOG_FILE;
    writeToBufLog(curOfs, curlba, logType);
    writeToBufLog(curOfs, curlba, m_dirtyFiles.size());

    for (int fid: m_dirtyFiles) {
        assert(fmm.count(fid));
        FileMetaData* fmd = fmm.at(fid);
        // file id
        writeToBufLog(curOfs, curlba, fid);
        // file size
        writeToBufLog(curOfs, curlba, fmd->fileSize);
        // file loc count
        writeToBufLog(curOfs, curlba, fmd->fileLocs.size());
        
        // file loc
        for (FileLocation floc : fmd->fileLocs) {
            // file offset
            writeToBufLog(curOfs, curlba, &floc.fileOffset);
            // length
            writeToBufLog(curOfs, curlba,  &floc.locLength);
            // segment id
            writeToBufLog(curOfs, curlba, floc.segmentMetaData->segmentId);
            // segment offset
            writeToBufLog(curOfs, curlba, floc.segmentOffset);
        }
    }

    // force all data in buffer to disk
    //forceBlockToDiskLog(curOfs, curlba);

    m_dirtyFiles.clear();

    return true;
}

bool PersistMetaMod::writeDirtySegments(int& curOfs) {

    if (m_dirtySegs.empty()) { return true; }

    nv_unordered_map<sid_t, SegmentMetaData*>& dsm = m_segMetaMod->m_metaMap;
    //int curOfs = 0;
    lba_t& curlba = m_logHead;

    unsigned char logType = APPLOG_LOG_SEG;
    writeToBufLog(curOfs, curlba, logType);
    writeToBufLog(curOfs, curlba, m_dirtySegs.size());

    for (sid_t sid: m_dirtySegs) {
        assert(dsm.count(sid));
        SegmentMetaData* dmd = dsm.at(sid);
        if (dmd->locations.size() == 0) continue;
        if (dmd->isUpdateLog) continue;
        // segment id
        writeToBufLog(curOfs, curlba, sid);
        // chunks
        assert(m_sb.segmentSize == dmd->locations.size());
        for (unsigned int i = 0; i < m_sb.segmentSize; i++) {
            // device id
            writeToBufLog(curOfs, curlba, (unsigned char) dmd->locations[i].first);
            // addr list size
            unsigned short addrListSize = 1;
            unsigned int hasLog = dmd->logLocations.count(i);
            if (hasLog) {
                addrListSize += dmd->logLocations[i].size();
            }
            writeToBufLog(curOfs, curlba, addrListSize);
            // addr
            if (hasLog) {
                for (auto rec : dmd->logLocations[i]) {
                    writeToBufLog(curOfs, curlba, rec.first);
                }
            }
            writeToBufLog(curOfs, curlba, dmd->locations[i].second);
        }
    }

    // force all data in buffer to disk
    //forceBlockToDiskLog(curOfs, curlba);
    m_dirtySegs.clear();

    return true;
}

bool PersistMetaMod::writeDirtyLogSegments(int& curOfs) {

    if (m_dirtyLogSegs.empty()) { return true; }

    nv_unordered_map<sid_t, SegmentMetaData*>& dsm = m_segMetaMod->m_metaMap;
    //int curOfs = 0;
    lba_t &curlba = m_logHead;

    unsigned char logType = APPLOG_LOG_LSEG;
    writeToBufLog(curOfs, curlba, logType);
    writeToBufLog(curOfs, curlba, m_dirtyLogSegs.size());

    for (sid_t lid: m_dirtyLogSegs) {
        assert(dsm.count(lid));
        SegmentMetaData* lmd = dsm.at(lid);
        if (lmd->locations.size() == 0) continue;
        if (!lmd->isUpdateLog) continue;
        // segment id
        writeToBufLog(curOfs, curlba, lid);
        // chunk 
        unsigned int chunkVSize = lmd->locations.size();
        unsigned char logChunkNum = 0;
        for (unsigned int i = 0; i < chunkVSize; i++) {
            if (lmd->locations[i].first != INVALID_DISK) logChunkNum ++;
        }
        writeToBufLog(curOfs, curlba, logChunkNum);
        for (unsigned int i = 0; i < chunkVSize; i++) {
            if (lmd->locations[i].first == INVALID_DISK) continue;
            // log chunk id
            writeToBufLog(curOfs, curlba, (unsigned char) i);
            if (i < m_sb.segmentSize) {
                assert(lmd->logLocations.count(i));
                assert(lmd->logLocations[i].size());
                // data segment id, data segment chunk id
                writeToBufLog(curOfs, curlba, lmd->logLocations[i][0].first);
                writeToBufLog(curOfs, curlba, (unsigned char) lmd->logLocations[i][0].second);
            } else {
                // log disk id, log chunk addr
                writeToBufLog(curOfs, curlba, (unsigned char) lmd->locations[i].first);
                writeToBufLog(curOfs, curlba, lmd->locations[i].second);
            }
        }
    }

    // force all data in buffer to disk
    //forceBlockToDiskLog(curOfs, curlba);

    m_dirtyLogSegs.clear();

    return true;
}

/*********************/
/**** Helper Func ****/
/*********************/

template<typename T> void PersistMetaMod::writeToBufCP(int& curOfs, lba_t& curlba, T t) {
    // copy to buf
    memcpy(m_chunkbuf + curOfs, &t, sizeof(T));
    curOfs += sizeof(T);
    // determine whether to flush a block
    flushBlock(curOfs, curlba);
    assert(curlba - (m_sb.checkpoint + m_sb.checkpointSize * ((m_chkpt.version + 1) % 2)) < m_sb.checkpointSize);
}

template<typename T> void PersistMetaMod::writeToBufLog(int& curOfs, lba_t& curlba, T t) {
    memcpy(m_chunkbuf + curOfs, &t, sizeof(T));
    curOfs += sizeof(T);
    // determine whether to flush a block
    flushBlock(curOfs, curlba);
    assert(curlba < m_sb.checkpoint + m_sb.checkpointSize * 2 + m_sb.logzoneSize);
}

void PersistMetaMod::flushBlock(int& curOfs, lba_t& dest) {
    if (curOfs < m_blockSize) return;

    m_diskMod->writeBlock(0, dest, m_blockSize, m_chunkbuf);
    curOfs -= m_blockSize;
    dest += 1;

    if (curOfs > m_blockSize) {
        memcpy(m_chunkbuf, m_chunkbuf + m_blockSize, curOfs);
    }
}

void PersistMetaMod::forceBlockToDiskCP(int &curOfs, lba_t& dest) {
    forceBlockToDisk(curOfs, dest);
    assert(dest - (m_sb.checkpoint + m_sb.checkpointSize * ((m_chkpt.version + 1) % 2)) < m_sb.checkpointSize);
}

void PersistMetaMod::forceBlockToDiskLog(int &curOfs, lba_t& dest) {
    forceBlockToDisk(curOfs, dest);
    assert((unsigned) dest < m_sb.checkpoint + m_sb.checkpointSize * 2 + m_sb.logzoneSize);
}

void PersistMetaMod::forceBlockToDisk(int &curOfs, lba_t& dest) {
    if (curOfs > 0) {
        flushBlock(curOfs, dest);
        if (curOfs > 0) {
            memset(m_chunkbuf + curOfs, 0, m_blockSize - curOfs);
            curOfs = m_blockSize;
            flushBlock(curOfs, dest);
        }
    }
}
