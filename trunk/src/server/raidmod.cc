#include <thread>
#include <mutex>
#include <iostream>
#include <atomic>
#include "common/enum.hh"
#include "common/debug.hh"
#include "common/configmod.hh"
#include "cachemod.hh"
#include "raidmod.hh"
#include "coding/coding.hh"
#include "coding/raid0coding.hh"
#include "coding/raid1coding.hh"
#include "coding/raid5coding.hh"
#include "coding/rdpcoding.hh"
#include "coding/evenoddcoding.hh"
#include "coding/cauchycoding.hh"

RaidMod::RaidMod(DiskMod* diskMod, vector<CodeSetting> codeSettings,
        LowLockQueue<SegmentMetaData*>* gcQueue, SegmentMetaDataMod* segMetaMod,
        SyncMod* syncMod, PersistMetaMod* perMetaMod) {
    m_diskMod = diskMod;
    m_segMetaMod = segMetaMod;
    m_raidSetting.reserve(2);
    m_coding.reserve(2);
    m_raidSetting[DATA].numDisks = diskMod->getNumDisks(DATA);  // cover data SSDs
    m_raidSetting[LOG].numDisks = diskMod->getNumDisks();       // cover data SSDs and log HDDs
    m_raidSetting[DATA].codeSetting = codeSettings[DATA];
    m_raidSetting[LOG].codeSetting = codeSettings[LOG];
    m_gcQueue = gcQueue;
    m_syncMod = syncMod;
    m_perMetaMod = perMetaMod;

    assert(m_raidSetting[DATA].numDisks - (codeSettings[DATA].n - codeSettings[DATA].k)
            >= m_raidSetting[DATA].codeSetting.k);
    assert(m_raidSetting[LOG].numDisks - (codeSettings[LOG].n - codeSettings[LOG].k)
            >= m_raidSetting[LOG].codeSetting.k);

    for (CodeSetting codeSetting : codeSettings) {
        assert(
                codeSetting.codingScheme >= 0
                        && codeSetting.codingScheme < DEFAULT_CODING);
        switch (codeSetting.codingScheme) {
        case RAID0_CODING:
            m_coding.push_back(new Raid0Coding());
            break;
        case RAID1_CODING:
            m_coding.push_back(new Raid1Coding());
            break;
        case RAID5_CODING:
            m_coding.push_back(new Raid5Coding());
            break;
        case RDP_CODING:
            m_coding.push_back(new RdpCoding());
            break;
        case EVENODD_CODING:
            m_coding.push_back(new EvenOddCoding());
            break;
        case CAUCHY_CODING:
            m_coding.push_back(new CauchyCoding(codeSetting));
            break;
        default:
            debug_error("Invalid coding = %d\n", codeSetting.codingScheme);
            exit(-1);
        }
    }

    int numThread = ConfigMod::getInstance().getNumThread();
	m_rtp.size_controller().resize(numThread*4);
	m_wtp.size_controller().resize(numThread);
	int segmentSize = ConfigMod::getInstance().getLogSegmentSize();
	m_segbuf = (char*) buf_malloc (sizeof(char) * segmentSize);
}

#ifdef USE_MD_RAID

// TODO: set bitmap in segmentmetadata
void RaidMod::writeSegment(SegmentMetaData* segmentMetaData, char* buf, LL ts) {
    assert(segmentMetaData->locations.size() == 0);

    const lba_t lba = m_diskMod->writeBlock(0, buf, ts);
    segmentMetaData->locations.push_back(make_pair(0, lba));
    debug ("Block written Disk = %d, LBA = %d\n", 0, lba);

    // add segment to GC List
    if (m_gcQueue == nullptr) {
        debug ("%s\n", "No gcQueue specified");
        return;   // null is only for testing
    }
    m_gcQueue->push (segmentMetaData);
}

// TODO: lock segmentMetaData on read
void RaidMod::readSegment(SegmentMetaData* segmentMetaData, char* buf, off_len_t offLen, LL ts) {
    assert(segmentMetaData->locations.size() > 0);

    SegmentLocation location = segmentMetaData->locations[0];
    m_diskMod->readBlock(0, location.second, buf, offLen, ts);

}


#else

// TODO: set bitmap in segmentmetadata
void RaidMod::writeSegment(SegmentMetaData* segmentMetaData, char* buf, LL ts) {
    assert(segmentMetaData->locations.size() == 0);
    vector<Chunk> chunks = m_coding[DATA]->encode(buf, m_raidSetting[DATA].codeSetting);
    const int n = chunks.size();
#ifndef NDEBUG
    const int k = m_raidSetting[DATA].codeSetting.k;
    const int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
#endif

    vector<disk_id_t> selectedDisks = m_diskMod->selectDisks(n, 
            (unsigned int) segmentMetaData->segmentId);
    assert((int) selectedDisks.size() == n);

	std::atomic_int wcnt;
	wcnt = 0;

    segmentMetaData->locations.resize(n);
    for (int i = 0; i < n; i++) {
        assert (chunks[i].length == chunkSize); 
        segmentMetaData->locations[i].first = selectedDisks[i];
        //const int lba = m_diskMod->writeBlock(selectedDisks[i], chunks[i].buf+j, ts);
        wcnt++;
        m_wtp.schedule(boost::bind(&DiskMod::writeBlock_mt, m_diskMod, 
                selectedDisks[i], chunks[i].buf, 
                boost::ref(segmentMetaData->locations[i].second), 
                boost::ref(wcnt), ts));
    }

    // wait for all write threads to finish
	while (wcnt > 0);

    for (int i = 0; i < n; i++) {
        debug ("Block written Disk = %d, LBA = %d\n", selectedDisks[i], segmentMetaData->locations[i].second);
    }

    // free chunks
    for (Chunk chunk : chunks) {
        free(chunk.buf);
    }

    m_segMetaMod->updateHeap(segmentMetaData->segmentId);    

    // add segment to GC List
    if (m_gcQueue == nullptr) {
        debug ("%s\n", "No gcQueue specified");
        return;   // null is only for testing
    }
    m_gcQueue->push (segmentMetaData);

}

/* assume buf is of segmentSize, modifications are made in corresponding to offLens */
void RaidMod::writePartialSegment(SegmentMetaData* segmentMetaData, char* buf, 
        vector<off_len_t> offLens, LL ts, bool parityUpdate, char* delta) {
#ifndef NDEBUG
    int n = m_raidSetting[DATA].codeSetting.n;
#endif
    int k = m_raidSetting[DATA].codeSetting.k;
    int curOff = 0, len = 0, chunkId = 0;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
    int blockSize = ConfigMod::getInstance().getBlockSize();
    assert(blockSize > 0);
    // assume in-place update, segmentMetaData should be already full 
    assert(segmentMetaData != nullptr && 
            segmentMetaData->locations.size() == (uint32_t)n);


	std::atomic_int wcnt;
	wcnt = 0;

    for (off_len_t offLen : offLens) {
        curOff = offLen.first % chunkSize;
        debug ("off_len_t %d len %d\n", offLen.first, offLen.second);
        chunkId = offLen.first / chunkSize;
        // len of update within a block : min(update len, offset to the end of chunk)
        len = min (offLen.second + offLen.first - chunkId * chunkSize, chunkSize - (curOff % chunkSize));
        assert(len > 0);
        debug ("Going to update Chunk %d [%d]->[%d]\n", chunkId, curOff, curOff+len);
        // may update several chunks, align buffer ref. to changes within chunks
        //m_diskMod->writePartialBlock(segmentMetaData->locations[chunkId].first, 
        //        segmentMetaData->locations[chunkId].second[blockId], curOff, 
        //        len, buf+curOff, ts);
        wcnt++;
        m_wtp.schedule(boost::bind(&DiskMod::writePartialBlock_mt, m_diskMod, 
                segmentMetaData->locations[chunkId].first, 
                segmentMetaData->locations[chunkId].second, curOff,
                len, buf + offLen.first, boost::ref(wcnt), ts));
        if (parityUpdate) {
            // TODO : calculate parity offsets, update parity using XOR
        }
    }

    // wait for all write threads to finish
	while (wcnt > 0) {
		//usleep(1);
	}
    if (m_perMetaMod) { m_perMetaMod->addDirtySeg(segmentMetaData->segmentId); }

}

void RaidMod::writeLogSegment(SegmentMetaData* lsmd, char* buf, int bitmap, LL ts) {

    const int lN = m_raidSetting[LOG].codeSetting.n;
    const int lK = m_raidSetting[LOG].codeSetting.k;
#ifndef NDEBUG
    const int k = m_raidSetting[DATA].codeSetting.k;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
#endif

    // Sync segments on threshold reached
    int logTh = ConfigMod::getInstance().getLogTh();
    int dataTh = ConfigMod::getInstance().getDataTh();
    int dataRemain = (int) (m_diskMod->getRemainingSpace(DATA) * 100);
    int logRemain = (int) (m_diskMod->getRemainingSpace(LOG) * 100);
    if (logRemain <= logTh || dataRemain <= dataTh) {
        syncAllSegments();
        dataRemain = (int) (m_diskMod->getRemainingSpace(DATA) * 100);
        logRemain = (int) (m_diskMod->getRemainingSpace(LOG) * 100);
        debug("SYNC before write free space data %d hdd %d **\n", dataRemain, logRemain);
    }

    assert(lsmd->locations.size() == 0);

    // encode the log data 
    debug("write log seg %d bitmap %x\n", lsmd->segmentId, bitmap);
    vector<Chunk> chunks = m_coding[LOG]->encode(buf, m_raidSetting[LOG].codeSetting);
    assert(chunks.size() == (uint32_t)lN);
    
    // find the log disk to place the log parity chunks
    vector<disk_id_t> selectedLogDisks = m_diskMod->selectDisks(lN-lK, true);
    vector<disk_id_t> dataDiskIds = m_diskMod->getDataDiskIds();

    lsmd->locations.resize(lN);

	std::atomic_int wcnt;
	wcnt = 0;
    // write out to disks according to dsmd and selected log disks
    // updates are "log" to corresponding SSDs
    for (int i = 0; i < lK; i++) {
        assert (chunks[i].length == chunkSize);

        // skip empty blocks
        if ((bitmap & (0x1 << i)) == 0) { 
            lsmd->locations[i] = make_pair(INVALID_DISK,INVALID_LBA);
            continue; 
        }

        disk_id_t diskId = dataDiskIds[i];

        // log the updated data blocks to the same data disk 
        // TODO : variable size update logging
        assert(chunks[i].length == chunkSize);
        debug("look into content %c\n", chunks[i].buf[0]);
        //const int lba = m_diskMod->writeBlock(diskId,chunks[i].buf+j, true, ts);
        lsmd->locations[i].first = diskId;

        wcnt++;
        m_wtp.schedule(boost::bind(
                &DiskMod::writeBlock_mt, m_diskMod,
                diskId, chunks[i].buf, 
                boost::ref(lsmd->locations[i].second),
                boost::ref(wcnt), ts)); 
        // update metadata 
        debug("Log block %d of seg %d gets lba %d on disk %d\n", i, 
                lsmd->segmentId, lsmd->locations[i].second, diskId);
    }

    // parity log chunk to log disks
    for (int i = 0; i < lN-lK; i++) {
        assert (chunks[i+lK].length == chunkSize);
        debug("look into content %c\n", chunks[i+lK].buf[0]);
        disk_id_t diskId = selectedLogDisks[i];
        lsmd->locations[i+lK].first = diskId;

        wcnt++;
        m_wtp.schedule(boost::bind(
                &DiskMod::writeBlock_mt, m_diskMod,
                diskId, chunks[i+lK].buf,
                boost::ref(lsmd->locations[i+lK].second), 
                boost::ref(wcnt), ts));
        // TODO : update the backref
    }

    // wait for all write threads to finish
	while (wcnt > 0);

    // free chunks
    for (Chunk chunk : chunks) {
        free(chunk.buf);
    }

    if (m_perMetaMod) { m_perMetaMod->addDirtyLogSeg(lsmd->segmentId); }

}

void RaidMod::writeIncompleteSegment(SegmentMetaData* dsmd, char* buf, int bitmap, LL ts) {
    // write new data chunks to new disks, log parity to log disks/SSD

    int n = m_raidSetting[DATA].codeSetting.n;
    int k = m_raidSetting[DATA].codeSetting.k;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
    bool toLogDisk = true;	// whether parity should go to log disk
    set<disk_id_t> filter;

    if (dsmd->locations.empty()) {
        dsmd->locations.resize(n);
        for (int i = 0; i < n; i++) {
            dsmd->locations[i] = make_pair(INVALID_DISK, INVALID_LBA);
        }
    }
    assert(dsmd->locations.size() == (uint32_t) n);

	std::atomic_int wcnt;
	wcnt = 0;
    // new data chunks 
    for (int i = 0; i < k; i++) {
        bool writeChunk = bitmap & (0x1 << i);

        if (!writeChunk) {  // avoid selecting the disks used
            if (dsmd->locations.size() > (uint32_t) i) {
                filter.insert(dsmd->locations[i].first);
            }
            continue;
        }

        if (i == k-1) {  // the segment is fully filled
            toLogDisk = false;
        }

        // select 1 disk for each new chunk
        vector<disk_id_t> disks = m_diskMod->selectDisks(1, false, dsmd->segmentId, filter);
        assert(!disks.empty());

        disk_id_t diskId = disks[0];

        // write out the chunks
        dsmd->locations[i].first = diskId;
        wcnt++;
        m_wtp.schedule(boost::bind(&DiskMod::writeBlock_mt, m_diskMod,
                diskId, buf + i * chunkSize, 
                boost::ref(dsmd->locations[i].second), 
                boost::ref(wcnt), ts));
        debug ("[ICW] Block written Disk = %d, LBA = %d [%x]\n", diskId, 
                dsmd->locations[i].second, buf[i * chunkSize]);
        
        // disk included in segment, should not be reselected
        filter.insert(diskId);
    }

    if (n-k > 0) {
        // new log chunk
        vector<disk_id_t> disks = m_diskMod->selectDisks(n-k, toLogDisk, dsmd->segmentId, filter);
        assert(disks.size() >= (uint32_t) n-k);
        for (int i = k; i < n; i++) {
            disk_id_t diskId = disks[i-k];
            if (dsmd->locations[i].first == INVALID_DISK || !toLogDisk) {
                dsmd->locations[i].first = diskId;
            } else {
                diskId = dsmd->locations[i].first;
            }
            wcnt++;
            m_wtp.schedule(boost::bind(&DiskMod::writeBlock_mt, m_diskMod,
                    diskId, buf + i * chunkSize,
                    boost::ref(dsmd->locations[i].second), 
                    boost::ref(wcnt), ts));
            debug ("Block written Disk = %d, LBA = %d\n", diskId, 
                    dsmd->locations[i].second);
        }
    }

    // wait for all write threads to finish
	while (wcnt > 0);

    if (m_perMetaMod) { m_perMetaMod->addDirtySeg(dsmd->segmentId); }
}

#ifdef STRIPE_PLOG
void RaidMod::writeBatchedLogParity(int level, vector<SegmentMetaData*> lsmdV, char* buf, LL ts) {
    const int n = m_raidSetting[DATA].codeSetting.n;
    const int k = m_raidSetting[DATA].codeSetting.k;
    const int segmentSize = ConfigMod::getInstance().getSegmentSize();
    const int chunkSize = segmentSize / k;
    const int blockSize = ConfigMod::getInstance().getBlockSize();

    assert((unsigned int) level == lsmdV.size());

    vector<disk_id_t> disks = m_diskMod->selectDisks(n-k, true);
    vector<vector<lba_t>> lbaV;
    lbaV.resize(n-k);

    std::atomic_int wcnt;
    wcnt = 0;
    for (int i = k; i < n; i++) {
        wcnt++;
        m_wtp.schedule(boost::bind(&DiskMod::writeBlocks_mt, m_diskMod,
                disks[i-k], buf + (i-k) * level * chunkSize, level, 
                boost::ref(lbaV[i-k]), boost::ref(wcnt), ts));
    }
    while (wcnt > 0);

    int blockPerChunk = chunkSize / blockSize;
    assert(blockPerChunk > 0);

    for (int lv = 0; lv < level; lv++) {
        lsmdV[lv]->locations.resize(n);
        // device-by-device (chunk-by-chunk)
        for (int i = k; i < n; i++) {
            disk_id_t diskId = disks[i-k];
            lsmdV[lv]->locations[i].first = diskId;
            assert(lbaV[i-k].size() > (uint32_t) lv*blockPerChunk);
            lsmdV[lv]->locations[i].second = lbaV[i-k][lv*blockPerChunk];
        }
    }

}
#endif

void RaidMod::writeBatchedLogSegments(int level, vector<SegmentMetaData*> lsmdV, char** buf, int chunkMap, LL ts) {
    const int lN = m_raidSetting[LOG].codeSetting.n;
    const int lK = m_raidSetting[LOG].codeSetting.k;
    const int segmentSize = ConfigMod::getInstance().getLogSegmentSize();
    const int chunkSize = segmentSize / lK;
    const int blockSize = ConfigMod::getInstance().getBlockSize();

    vector<vector<lba_t>> lbaV;
    lbaV.resize(lN);
    char ** tbuf = (char**) buf_malloc (sizeof(char*) * lN);

    // Sync segments on threshold reached
    int dataTh = ConfigMod::getInstance().getDataTh();
    int logTh = ConfigMod::getInstance().getLogTh();
    int dataRemain = (int) (m_diskMod->getRemainingSpace(DATA) * 100);
    int logRemain= (int) (m_diskMod->getRemainingSpace(LOG) * 100);
    if (dataRemain <= dataTh || logRemain <= logTh) {
        syncAllSegments();
        dataRemain = (int) (m_diskMod->getRemainingSpace(DATA) * 100);
        debug("SYNC before write free space data %d ** DONE **\n", dataRemain);
    }

    // map and encode the segment in vertical device buffers
    for (int lv = 0; lv < level; lv++) {
        for (int i = 0; i < lN; i++) {
            tbuf[i] = buf[i] + lv*chunkSize;
            debug("chunk %d is [%x]\n", i, tbuf[i][0]);
        }
        debug("encode level %d in a chunk group\n",lv); 
        m_coding[LOG]->encode(tbuf, m_raidSetting[LOG].codeSetting);
    }

    // write the data in device buffers to each device
    vector<disk_id_t> selectedLogDisks = m_diskMod->selectDisks(lN-lK, true);
    vector<disk_id_t> dataDiskIds = m_diskMod->getDataDiskIds();

    std::atomic_int wcnt;
    wcnt = 0;
	disk_id_t diskId = INVALID_DISK;
    for (int i = 0; i < lN; i++) {
        if ((chunkMap & (0x1 << i)) == 0 && i < lK) continue;
        wcnt++;
        diskId = (i < lK)? dataDiskIds[i]:selectedLogDisks[i-lK];
        m_wtp.schedule(boost::bind(&DiskMod::writeBlocks_mt, m_diskMod,
                diskId, buf[i], level, 
                boost::ref(lbaV[i]), boost::ref(wcnt), ts));
    }
    // wait for all write threads to finish
    while (wcnt > 0);

    // write back the LBAs into lsmdV
    int blockPerChunk = chunkSize / blockSize;
    assert(blockPerChunk > 0);
    // segment-by-segment
    for (int lv = 0; lv < level; lv++) {
        assert(lsmdV[lv]->locations.empty());
        lsmdV[lv]->locations.resize(lN);
        // device-by-device (chunk-by-chunk)
        for (int i = 0; i < lN; i++) {
            if ((chunkMap & (0x1 << i)) == 0 && i < lK) {   
                // no data is written for this device
                lsmdV[lv]->locations[i] = make_pair(INVALID_DISK,INVALID_LBA);
                continue;
            }
            diskId = (i < lK)? dataDiskIds[i]:selectedLogDisks[i-lK];
            lsmdV[lv]->locations[i].first = diskId;
            assert(lbaV[i].size() > (uint32_t) lv*blockPerChunk);
            lsmdV[lv]->locations[i].second = lbaV[i][lv*blockPerChunk];
        }
        if (m_perMetaMod) { 
            m_perMetaMod->addDirtyLogSeg(lsmdV[lv]->segmentId); 
        }
    }

    free (tbuf);
}

void RaidMod::writeBatchedSegments(int level, vector<SegmentMetaData*> lsmdV, char* buf, LL ts) {
    const int n = m_raidSetting[DATA].codeSetting.n;
    const int k = m_raidSetting[DATA].codeSetting.k;
    const int segmentSize = ConfigMod::getInstance().getSegmentSize();
    const int chunkSize = segmentSize / k;
    const int blockSize = ConfigMod::getInstance().getBlockSize();

    // init vectors referenced in multi-thread operations
    vector<vector<Chunk>> chunksV;
    chunksV.resize(level);
    vector<vector<lba_t>> lbaV;
    lbaV.resize(n);

    // select the disks for these new segments
    assert(!lsmdV.empty());
    vector<disk_id_t> selectedDisks = m_diskMod->selectDisks(n, 
            (unsigned int) lsmdV[0]->segmentId);
    assert((int) selectedDisks.size() == n);

    // encode multiple segments
    std::atomic_int cnt;
    for (int lv = 0; lv < level; lv++) {
        int k;
        chunksV[lv] = encodeSegment(buf + lv * segmentSize * 2, false, k);
        lsmdV[lv]->locations.resize(n);
    }

    // mapping for block sequences
    // chunk 0: all levels + chunk 1: all levels + ... + chunk n: all levels 
    // TODO : avoid malloc the whole buffer and memcpy ..
    char* blockMap = (char*) buf_malloc (level * n * sizeof(char) * chunkSize);
    for (int i = 0; i < k; i++) {
        for (int lv = 0; lv < level; lv++) {
            // original
            memcpy(blockMap + (i * level + lv) * chunkSize, buf + lv + ((i+lv)%n) * chunkSize, chunkSize);
            // rotate by 1
            //memcpy(blockMap + (i * level + lv) * chunkSize, buf + lv * 2 * segmentSize + ((n-(lv%n)+i) % n) * chunkSize, chunkSize);
            // rotate by m
            //memcpy(blockMap + (i * level + lv) * chunkSize, buf + lv * 2 * segmentSize + ((n-(((n-k)*lv)%n)+i) % n) * chunkSize, chunkSize);
        }
    }

    for (int i = k; i < n; i++) {
        for (int lv = 0; lv < level; lv++) {
            // original
            memcpy(blockMap + (i * level + lv) * chunkSize, chunksV[lv][(i+lv)%n].buf, chunkSize);
            // rotate by 1
            //memcpy(blockMap + (i * level + lv) * chunkSize, chunksV[lv][(n-(lv%n)+i)%n].buf, chunkSize);
            // rotate by m
            //memcpy(blockMap + (i * level + lv) * chunkSize, chunksV[lv][(n-(((n-k)*lv)%n)+i)%n].buf, chunkSize);
        }
    }

    for (vector<Chunk> chunks : chunksV) {
        for (Chunk chunk : chunks) {
            free(chunk.buf);
        }
    }

    // write multiple blocks in parallel
    // TODO : load balancing on disks
    cnt = 0;
    for (int i = 0; i < n; i++) {
        cnt++;
        m_wtp.schedule(boost::bind(&DiskMod::writeBlocks_mt, m_diskMod,
                selectedDisks[i], blockMap + i * level * chunkSize, level, 
                boost::ref(lbaV[i]), boost::ref(cnt), ts));
    }
    // wait for all write threads to finish
    while (cnt > 0);

    // write back the LBAs into lsmdV
    int blockPerChunk = chunkSize / blockSize;
    assert(blockPerChunk > 0);
    // segment-by-segment
    for (int lv = 0; lv < level; lv++) {
        // chunk-by-chunk
        for (int i = 0; i < n; i++) {
            // original
            lsmdV[lv]->locations[i].first = selectedDisks[(i+lv)%n];
            // rotate by 1
            //lsmdV[lv]->locations[i].first = selectedDisks[(n-(lv%n)+i)%n];
            // rotate by m
            //lsmdV[lv]->locations[i].first = selectedDisks[(n-(((n-k)*lv)%n)+i)%n];
            assert(lbaV[(i+lv)%n].size() > (uint32_t) lv*blockPerChunk);
            lsmdV[lv]->locations[i].second = lbaV[(i+lv)%n][lv*blockPerChunk];
            //lsmdV[lv]->locations[i].second = lbaV[(n-(lv%n)+i)][lv*blockPerChunk];
            //lsmdV[lv]->locations[i].second = lbaV[(n-(((n-k)*lv)%n))+i)][lv*blockPerChunk];
        }
    }

    free(blockMap);
}

// ONLY support new write 
void RaidMod::encodeSegment_mt(char* buf, vector<Chunk>& chunks, 
        bool isUpdate, std::atomic_int& cnt) {
    if (isUpdate) {
        chunks = m_coding[LOG]->encode(buf, m_raidSetting[LOG].codeSetting);
    } else {
        chunks = m_coding[DATA]->encode(buf, m_raidSetting[DATA].codeSetting);
    }
    cnt--;
}

vector<Chunk> RaidMod::encodeSegment(char* buf, bool isUpdate, int& k) {
    if (isUpdate) {
        k = m_raidSetting[LOG].codeSetting.k;
        return m_coding[LOG]->encode(buf, m_raidSetting[LOG].codeSetting);
    } else {
        k = m_raidSetting[DATA].codeSetting.k;
        return m_coding[DATA]->encode(buf, m_raidSetting[DATA].codeSetting);
    }
}

// TODO: lock segmentMetaData on read
void RaidMod::readSegment(SegmentMetaData* segmentMetaData, char* buf, 
        off_len_t offLen, LL ts, bool commit) {

    assert(segmentMetaData->locations.size() > 0);

    // obtain chunk status
    vector<disk_id_t> diskIds;
    for (SegmentLocation location : segmentMetaData->locations) {
        diskIds.push_back(location.first);
    }
    vector<bool> chunkStatus = m_diskMod->getDiskStatus(diskIds);

    // read reqSymbols into chunk list
    vector<ChunkSymbols> reqSymbols = m_coding[DATA]->getReqSymbols(chunkStatus,
            m_raidSetting[DATA].codeSetting, offLen);

    const int n = m_raidSetting[DATA].codeSetting.n;
    const int k = m_raidSetting[DATA].codeSetting.k;
    int lK = m_raidSetting[LOG].codeSetting.k;
    bool partialRead = (reqSymbols.size() == (uint32_t)k)? false : true;
    bool degraded = (reqSymbols.back().chunkId >= k);
    map<SegmentMetaData*, set<chunk_id_t> > updateLogSegments;                  // logSegment, chunkId
    int updatedOfflineChunkIds = 0;                                     // chunkId, diskId
    vector<tuple<chunk_id_t, int, vector<off_len_t>>> updatedAliveChunks;          // chunkId, pos in chunks, OffLens

    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
    chunk_id_t startChunk = offLen.first / chunkSize;
    chunk_id_t endChunk = (offLen.first + offLen.second -1) / chunkSize;

    vector<Chunk> chunks;
    chunk_id_t runningId = 0;
    chunk_id_t chunkId = INVALID_CHUNK;
    int chunkPos = 0;
    bool onlyOfflineUpdatedChunks = true;

    //// Read Conditions:
    // A. Full segment read
    //   1. All chunks in segment are not updated -> normal decode
    //   2. Some chunks in segment are updated
    //     (a) all missing chunks are data without updates
    //         -> read the old version for normal decode, return new version
    //     (b) all missing chunks are data with updates
    //         -> read alive data without updates + decode log segments
    //     (c) some missing data chunks are updated, while some do not
    //         -> do both (a) and (b)
    //   3. All chunks are updated -> same as 2(b) except data read are updated
    //
    // B. Partial segment read
    //   1. All missing chunks in read range are not updated -> normal decode
    //   2. Some missing chunks are updated -> both (1) and (3)
    //   3. All missing chunks are updated -> decode log segments
    for (ChunkSymbols chunkSymbols : reqSymbols) {

        chunkId = chunkSymbols.chunkId;
        // chunks skipped are missing ones
        for (; !partialRead && runningId < chunkId; runningId++) {
            debug("skipping chunk %d, with log ?%lu\n", runningId, segmentMetaData->logLocations.count(runningId));
            // check if the missing chunk is updated
            if (!segmentMetaData->logLocations.count(runningId)) {
                // some offline chunks are not updated -> normal decode
                onlyOfflineUpdatedChunks = false;
                continue;
            }

            updatedOfflineChunkIds++;

            assert(segmentMetaData->curLogId[runningId] != INVALID_LOG_SEG_ID);
            SegmentMetaData * logSegment = m_segMetaMod->m_metaMap[segmentMetaData->curLogId[runningId]];
            updateLogSegments[logSegment].insert(runningId);
        }
        runningId += 1;

        // check if the online chunk is updated
        if (!partialRead && segmentMetaData->logLocations.count(chunkId) && degraded) {
            updatedAliveChunks.push_back(make_tuple(chunkId, chunkPos, chunkSymbols.offLens));
        }

        // see if parity is needed for normal decoding
        // if all offline chunks are updated, no need to do normal decoding,
        // read data directly from log segments
        if (chunkId >= k && updatedOfflineChunkIds >= n-k) { 
            break;
        }

        // some online chunk is not updated -> required normal decode
        if (onlyOfflineUpdatedChunks && chunkId < k && chunkId <= endChunk && chunkId >= startChunk) {
            onlyOfflineUpdatedChunks = false;
        }

        chunkPos++;
    }

    // normal decode
    if (!onlyOfflineUpdatedChunks) {

		std::atomic_int rcnt;
		rcnt = 0;
		int pos = 0;
		memset(m_segbuf, 0, chunkSize * n);

        // read the normal symbols before normal decode
        for (ChunkSymbols chunkSymbols : reqSymbols) {
            chunkId = chunkSymbols.chunkId;
            // read chunk out (packed) according to chunkSymbols
            Chunk chunk;
            chunk.chunkId = chunkId;
            chunk.length = getChunkLength(chunkSymbols.offLens);
            chunk.buf = m_segbuf + pos * chunkSize; //(char*) malloc(sizeof(char) * chunkSize);
            pos++;
            //debug ("Chunk length = %d at disk %d lba %d\n", chunk.length, location.first, location.second.front());

            SegmentLocation location = segmentMetaData->locations[chunk.chunkId];
            if (location.first != INVALID_DISK) {
                //readChunkSymbols(location.first, location.second, chunk.buf,
                //        chunkSymbols.offLens, ts);
				rcnt++;
                m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this, 
                        location.first, location.second, chunk.buf, 
                        chunkSymbols.offLens, boost::ref(rcnt), ts));
                debug ("read chunk %d on disk %d at %d [%c]\n", chunk.chunkId, location.first, 
                        location.second, chunk.buf[0]);
            } else {
                memset(chunk.buf, 0, chunk.length);
                debug("zero chunk %d on disk %d\n", chunk.chunkId, location.first); 
            }
            chunks.push_back(chunk);
        }

        // wait for read threads to finish
		while (rcnt > 0);

        // decode

        // if some alived chunks are updated and not commited 
        // -> parity is for old chunks, need to read old verison for decoding
        vector<char*> updatedChunks;
        if (!updatedAliveChunks.empty()) {
			std::atomic_int rcnt;
			rcnt = 0;

            for (tuple<chunk_id_t, int, vector<off_len_t>> chunkInfo: updatedAliveChunks) {
                int chunkPos = -1;
                tie(chunkId, chunkPos, std::ignore) = chunkInfo;
                Chunk& chunk = chunks[chunkPos];
                SegmentLocation location = segmentMetaData->locations[chunkId];
                // retain the update-to-date version in another list
                updatedChunks.push_back(chunk.buf);
                //debug("before move %p [%c]\n", chunk.buf, chunk.buf[0]);
                // new a buffer for the old version
                chunk.buf = (char*) buf_malloc (chunk.length);
                //readChunkSymbols(location.first, segmentMetaData->logLocations[chunkId].front(),
                //        chunk.buf, std::get<2>(chunkInfo), ts);
				rcnt++;
                m_rtp.schedule(boost::bind(static_cast<void (RaidMod::*)(disk_id_t, lba_t, 
                        char*, vector<off_len_t>, std::atomic_int&, LL)>
                        (&RaidMod::readChunkSymbols), this,
                        location.first, segmentMetaData->logLocations[chunkId].front().first,
                        chunk.buf, std::get<2>(chunkInfo), boost::ref(rcnt), ts));
            }

			while (rcnt > 0);
        }

        m_coding[DATA]->decode(reqSymbols, chunks, buf, m_raidSetting[DATA].codeSetting, offLen);

        // if old version is read, need to write back new version
        if (!updatedAliveChunks.empty()) {
            int idx = 0;
            for (tuple<chunk_id_t, int, vector<off_len_t>> chunkInfo: updatedAliveChunks) {
                chunkId = std::get<0>(chunkInfo);
                chunkPos = std::get<1>(chunkInfo);
                if (chunkId >= startChunk && chunkId <= endChunk) {
                    int bufOffset = max(chunkId * chunkSize - offLen.first, 0);
                    int chunkOffset = max(offLen.first - chunkId * chunkSize, 0);
                    int len = min(chunkSize - chunkOffset, offLen.second - bufOffset);
                    // overwrite the old version with the update-to-date ones
                    debug("revert old chunk %d at lba %d [%c] to [%c]\n", chunkId, 
                            segmentMetaData->logLocations[chunkId].front().first, buf[bufOffset], updatedChunks[idx][0]);
                    memcpy(buf + bufOffset, updatedChunks[idx] + chunkOffset, len);
                }
                idx++;
                //free(updatedChunks[idx]);
                // free up the space alloc for old version
                free(chunks[chunkPos].buf);
            }
        }
    } 

    //map<int, map<int, pair<Chunk,int>>> dataSegUpdatedChunks; // data segId, chunks from log disks, lba
    //vector<int> logSegmentIds;
    // decode update log segments for updated data blocks missing
    if (updatedOfflineChunkIds) {
        debug ("Need to read %d chunks from update log segments\n", updatedOfflineChunkIds);

        // easier to copy back to buf, but this introduces memcpy overhead
        off_len_t logOffLen (0, chunkSize * m_raidSetting[LOG].codeSetting.n);
        char* logbuf = (char*) buf_malloc (sizeof(char) * m_raidSetting[LOG].codeSetting.n * chunkSize);

        for (map<SegmentMetaData*, set<int>>:: iterator log = 
                updateLogSegments.begin(); log != updateLogSegments.end(); 
                log++) {
            readLogSegment(log->first, logbuf, logOffLen, ts);
            //logSegmentIds.push_back(log->first->segmentId);

            // copy the missing chunks back to output buffer
            for (chunk_id_t chunkId : log->second) {
                if (chunkId >= startChunk && chunkId <= endChunk) {
                    disk_id_t diskId = segmentMetaData->locations[chunkId].first;
                    int bufOffset = max(chunkId * chunkSize - offLen.first, 0);
                    int chunkOffset = max(offLen.first - chunkId * chunkSize, 0);
                    int len = min(chunkSize - chunkOffset, offLen.second - bufOffset);
                    debug("Need to copy back chunk %d on disk %d from %d len %d into buf at %d [%c]\n", 
                            chunkId, diskId, chunkOffset, len, bufOffset, 
                            logbuf[diskId*chunkSize+chunkOffset]);
                    memcpy(buf + bufOffset, logbuf + diskId * chunkSize + chunkOffset, len);
                }
            }

            // "save" the useful updated data chunks, discard log parity blocks
            assert(log->first->locations.size() > (uint32_t)lK);
            lba_t logLBA = log->first->locations[lK].second;
            for (int i = 0; i < lK; i++) {
                disk_id_t diskId = log->first->locations[i].first;
                if (diskId == INVALID_DISK) continue;
                assert(log->first->locations[i].second != INVALID_LBA);
                //int lba = log->first->locations[i].second;

                assert(m_syncMod != nullptr);
                assert(m_syncMod->m_logDiskMap.count(logLBA));
                SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[m_syncMod->m_logDiskMap[logLBA]];
                sid_t dataSegmentId = lsmd->logLocations[diskId].front().first;
                chunk_id_t chunkId = lsmd->logLocations[diskId].front().second;
                debug("Copy chunk %d on disk %d of log %d belongs to data segment %d\n", 
                        chunkId, diskId, log->first->segmentId, dataSegmentId);

                //Chunk chunk;
                //chunk.chunkId = chunkId;
                //chunk.length = chunkSize;
                //chunk.buf = (char*) buf_malloc (sizeof(char) * chunkSize);
                //memcpy(chunk.buf, logbuf + i * chunkSize, chunkSize);
                //dataSegUpdatedChunks[dataSegmentId][chunkId] = make_pair(chunk,lba);
            }
        }
        free(logbuf);

        // TODO : async commit, reuse chunks read
        if (commit) {
            //debug("Commit updates for %lu log, involves %lu data segments\n",
            //        logSegmentIds.size(), dataSegUpdatedChunks.size());
            //commitUpdate(dataSegUpdatedChunks, logSegmentIds, ts);
        }
    }

	// use global buf and no need to free
    // free chunks
    //for (Chunk chunk : chunks) {
    //    free(chunk.buf);
    //}
}

int RaidMod::commitUpdate(map<sid_t, map<chunk_id_t, pair<Chunk,lba_t> > > dataSegUpdatedChunks, 
        vector<sid_t> logSegmentIds, bool onlyRCW, LL ts) {

    // TODO : sync the parity when updated data block is missing
    // count if there is other chunk missing
    int k = m_raidSetting[DATA].codeSetting.k;
    int lK = m_raidSetting[LOG].codeSetting.k;
    int n = m_raidSetting[DATA].codeSetting.n;
    int lN = m_raidSetting[LOG].codeSetting.n;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
    int pageSize = ConfigMod::getInstance().getPageSize();
    vector<off_len_t> chunkOffLens;                           // for readChunkSymbols()
    chunkOffLens.push_back(make_pair(0,chunkSize));
    char * segbuf = (char*) buf_malloc (n * chunkSize * sizeof(char));
    char * readbuf = nullptr;
    vector<Chunk> oldChunks;
    vector<Chunk> chunks;

    int rcwSize = 1024, rcwCnt = 0; 
    unsigned int processed = 0;
    char * rcwbuf = (char*) buf_malloc (n * chunkSize * sizeof(char) * rcwSize);
    vector<SegmentMetaData*> rcwV;
    rcwV.resize(rcwSize);

    std::atomic_int rcwrcnt;
    rcwrcnt = 0;

    for (auto& entry : dataSegUpdatedChunks) {
        processed ++;
        memset(segbuf, 0, n * chunkSize * sizeof(char));
        SegmentMetaData* dsmd = m_segMetaMod->m_metaMap[entry.first];
        // get chunk status
        vector<disk_id_t> diskIds;
        for (SegmentLocation location : dsmd->locations) {
            diskIds.push_back(location.first);
        }
        vector<bool> chunkStatus = m_diskMod->getDiskStatus(diskIds);

        bool rmw = !onlyRCW;
        unsigned int updateAliveCount = 0;
        vector<chunk_id_t> updatedChunks;
        // find missing chunks and see if they are in buffer -> RCW/RMW
        for (int i = 0; i < k && rmw; i++) {
            if (dsmd->logLocations.count(i)) {
                updatedChunks.push_back(i);
            }
            if (!chunkStatus[i]) {
                // if missing chunk is not in buffer, will do RMW
                if (!entry.second.count(i)) {
                    rmw = true;
                }
            }
            if (entry.second.count(i)) {
                lba_t lba = entry.second[i].second;
                bool found = false;
                // special case: the block is updated to a newer version
                // this log block is stale corresponding to data segment
                if (dsmd->locations[i].second == lba) {
                    found = true;
                }
                if (!found && dsmd->logLocations.count(i)) {
                    for (int j = 0; (uint32_t)j < dsmd->logLocations[i].size(); j++) {
                        if (dsmd->logLocations[i][j].first == lba) {
                            found = true;
                        }
                    }
                }
                if (!found) {
                    // erase the log block from update list
                    debug("NO need to update chunk %d for data segment %d\n", 
                            i, dsmd->segmentId);
                    entry.second.erase(i);
                    continue;
                }
                // check if old-version is available
                if (chunkStatus[i]) {
                    updateAliveCount++;
                }
            }
        }

        rmw = !onlyRCW && (rmw || SyncMod::useRMW(k, chunkStatus, updatedChunks));
        debug("Commit update for data segment %d using %s\n", entry.first, rmw?"RMW":"RRW");
        // commit the update
        if (rmw) {
            // init the buffer for old chunks
            if (readbuf == nullptr) {
                readbuf = (char*) buf_malloc (chunkSize * sizeof(char) * (lN-lK));
            } else {
                memset(readbuf, 0, chunkSize * sizeof(char) * (lN-lK));
            }

            off_len_t segOffLen (0, k * chunkSize);
            vector<ChunkSymbols> reqSymbols = m_coding[DATA]->getReqSymbols(chunkStatus,
                    m_raidSetting[DATA].codeSetting, segOffLen);

            if (updateAliveCount == entry.second.size()) {
                map<chunk_id_t, pair<Chunk,lba_t>> logChunkInfo = entry.second;
                debug("size of map %lu\n", logChunkInfo.size());
                // only read the old version of chunks

				std::atomic_int rcnt;
				rcnt = 0;

                for (auto& chunkInfo : logChunkInfo) {
                    chunk_id_t chunkId = chunkInfo.first;
                    disk_id_t diskId = dsmd->locations[chunkId].first;
                    if (!dsmd->logLocations.count(chunkId)) {
                        free(entry.second[chunkId].first.buf);
                        entry.second.erase(chunkId);
                        debug("skip chunk %d for data segment %d\n", chunkId, dsmd->segmentId);
                        continue;
                    }
                    lba_t& lba= dsmd->logLocations[chunkId].front().first;
                    
                    debug("reading data segment %d for chunk %d with logLocation size %lu\n", 
                            entry.first, chunkId, dsmd->logLocations[chunkId].size());
                    //readChunkSymbols(diskId, LBAs, segbuf + chunkId * chunkSize, 
                    //        chunkOffLens, ts);
					rcnt++;
                    m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                            diskId, lba, segbuf + chunkId * chunkSize,
                            chunkOffLens, boost::ref(rcnt), ts));
                }

				while (rcnt > 0);

            } else if (!entry.second.empty()) {

				std::atomic_int rcnt;
				rcnt = 0;

                // read first 'k' alive data and code blocks 
                for (ChunkSymbols symbol : reqSymbols) {
                    // old version of alived chunks
                    chunk_id_t chunkId = symbol.chunkId;
                    if (!chunkStatus[chunkId]) continue;
                    Chunk chunk;
                    chunk.chunkId = chunkId;
                    chunk.length = chunkSize;
                    chunk.buf = (char*) buf_malloc(chunk.length * sizeof(char));
                    disk_id_t diskId = dsmd->locations[chunk.chunkId].first;
                    lba_t lba;
                    if (entry.second.count(chunkId) && dsmd->logLocations.count(chunkId)) {
                        lba = dsmd->logLocations[chunkId].front().first;
                    } else { 
                        lba = dsmd->locations[chunkId].second;
                    }
                    // always use the oldest LBA
                    if (dsmd->logLocations.count(chunkId)) {
                        lba = dsmd->logLocations[chunkId].front().first;
                    }

                    //readChunkSymbols(diskId, LBAs, chunk.buf, symbol.offLens, ts);
                    if (diskId == INVALID_DISK) {
                        memset(chunk.buf, 0, chunkSize);
                    } else {
                        rcnt++;
                        m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                                diskId, lba, chunk.buf, symbol.offLens, 
                                boost::ref(rcnt), ts));
                        
                        debug("[COMMIT D-READ] rmw read chunk %d on disk %d at %d [%c]\n", 
                                chunk.chunkId, diskId, lba, chunk.buf[0]);
                    }

                    oldChunks.push_back(chunk);

                }

				while (rcnt > 0);

                // decode for the missing old-version chunks
                m_coding[DATA]->decode(reqSymbols, oldChunks, segbuf, 
                        m_raidSetting[DATA].codeSetting, segOffLen);

            }

            // push back the old-version chunks
            for (int i = 0; i < n; i++) {
                if (!entry.second.count(i)) {
                    // zero out the area of chunks not involved in RMW
                    memset(segbuf + i * chunkSize, 0, chunkSize);
                } else {
                    debug("[COMMIT D-READ] rmw get chunk %d [%c]\n", i, 
                            segbuf[i * chunkSize]);
                }
            }

            // prepare the vector for encoded chunks (partial old parity)
            for (Chunk chunk : oldChunks) {
                free(chunk.buf);
                chunk.buf = nullptr;
            }
            oldChunks.clear();

            // get partial old parity
            oldChunks = m_coding[DATA]->encode(segbuf, m_raidSetting[DATA].codeSetting);

            // overwrite the segment for encoding the partial new parity
            // Since the position of chunks is the same, don't need to zero out segbuf
            for (int i = 0; i < k; i++) {
                // new version of missing chunks
                if (entry.second.count(i)) {
                    memcpy(segbuf + i * chunkSize, entry.second[i].first.buf, chunkSize);
                    debug("[COMMIT D-READ] rmw new chunk %d [%c]\n", i, 
                            segbuf[i * chunkSize]);
                }
            }

        } else {
            // no other chunks are missing -> read all other data blocks

            for (int i = 0; i < k; i++) {
                if (entry.second.count(i)) {
                    // copy from buffer
                    memcpy(rcwbuf + (rcwCnt * n + i) * chunkSize, entry.second[i].first.buf, chunkSize);
                    debug("[COMMIT D-READ] rcw use chunk %d in buffer [%c]\n", i,
                            entry.second[i].first.buf[0]);
                } else {
                    // read from disks
                    SegmentLocation location = dsmd->locations[i];
                    lba_t lba = location.second;

                    //readChunkSymbols(dsmd->locations[i].first, LBAs, 
                    //        segbuf + i * chunkSize, chunkOffLens, ts);
                    if (dsmd->locations[i].first == INVALID_DISK) {
                        memset(rcwbuf + (rcwCnt * n + i) * chunkSize, 0, chunkSize);
                    } else {
                        rcwrcnt++;
                        m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                                dsmd->locations[i].first, lba, 
                                rcwbuf + (rcwCnt * n + i) * chunkSize, chunkOffLens, 
                                boost::ref(rcwrcnt), ts));

                        debug("[COMMIT D-READ] rcw read chunk %d on disk %d at %d [%c]\n", 
                                i, location.first, lba, segbuf[i * chunkSize]);
                    }

                }
            }
            rcwV[rcwCnt++] = dsmd;

        }

        if (rmw) {
            // get partial new parity
            chunks = m_coding[DATA]->encode(segbuf, m_raidSetting[DATA].codeSetting);

            // update the parity
            syncParityBlocks(dsmd, chunks, oldChunks, segbuf, readbuf, chunkStatus, !rmw, ts);
            
            // update data segment metadata
            for (int i = 0; i < k; i++) {
                // failed disk, and no update chunk is read from it -> log not modified
                if (!chunkStatus[i] && !entry.second.count(i)) continue;
                // rmw: only clean up modified block log
                // rcw: both modified blocks' log, and erase other blocks' log 
                if (entry.second.count(i)) {
                    lba_t lba = entry.second[i].second;

                    if (dsmd->locations[i].second == lba) {
                        // the updated chunk version is the newest
                        if (dsmd->logLocations.count(i)) {
                            for (pair<lba_t, sid_t> loc : dsmd->logLocations[i]) {
                                lba_t lba = loc.first;
                                m_diskMod->setLBAFree(dsmd->locations[i].first, lba);
                            }
                        }
                        dsmd->logLocations.erase(i);
                        dsmd->curLogId[i] = INVALID_LOG_SEG_ID;
                    } else {
                        // the updated chunk is a log data chunk
                        // erase only LBA vectors that is older than current version
                        for (vector<pair<lba_t,sid_t> >:: iterator it = dsmd->logLocations[i].begin();
                                it != dsmd->logLocations[i].end(); it++) {

                            m_diskMod->setLBAFree(dsmd->locations[i].first,it->first);

                            if (it->first == lba) {
                                dsmd->logLocations[i].erase(dsmd->logLocations[i].begin(), it);
                                break;
                            }
                        }
                        if (dsmd->logLocations[i].empty()) {
                            dsmd->logLocations.erase(i);
                        }
                    }
                } else if (!rmw) {
                    dsmd->logLocations.erase(i);
                    dsmd->curLogId[i] = INVALID_LOG_SEG_ID;
                }
            }

            // free chunks
            for (Chunk chunk : chunks) {
                free(chunk.buf);
                chunk.buf = nullptr;
            }
            chunks.clear();

            for (Chunk chunk : oldChunks) {
                free(chunk.buf);
                chunk.buf = nullptr;
            }
            oldChunks.clear();

            for (auto& chunk : entry.second) {
                free(chunk.second.first.buf);
            }

        } else if (!rmw && (rcwCnt >= rcwSize || processed >= dataSegUpdatedChunks.size()) ){ // batched encode and update for RCW

            // wait for all batched reads
			while (rcwrcnt > 0);

            // batch encode
            for (int i = 0; i < rcwCnt; i++) {
                // get new parity
                chunks = m_coding[DATA]->encode(rcwbuf + i * n * chunkSize, m_raidSetting[DATA].codeSetting);
                for (int id = k; id < n; id++) {
                    memcpy(rcwbuf + (i * n + id) * chunkSize, chunks[id].buf, chunkSize);
                }
                for (Chunk chunk: chunks) {
                    free(chunk.buf);
                    chunk.buf = nullptr;
                }
                chunks.clear();
            }

            // update the parity
            vector<bool> chunkStatus;
            for (int i = 0; i < n; i++) chunkStatus.push_back(true);
            atomic<int> syncCnt;
            syncCnt = 0;
            for (int i = 0; i < rcwCnt; i++) {
                //syncParityBlocks(rcwV[i], chunks, oldChunks, rcwbuf + i * n * chunkSize, readbuf, chunkStatus, !rmw, ts);
                syncCnt++;
                m_rtp.schedule(boost::bind(&RaidMod::syncParityBlocks_mt, this, rcwV[i], 
                        rcwbuf + i * n * chunkSize, readbuf, chunkStatus, boost::ref(syncCnt), ts));
            }
            while (syncCnt > 0);
            
            for (int i = 0; i < rcwCnt; i++) {
                SegmentMetaData* dsmd = rcwV[i];
                auto& entry = dataSegUpdatedChunks[dsmd->segmentId];
                for (int id = 0; id < k; id++) {
                    // rcw: both modified blocks' log, and erase other blocks' log 
                    if (entry.count(id)) {
                        lba_t lba = entry[id].second;

                        if (dsmd->locations[id].second == lba) {
                            // the updated chunk version is the newest
                            if (dsmd->logLocations.count(id)) {
                                for (pair<lba_t, sid_t> loc : dsmd->logLocations[id]) {
                                    lba_t lba = loc.first;
                                    m_diskMod->setLBAFree(dsmd->locations[id].first, lba);
                                }
                            }
                            dsmd->logLocations.erase(id);
                            dsmd->curLogId[id] = INVALID_LOG_SEG_ID;
                        } else {
                            // the updated chunk is a log data chunk
                            // erase only LBA vectors that is older than current version
                            for (vector<pair<lba_t,sid_t> >:: iterator it = dsmd->logLocations[id].begin();
                                    it != dsmd->logLocations[i].end(); it++) {

                                m_diskMod->setLBAFree(dsmd->locations[id].first,it->first);

                                if (it->first == lba) {
                                    dsmd->logLocations[id].erase(dsmd->logLocations[id].begin(), it);
                                    break;
                                }
                            }
                            if (dsmd->logLocations[id].empty()) {
                                dsmd->logLocations.erase(id);
                            }
                        }
                    } else if (!rmw) {
                        for (pair<lba_t, sid_t> loc : dsmd->logLocations[id]) {
                            lba_t lba = loc.first;
                            m_diskMod->setLBAFree(dsmd->locations[id].first, lba);
                        }
                        dsmd->logLocations.erase(id);
                        dsmd->curLogId[id] = INVALID_LOG_SEG_ID;
                    }
                }
            }

            rcwCnt = 0;
            
        }

    }

    // update log segment metadata
    for (sid_t sid : logSegmentIds) {
        SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[sid];

        for (int i = 0; i < lK; i++) {
            SegmentLocation& loc = lsmd->locations[i];
            debug("update log segment %d chunk %d\n", lsmd->segmentId, i);
            if (loc.first == INVALID_DISK) continue;
            debug("Clear log segment %d from %d len %d\n", lsmd->segmentId,
                   i * chunkSize / pageSize, chunkSize / pageSize);
            loc = make_pair(INVALID_DISK,INVALID_LBA);
            lsmd->bitmap.clearBitRange(i * chunkSize / pageSize,
                    chunkSize / pageSize);
            //m_segMetaMod->updateHeap(lsmd->segmentId, 0-chunkSize);
            
        }
        m_segMetaMod->m_metaMap.erase(sid);
        delete lsmd;
    }

    free(segbuf);
    free(rcwbuf);
    if (readbuf != nullptr) { free(readbuf); }

    return dataSegUpdatedChunks.size();
}

#ifndef STRIPE_PLOG
bool RaidMod::syncAllSegments(LL ts) {
    lock_guard<mutex> lk (m_syncMod->m_logDiskMapMutex);

    const LL logZoneSize = ConfigMod::getInstance().getLogZoneSize();
    int lK = m_raidSetting[LOG].codeSetting.k;
    int chunkSize = ConfigMod::getInstance().getLogSegmentSize() / lK;
    const int logDiskNum = m_diskMod->getNumDisks(LOG);
    const int dataDiskNum = m_diskMod->getNumDisks(DATA);

    char* zeroChunk = (char*) buf_calloc (chunkSize, sizeof(char));


    // scan sequentially from the beginning of log disks
    lba_t runningLBA = 0;

    // info on log chunks, e.g. LBA range, segment associated, on log disk
    map<lba_t, sid_t>& logDiskMap = m_syncMod->m_logDiskMap;

    map<sid_t, map<chunk_id_t, pair<Chunk,lba_t> > > dataSegUpdatedChunks;     // dsegId, (chunkId, chunk, lba)
    vector<sid_t> logSegments;

    map<sid_t, map<chunk_id_t, pair<Chunk,lba_t> > > dataSegUpdatedAliveChunks;     // dsegId, (chunkId, chunk, lba)
    vector<sid_t> logAliveSegments;

    // info on disk status, assume disk failures do not occur during parity commit
    // TODO: handle disk failure during parity commit
    bool useZone = false;
    set<disk_id_t> failedDisks = m_diskMod->getFailDisks();

    char* logBlockBuf = (char*) buf_malloc (sizeof(char) * logZoneSize * chunkSize * logDiskNum);
    char* logbuf = (char*) buf_malloc (sizeof(char) * chunkSize * lK);

    off_len_t logOffLen (0, chunkSize * lK);
    vector<off_len_t> chunkOffLen;
    chunkOffLen.push_back(off_len_t(0, chunkSize));

    unsigned int logDiskMapSize = logDiskMap.size();
    unsigned int checkedSize = 0;

    lba_t startLBA = INVALID_LBA;
    lba_t endLBA = INVALID_LBA;
    // loop all log blocks on HDD log disk
    while (checkedSize <= logDiskMapSize) {
        // start of a new zone, before proceeding to next zone
        if (((runningLBA % logZoneSize == 0 && useZone) && runningLBA != 0) || 
                checkedSize == logDiskMapSize) {
            // decode the log segments 
            // 1. read HDD log blocks

            if (useZone) {
                std::atomic_int rcnt;
                rcnt = 0;

                for (int i = 0; i < logDiskNum; i++) {
                    if (failedDisks.count(i+dataDiskNum)) continue;
                    //m_diskMod->readBlocks(dataDiskNum + i, startLBA, endLBA, 
                    //        logBlockBuf + i * chunkSize * logZoneSize, ts);
                    rcnt++;
                    m_rtp.schedule(boost::bind(&DiskMod::readBlocks_mt, 
                            m_diskMod, dataDiskNum + i, startLBA, endLBA,
                            logBlockBuf + i * chunkSize * logZoneSize, 
                            boost::ref(rcnt), ts));
                    debug("read disk %d to buf at %lld, LBA %d to %d\n", 
                            i+dataDiskNum, i*chunkSize*logZoneSize, startLBA, endLBA);
                }

                while (rcnt > 0);
                // 2. construct each log segment and decode them
                vector<Chunk> logSegSymbols;
                Chunk chunk;
                debug("sync %lu log segments\n", logSegments.size());
                for (int i = 0; (uint32_t) i < logSegments.size(); i++) {
                    SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[logSegments[i]];

                    // get required symbols for decode
                    vector<disk_id_t> diskIds;
                    for (SegmentLocation &loc : lsmd->locations) {
                        diskIds.push_back(loc.first);
                    }
                    vector<bool> chunkStatus = m_diskMod->getDiskStatus(diskIds);
                    vector<ChunkSymbols> reqSymbols = 
                            m_coding[LOG]->getReqSymbols(chunkStatus, 
                            m_raidSetting[LOG].codeSetting, logOffLen);

                    debug("sync log segment %d\n", lsmd->segmentId);

					std::atomic_int rcnt;
					rcnt = 0;

                    for(ChunkSymbols symbol : reqSymbols) {
                        chunk_id_t chunkId = symbol.chunkId;
                        SegmentLocation &loc = lsmd->locations[chunkId];
                        disk_id_t diskId = loc.first;
                        lba_t lba = loc.second;
                        chunk.chunkId = chunkId;
                        chunk.length = chunkSize;
                        if (diskId == INVALID_DISK) {
                            chunk.buf = zeroChunk;
                        } else {
                            chunk.buf = (char*) buf_malloc (sizeof(char) * chunkSize);
                            if (chunkId >= lK) {
                                memcpy(chunk.buf, logBlockBuf + 
                                        ((diskId - dataDiskNum) * logZoneSize + 
                                        lba - startLBA) * chunkSize, 
                                        chunkSize);
                            } else {
                                //readChunkSymbols(diskId, lba, chunk.buf, chunkOffLen, ts);
								rcnt++;
                                m_rtp.schedule(boost::bind(
                                        &RaidMod::readChunkSymbols, this,
                                        diskId, lba, chunk.buf, chunkOffLen, 
										boost::ref(rcnt), ts));
                            }
                        }
                        logSegSymbols.push_back(chunk); 
                    }

					while (rcnt > 0);

                    debug("decode log segment %d with %lu symbols\n", 
                            lsmd->segmentId, reqSymbols.size());
                    // decode the log segment
                    m_coding[LOG]->decode(reqSymbols, logSegSymbols, logbuf, 
                            m_raidSetting[LOG].codeSetting, logOffLen);

                    // construct the mapping (data chunks)
                    lba_t logLBA = lsmd->locations[lK].second;
                    for (int j = 0; j < lK; j++) {
                        SegmentLocation& loc = lsmd->locations[j];
                        disk_id_t diskId = loc.first;

                        if (diskId == INVALID_DISK) continue;

                        lba_t lba = loc.second;

                        assert(logDiskMap.count(logLBA));
                        lba_t logSegId = logDiskMap[logLBA];
                        SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[logSegId];

                        disk_id_t dsid = lsmd->logLocations[j].front().first;
                        chunk_id_t chunkId = lsmd->logLocations[j].front().second;
                        // avoid memory leak if a chunk is alread there
                        if (dataSegUpdatedChunks[dsid].count(chunkId)) {
                            free(dataSegUpdatedChunks[dsid][chunkId].first.buf);
                        }
                        chunk.chunkId = chunkId;
                        chunk.length = chunkSize;
                        chunk.buf = (char*) buf_malloc (sizeof(char) * chunkSize);
                        memcpy(chunk.buf, logbuf + j * chunkSize, chunkSize);
                        debug("log segment %d disk %d chunk %d is [%c]\n", 
                                lsmd->segmentId, diskId, chunkId, chunk.buf[chunkSize/2]);
                        dataSegUpdatedChunks[dsid][chunkId] = make_pair(chunk, lba);
                    }

                    for (Chunk chunk : logSegSymbols) {
                        if (chunk.buf == zeroChunk) continue;
                        free(chunk.buf);
                    }
                    logSegSymbols.clear();
                }
                commitUpdate(dataSegUpdatedChunks, logSegments, false, ts);
			}

            debug("sync alive %lu log segments\n", logAliveSegments.size());

            std::atomic_int rcnt;
            rcnt = 0;

            // commit update-to-date data segments
            for (int i = 0; (uint32_t) i < logAliveSegments.size(); i++) {
                SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[logAliveSegments[i]];
                Chunk chunk;
                lba_t logLBA = lsmd->locations[lK].second;
                for (int j = 0; j < lK; j++) {
                    SegmentLocation& loc = lsmd->locations[j];
                    disk_id_t diskId = loc.first;
                    if (diskId == INVALID_DISK) continue;

                    assert(logDiskMap.count(logLBA));
                    sid_t logSegId = logDiskMap[logLBA];
                    SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[logSegId];

                    sid_t dsid = lsmd->logLocations[j].front().first;
                    dataSegUpdatedAliveChunks[dsid] = map<chunk_id_t, pair<Chunk,lba_t>>();
                    // always use rcw for normal commit
                    //lba_t lba = loc.second;
                    //chunk_id_t chunkId = lsmd->logLocations[j].front().second;
                    //// avoid memory leak if a chunk is alread there
                    //if (dataSegUpdatedAliveChunks[dsid].count(chunkId)) {
                    //    free(dataSegUpdatedAliveChunks[dsid][chunkId].first.buf);
                    //}
                    //chunk.chunkId = chunkId;
                    //chunk.length = chunkSize;
                    //chunk.buf = (char*) buf_malloc (sizeof(char) * chunkSize);
                    //debug("sync alive log segment %d read disk %d at %d for chunk %d \n", lsmd->segmentId, diskId, lba, chunkId); 
                    ////readChunkSymbols(diskId, lba, chunk.buf, chunkOffLen, ts);
                    //rcnt++;
                    //m_rtp.schedule(boost::bind(
                    //        static_cast<void (RaidMod::*)(disk_id_t, lba_t, 
                    //        char*, vector<off_len_t>, std::atomic_int&, LL)>
                    //        (&RaidMod::readChunkSymbols), this,
                    //        diskId, lba, chunk.buf, chunkOffLen, 
                    //        boost::ref(rcnt), ts));
                    //dataSegUpdatedAliveChunks[dsid][chunkId] = make_pair(chunk, lba);
                }

            }
            while (rcnt > 0);
            rcnt = 0;

            debug ("commit alive %lu log segments for %lu data segments\n", logAliveSegments.size(), dataSegUpdatedAliveChunks.size());
            debug ("logDiskMap %d\n", logDiskMapSize);
            commitUpdate(dataSegUpdatedAliveChunks, logAliveSegments, true, ts);

            // prepare for next zone
            // refresh the set of fail disk 
            failedDisks = m_diskMod->getFailDisks();
            dataSegUpdatedChunks.clear();
            logSegments.clear();
            dataSegUpdatedAliveChunks.clear();
            logAliveSegments.clear();
            useZone = false;
            startLBA = INVALID_LBA;
            endLBA = INVALID_LBA;
        }
        
        // finding which log block is needed
        if (logDiskMap.count(runningLBA)) {
            checkedSize += 1;
            sid_t lsid = logDiskMap[runningLBA];
            SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[lsid];
            // any disk failed will required read of log block
            bool diskFailed = false;
            for (int i = 0; i < lK; i++) {
                if (failedDisks.count(i) && lsmd->locations[i].first != INVALID_DISK) {
                    diskFailed = true;
                    break;
                }
            }
            if (diskFailed) {
                if (startLBA == INVALID_LBA) {
                    startLBA = runningLBA;   
                }
                logSegments.push_back(lsid);
                useZone = true;
            } else {
                logAliveSegments.push_back(lsid);
            }
            if (startLBA != INVALID_LBA) {
                endLBA = runningLBA;
            }
        } else if (checkedSize == logDiskMapSize) {
            break;
        }
        runningLBA++;
    }

    logDiskMap.clear();
    m_segMetaMod->resetLogSegmentId();
    m_diskMod->resetDisksWriteFront();
    m_diskMod->fsyncDisks();

    free(logBlockBuf);
    free(logbuf);
    free(zeroChunk);

    // create persistent metadata snapshot
    // TODO: remove log segment metadata before snapshot
    if (m_perMetaMod) { m_perMetaMod->createSnapshot(); }

    return true;
}
#else
// we assume all parities can be held in-memory
// and n-k = # of log disks
bool RaidMod::syncAllSegments(LL ts) {
    lock_guard<mutex> lk (m_syncMod->m_logDiskMapMutex);

    const LL logZoneSize = ConfigMod::getInstance().getLogZoneSize();
    int k = m_raidSetting[DATA].codeSetting.k;
    int lK = m_raidSetting[LOG].codeSetting.k;
    int chunkSize = ConfigMod::getInstance().getLogSegmentSize() / lK;
    int blockSize = ConfigMod::getInstance().getBlockSize();
    const int logDiskNum = m_diskMod->getNumDisks(LOG);
    const int dataDiskNum = m_diskMod->getNumDisks(DATA);

    map<lba_t,sid_t>& logDiskMap = m_syncMod->m_logDiskMap;

    // scan sequentially from the beginning of log disks
    int runningLBA = 0;

    // TODO : handle log disk failures

    char* logBlockBuf = (char*) buf_malloc (sizeof(char) * logZoneSize * chunkSize * logDiskNum);
    char** parity = (char**) buf_calloc (sizeof(char*), MAX_SEGMENTS);
    char** parityUpdate = (char**) buf_calloc (sizeof(char*), MAX_SEGMENTS);
    std::atomic_int rcnt, pcnt;

    pcnt = 0;
    for (map<lba_t,sid_t>::iterator mit = logDiskMap.begin(); 
            mit != logDiskMap.end(); mit++) {

        // large region-based read on parity log
        if (mit->first % logZoneSize == 0) {
            runningLBA = mit->first;
            rcnt = 0;
            for (int i = 0; i < logDiskNum; i++) {
                rcnt++;
                m_rtp.schedule(boost::bind(&DiskMod::readBlocks_mt, 
                        m_diskMod, dataDiskNum + i, mit->first, mit->first+logZoneSize,
                        logBlockBuf + i * chunkSize * logZoneSize, 
                        boost::ref(rcnt), ts));
            }
            while (rcnt > 0);
        }

        // obtain data segment info
        SegmentMetaData* lsmd = m_segMetaMod->m_metaMap[mit->second];
        sid_t dsid = lsmd->locations[0].second;
        assert(m_segMetaMod->m_metaMap.count(dsid));
        
        // create cache for updates
        if (parityUpdate[dsid] == nullptr) {
            parityUpdate[dsid] = (char*) buf_calloc (sizeof(char) , chunkSize * logDiskNum);
            parity[dsid] = (char*) buf_calloc (sizeof(char) , chunkSize * logDiskNum);
            SegmentMetaData* dsmd = m_segMetaMod->m_metaMap[dsid];
            // allow parallel read of original parity
            for (int i = 0; i < logDiskNum; i++) {
                disk_id_t diskId = dsmd->locations[k+i].first;
                lba_t lba = dsmd->locations[k+i].second;
                pcnt++;
                m_rtp.schedule(boost::bind(&DiskMod::readBlocks_mt,
                        m_diskMod, diskId, lba, lba + chunkSize/blockSize - 1,
                        parity[dsid] + i * chunkSize, boost::ref(pcnt), ts));
            }
        }

        // aggregate parity updates
        for (int i = 0; i < logDiskNum; i++) {
            char * target = parityUpdate[dsid] + i * chunkSize;
            char * nsrc = logBlockBuf + (i * logZoneSize + (mit->first - runningLBA)) * chunkSize;
            Coding::bitwiseXor(target, target, nsrc, chunkSize);
        }

    }
    while (pcnt > 0);

    pcnt = 0;
    // write the updated parities back to SSDs
    for (int i = 0; i < MAX_SEGMENTS; i++) {
        if (!parity[i]) continue;
        char * target  = parity[i];
        char * src = parityUpdate[i];
        Coding::bitwiseXor(target, target, src, chunkSize * logDiskNum);

        SegmentMetaData* dsmd = m_segMetaMod->m_metaMap[i];
        for (int j = 0; j < logDiskNum; j++) {
            disk_id_t diskId = dsmd->locations[k+j].first;
            lba_t lba = dsmd->locations[k+j].second;
            pcnt++;
            m_wtp.schedule(boost::bind(&DiskMod::writeBlockToLba_mt,
                    m_diskMod, diskId, parity[i] + j * chunkSize, lba, chunkSize,
                    boost::ref(pcnt), ts));
        }
    }
    while (pcnt++);
    
    // free the logDiskMap
    logDiskMap.clear();

    // clean up buffer cache in memory
    for (int i = 0; i < MAX_SEGMENTS; i++) {
        if (parity[i] != nullptr) free(parity[i]);
        if (parityUpdate[i] != nullptr) free(parityUpdate[i]);
    }
    free(parity);
    free(parityUpdate);
    free(logBlockBuf);

    return true;
}
#endif

uint64_t RaidMod::recoverData(vector<disk_id_t> failed, vector<disk_id_t> target, LL ts) {
    int k = m_raidSetting[DATA].codeSetting.k;
    int n = m_raidSetting[DATA].codeSetting.n;
    int lK = m_raidSetting[LOG].codeSetting.k;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;

	if (failed.size() > (unsigned) n-k) {
		return false;
	}

	// for decoding
	set<disk_id_t> failedDisks(failed.begin(), failed.end());
	vector<chunk_id_t> failedChunks;
	vector<bool> chunkStatus;
	vector<ChunkSymbols> reqSymbols;
	off_len_t offLen;
	lba_t lba;
	disk_id_t diskId;
	// temp holder of alive data
	vector<Chunk> chunks;
	Chunk chunk;
#ifdef BATCH_RECOVERY
    // batch holder
    uint32_t batch_size = ConfigMod::getInstance().getRecoveryBatchSize();
    char *readbuf = (char*) buf_calloc (chunkSize * n * batch_size, sizeof(char));
    char *writebuf = (char*) buf_calloc (chunkSize * (n-k) * batch_size, sizeof(char));
    map<disk_id_t,set<lba_t> > readbufMap;
    vector<SegmentMetaData*> dataSegments;
    vector< pair<uint32_t, uint32_t> > segmentChunkEnd; // pos in chunks, pos in failedChunks
#endif
    char *segbuf = (char*) buf_calloc (chunkSize * n * 2, sizeof(char));
    // stats
    uint64_t recovered = 0;

	// init
	chunk.length = chunkSize;
	chunkStatus.resize(n);
#ifdef BATCH_RECOVERY
    dataSegments.reserve(batch_size);
    segmentChunkEnd.resize(batch_size);
    chunks.reserve(n * batch_size);
#else 
	chunks.reserve(n);
#endif

    assert(failed.size() == target.size());

	// scan all data segments
	m_segMetaMod->m_metaOptMutex.lock();
    // (3) recover in reverse order of updates
    unordered_set<sid_t> updatedSegments, updatedSegmentsMirror;
    vector<sid_t> updatedSegmentsV;
    map<lba_t, sid_t>& logDiskMap = m_syncMod->m_logDiskMap;
    bool scanLogChunks = false;
    lba_t runningLBA = logDiskMap.empty()? 1 : logDiskMap.rbegin()->first + 1;
    for (; runningLBA > 0 && scanLogChunks; runningLBA--) {
        if (logDiskMap.count(runningLBA - 1) < 1)
            continue;

        sid_t lsid = logDiskMap[runningLBA - 1];
        SegmentMetaData *lsmd = m_segMetaMod->m_metaMap.at(lsid);
        assert(lsmd->isUpdateLog);
        // mark the number of associated data segments
        for (chunk_id_t lcid = 0; lcid < lK; lcid++) {
            if (lsmd->locations[lcid].first == INVALID_DISK)
                continue;
            assert(lsmd->logLocations.count(lcid));
            sid_t dsid = lsmd->logLocations[lcid].front().first;
            pair<unordered_set<sid_t>::iterator, bool> ret = updatedSegments.insert(dsid);
            if (ret.second) updatedSegmentsV.push_back(dsid);
        }
        if (updatedSegmentsV.size() > batch_size * 8)
            break;
    }
    updatedSegmentsMirror.insert(updatedSegments.begin(), updatedSegments.end());
    uint32_t normalCount = 0;

    // (1) in the hashed order
	//for (pair<sid_t, SegmentMetaData*> segment : m_segMetaMod->m_metaMap) {
    // (2) lastest log chunk first, followed by stripes in ascending order of id
    sid_t lastSid = m_segMetaMod->probeNextSegmentId();
    for (sid_t sid = 0, did = 0; sid < lastSid; sid++) {
        if (!updatedSegments.empty()) {
            did = updatedSegmentsV[updatedSegmentsV.size()-updatedSegments.size()];
            updatedSegments.erase(did);
            sid--;
        } else {
            did = sid;
            // skip recovered segments
            if (updatedSegmentsMirror.count(sid)) {
                continue;
            }
        }
        if (m_segMetaMod->m_metaMap.count(did) == 0)
            continue;
        pair<sid_t, SegmentMetaData*> segment (did, m_segMetaMod->m_metaMap.at(did));
		// ignore log stripes (assume parity commit is always done before recovery)
		if (segment.second->isUpdateLog) {
			continue;
		}
		SegmentMetaData* dataSegment = segment.second;
		// ignore incompleted segments (buffered in memory)
		if (dataSegment->locations.size() < (unsigned) n) {
			continue;
		}
        normalCount++;
		// find out which chunk is lost
        uint32_t failedCount = 0;
		for (chunk_id_t i = 0; i < n; i++) {
			chunkStatus[i] = true;
			if (failedDisks.count(dataSegment->locations[i].first)) {
				if (failedCount == 0) {
					offLen.first = chunkSize * i;
					offLen.second = chunkSize;
				} else {
					offLen.second = chunkSize * (i - failedChunks[failedChunks.size()-failedCount] + 1);
				}
				failedChunks.push_back(i);
				chunkStatus[i] = false;
                failedCount++;
			}
		}
		// no lost chunks
		if (failedCount < 1) {
			continue;
		}

		// find the symbols to read
		reqSymbols = m_coding[DATA]->getReqSymbols(chunkStatus, m_raidSetting[DATA].codeSetting, offLen);
		
#ifdef BATCH_RECOVERY
        // push required chunk into list
        for (unsigned int i = 0; i < reqSymbols.size(); i++) {
			// push the chunk metadata to list
			chunk.chunkId = reqSymbols[i].chunkId;
			chunk.buf = 0;
			chunks.push_back(chunk);
            // push the lbas to read into list
			std::tie(diskId, lba) = dataSegment->locations[chunk.chunkId];
            if (diskId != INVALID_DISK) {
                assert(lba != INVALID_LBA);
                readbufMap[diskId].insert(lba);
                if (CacheMod::getInstance().isInit()) {
                    CacheMod::getInstance().insertReadBlock(diskId, lba);
                }
            }
        }
        // mark chunk ends
        segmentChunkEnd[dataSegments.size()].first = chunks.size();
        segmentChunkEnd[dataSegments.size()].second = failedChunks.size();
        dataSegments.push_back(dataSegment);
        assert(dataSegments.size() <= batch_size);
        if (dataSegments.size() == batch_size) {
            recovered += batchRecovery(readbuf, writebuf, segbuf, readbufMap, dataSegments, chunks, failedChunks, segmentChunkEnd, batch_size, target);
            dataSegments.clear();
            readbufMap.clear();
            chunks.clear();
            failedChunks.clear();
        }
#else /* else (not) BATCH_RECOVERY */
		// read the alive chunks in parallel
		std::atomic_int rcnt;
		rcnt = 0;
		for (unsigned int i = 0; i < reqSymbols.size(); i++) {
			// push the chunk metadata to list
			chunk.chunkId = reqSymbols[i].chunkId;
			chunk.buf = segbuf + chunk.chunkId * chunkSize;
			chunks.push_back(chunk);
			// read the chunk (symbols)
			diskId = dataSegment->locations[chunk.chunkId].first; // disk id
            if (diskId == INVALID_DISK) {
                memset(segbuf + chunk.chunkId * chunkSize, 0, chunkSize);
            } else {
                lba = dataSegment->locations[chunk.chunkId].second; // disk lba 
                assert(lba != INVALID_LBA);
                //m_diskMod->readBlocks(diskId, lba, lba, segbuf + chunk.chunkId * chunkSize, ts);
                m_rtp.schedule(boost::bind(&DiskMod::readBlocks_mt, 
                        m_diskMod, diskId, lba /* start */, lba /* end */,
                        segbuf + chunk.chunkId * chunkSize,
                        boost::ref(rcnt), ts));
                rcnt++;
            }
		}
		while(rcnt > 0);

		// decode for lost data
		m_coding[DATA]->decode(reqSymbols, chunks, segbuf + n * chunkSize, m_raidSetting[DATA].codeSetting, offLen);

		// write the recoved chunk
		std::atomic_int wcnt;
		wcnt = 0;
		for (unsigned int i = 0; i < failedChunks.size(); i++) {
            assert(i < target.size());
			chunk_id_t chunkId = failedChunks[i];
			// mark the chunk is now on the new disk
			dataSegment->locations[chunkId].first = target[i];
			// write the chunk to disk
            //dataSegment->locations[chunkId].second = m_diskMod->writeBlock(target[i], segbuf + chunkId * chunkSize, ts); 
			m_wtp.schedule(boost::bind(&DiskMod::writeBlock_mt, m_diskMod, 
					target[i], segbuf + (n + chunkId - offLen.first / chunkSize) * chunkSize, 
					boost::ref(dataSegment->locations[chunkId].second), 
					boost::ref(wcnt), ts));
			wcnt++;
            recovered += chunkSize;
		}
		while(wcnt > 0);
        chunks.clear();
        failedChunks.clear();
#endif
        reqSymbols.clear();
	}
#ifdef BATCH_RECOVERY
    if (!chunks.empty() && !failedChunks.empty()) {
        recovered += batchRecovery(readbuf, writebuf, segbuf, readbufMap, dataSegments, chunks, failedChunks, segmentChunkEnd, batch_size, target);
    }
#endif
	m_segMetaMod->m_metaOptMutex.unlock();
	// free the resources
#ifdef BATCH_RECOVERY
    free(writebuf);
    free(readbuf);
#endif
	free(segbuf);
	return recovered;
}

uint64_t RaidMod::batchRecovery(
        char *readbuf, char *writebuf, char* segbuf, map<disk_id_t, set<lba_t> > &readbufMap,
        vector<SegmentMetaData*> &dataSegments, vector<Chunk> &chunksRead,
        vector<chunk_id_t> &failedChunks, vector<pair<uint32_t,uint32_t> > segmentChunkEnd,
        uint32_t batchSize, vector<disk_id_t> target) {

    int k = m_raidSetting[DATA].codeSetting.k;
    int n = m_raidSetting[DATA].codeSetting.n;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;

	vector<ChunkSymbols> reqSymbols;
    ChunkSymbols defaultSymbol;
    vector<Chunk> chunks;
    reqSymbols.reserve(n);
    defaultSymbol.offLens.push_back(pair<uint32_t, uint32_t>(0, chunkSize));
    chunks.reserve(n);

    unordered_map<disk_id_t,uint32_t> diskRank;
    unordered_map<disk_id_t,unordered_map<lba_t, uint32_t> > lbaRank;
    uint32_t pos;

    // read chunks in parallel
    std::atomic_int rcnt;
    rcnt = 0;
    for (auto diskToRead : readbufMap) {
        // mark the disk position for offset calculation
        pos = std::distance(readbufMap.begin(), readbufMap.find(diskToRead.first));
        diskRank[diskToRead.first] = pos;
        // prefetch
        //lba_t start = *(diskToRead.second.begin());
        //lba_t end = *(diskToRead.second.end());
        //m_diskMod->prefetchRead(diskToRead.first, start, end - start); 
        // read the chunks
        m_rtp.schedule(boost::bind(&DiskMod::readBlocks_mt, 
                m_diskMod, diskToRead.first, diskToRead.second,
                readbuf + pos * batchSize * chunkSize,
                boost::ref(rcnt), 0));
        rcnt++;
    }
    while(rcnt > 0);

    for (auto diskToRead : readbufMap) {
        uint32_t count = 0;
        for (auto lba = diskToRead.second.begin(); lba != diskToRead.second.end(); lba++ ) {
            pair<lba_t, uint32_t> record (*lba, count);
            lbaRank[diskToRead.first].insert(record);
            count++;
        }
        // release cache
        //lba_t start = *(diskToRead.second.begin());
        //lba_t end = *(diskToRead.second.end());
        //m_diskMod->freeReadCache(diskToRead.first, start, end - start); 
    }

    // decode all failed chunks in segments
    uint32_t chunkStart = 0;
    uint32_t failedChunkStart = 0;
    uint32_t chunkEnd, failedChunkEnd, chunkId;
    lba_t lba;
    disk_id_t diskId;
    off_len_t offLen;

    // disk write count
    vector<uint32_t> writeCounts;
    vector<lba_t> firstLBAs;
    for (uint32_t i = 0; i < target.size(); i++) {
        writeCounts.push_back(0);
        firstLBAs.push_back(INVALID_LBA);
    }

    for (uint32_t i = 0; i < dataSegments.size(); i++) {
        std::tie(chunkEnd, failedChunkEnd) = segmentChunkEnd[i];
        for (uint32_t chunkIdx = chunkStart; chunkIdx < chunkEnd; chunkIdx++) {
            chunkId = chunksRead[chunkIdx].chunkId;
            std::tie(diskId, lba) = dataSegments[i]->locations[chunkId];
            if (diskId == INVALID_DISK) {
                memset(segbuf + chunkId * chunkSize, 0, chunkSize);
                chunksRead[chunkIdx].buf = segbuf + chunkId * chunkSize;
                chunks.push_back(chunksRead[chunkIdx]);
            } else {
                // locate the chunk in readbuf
                assert(readbufMap.count(diskId) > 0);

                auto lbaIt = readbufMap[diskId].find(lba);
                assert(lbaIt != readbufMap[diskId].end());

                pos = lbaRank[diskId].at(*lbaIt);
                chunksRead[chunkIdx].buf = readbuf + (diskRank[diskId] * batchSize + pos) * chunkSize;
                chunks.push_back(chunksRead[chunkIdx]);
            }
            defaultSymbol.chunkId = chunkId;
            reqSymbols.push_back(defaultSymbol);
        }
        for (uint32_t j = failedChunkStart; j < failedChunkEnd; j++) {
            if (j == failedChunkStart) {
                offLen.first = failedChunks[j] * chunkSize;
                offLen.second = chunkSize;
            } else {
                offLen.second = (failedChunks[j] - offLen.first + 1) * chunkSize;
            }
        }
        // decode 
		m_coding[DATA]->decode(reqSymbols, chunks, segbuf + n * chunkSize, m_raidSetting[DATA].codeSetting, offLen);
        // copy chunks to write buffer
        for (uint32_t j = failedChunkStart; j < failedChunkEnd; j++) {
            uint32_t index = j - failedChunkStart;
            memcpy(writebuf + (index * batchSize + writeCounts[index]++) * chunkSize,
                    segbuf + (n + failedChunks[j] - offLen.first / chunkSize) * chunkSize,
                    chunkSize );
        }
        
        chunkStart = chunkEnd;
        failedChunkStart = failedChunkEnd;
        chunks.clear();
        reqSymbols.clear();
    }
    std::atomic_int wcnt;
    wcnt = 0;
    uint64_t recovered = 0;
    // write chunk in parallel
    for (uint32_t i = 0; i < target.size(); i++) {
        m_wtp.schedule(boost::bind(&DiskMod::writeSeqBlocks_mt, m_diskMod, 
                target[i], writebuf + (i * batchSize * chunkSize), 
                writeCounts[i], boost::ref(firstLBAs[i]), 
                boost::ref(wcnt), 0));
        wcnt++;
        recovered += writeCounts[i] * chunkSize;
    }
    while(wcnt > 0);
    // update metadata
    chunkStart = 0;
    failedChunkStart = 0;
    for (uint32_t i = 0; i < target.size(); i++) {
        writeCounts[i] = 0;
    }
    for (uint32_t i = 0; i < dataSegments.size(); i++) {
        std::tie(chunkEnd, failedChunkEnd) = segmentChunkEnd[i];
        for (uint32_t j = failedChunkStart; j < failedChunkEnd; j++) {
            uint32_t index = j - failedChunkStart;
            dataSegments[i]->locations[failedChunks[j]].first = target[index];
            dataSegments[i]->locations[failedChunks[j]].second = firstLBAs[index] + writeCounts[index];
            writeCounts[index]++;
        }
        chunkStart = chunkEnd;
        failedChunkStart = failedChunkEnd;
    }
    return recovered;
}

// return whether sync is successful
bool RaidMod::syncSegment(sid_t segmentId, LL ts) {
    SegmentMetaData* dataSegment = m_segMetaMod->m_metaMap[segmentId];
    int k = m_raidSetting[DATA].codeSetting.k;
    int n = m_raidSetting[DATA].codeSetting.n;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
    int pageSize = ConfigMod::getInstance().getPageSize();
    // chunk-wise update
    // TODO : udpate sepecific chunks
    vector<disk_id_t> diskIds;
    map<SegmentMetaData*, set<pair<chunk_id_t,disk_id_t>>> logSegments;     // logSegId, chunkId, diskId
    char* segbuf = (char*) buf_calloc (chunkSize* n, sizeof(char));
    char* readbuf = (char*) buf_malloc (sizeof(char) * chunkSize * n);

    vector<off_len_t> chunkOffLens;                           // for readChunkSymbols()
    chunkOffLens.push_back(make_pair(0,chunkSize));

    // get disks status, get update ratio
    vector<chunk_id_t> updatedChunks;
    assert(dataSegment->locations.size() == (uint32_t) n);
    for (int i = 0; i < n; i++) {
        diskIds.push_back(dataSegment->locations[i].first);
        if (dataSegment->logLocations.count(i)) {
            updatedChunks.push_back(i);
        }
    }

    // no need to sync any log data blocks
    if (updatedChunks.empty()) {
        debug("Segment %d has no updated chunks\n", segmentId);
        return true;
    }
    
    vector<bool> chunkStatus = m_diskMod->getDiskStatus(diskIds);
    assert(chunkStatus.size() == (uint32_t)n);

    // TODO : other method to choose rcw or rmw scheme
    bool rcw = !SyncMod::useRMW(k, chunkStatus, updatedChunks); 

    debug("Use %s (%lu) for segment %d\n", rcw?"RCW":"RMW", 
            updatedChunks.size() * 100 / k, segmentId); 
    memset(readbuf, 0, n * chunkSize);
    memset(segbuf, 0, n * chunkSize);

    std::atomic_int rcnt;
    rcnt = 0;

    for (chunk_id_t i : updatedChunks) {
        if (!dataSegment->logLocations.count(i)) {
            // chunk is not updated
            continue;
        }

        disk_id_t diskId;
        lba_t lbaNew;
        tie(diskId, lbaNew) = dataSegment->locations[i];

        // TODO : check whole lba vector for partial chunk update
        vector<pair<lba_t,sid_t> >::reverse_iterator runningLba = dataSegment->logLocations[i].rbegin();
        vector<pair<lba_t,sid_t> >::reverse_iterator runningEnd = dataSegment->logLocations[i].rend();
        for (; runningLba != runningEnd; runningLba++) {
            if (runningLba->second == INVALID_LOG_SEG_ID) continue;
            SegmentMetaData* logSegMeta = m_segMetaMod->m_metaMap[runningLba->second];
            logSegments[logSegMeta].insert(make_pair(i,diskId));
        }

        // for invalidating metadata
        sid_t latest = dataSegment->curLogId[i];
        
        assert(latest != INVALID_SEG);
        debug("sync segment %d chunk %d latest %d\n", segmentId, i, latest);
        
        if (!rcw) { 
            // read both the new and old version of chunks, TODO : reorganize for parallel read
            lba_t lbaOld = dataSegment->logLocations[i].front().first;
            if (chunkStatus[i]) {
                // normal read
                //readChunkSymbols(diskId, lbaOld, readbuf + i * chunkSize, chunkOffLens, ts);
                //readChunkSymbols(diskId, lbaNew, segbuf + i * chunkSize, chunkOffLens, ts);
                rcnt++;
                m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                        diskId, lbaOld, readbuf + i * chunkSize, chunkOffLens, 
                        boost::ref(rcnt), ts));
                rcnt++;
                m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                        diskId, lbaNew, segbuf + i * chunkSize, chunkOffLens, 
                        boost::ref(rcnt), ts));
            } else {
                assert(0);
            }
        }
    }

    while(rcnt > 0);

    if (rcw) {
        off_len_t offlen (0, k*chunkSize);
        // always get latest data segment
        readSegment(dataSegment, segbuf, offlen, ts, false);
    }

    // encode for (parts of the) new parities
    vector<Chunk> chunks = m_coding[DATA]->encode(segbuf, m_raidSetting[DATA].codeSetting);
    vector<Chunk> oldChunks;
    if (!rcw) {
        // encode for parts of the old parities
        oldChunks = m_coding[DATA]->encode(readbuf, m_raidSetting[DATA].codeSetting);
        assert(oldChunks.size() == (uint32_t)n);
    }
    assert(chunks.size() == (uint32_t)n);

    syncParityBlocks(dataSegment, chunks, oldChunks, segbuf, readbuf, chunkStatus, rcw, ts);
    
    // invalidate logs and meta
    for (auto& logSeg : logSegments) {
        for (pair<chunk_id_t, disk_id_t> chunk : logSeg.second) {
            chunk_id_t chunkId = chunk.first;
            disk_id_t diskId = chunk.second;
            // log segment
            // TODO : update lookupVector
            // mark location stale
            logSeg.first->locations[diskId] = make_pair(INVALID_DISK,INVALID_LBA);
            // invalidate bitmap 
            logSeg.first->bitmap.clearBitRange(diskId * chunkSize / pageSize, 
                    chunkSize / pageSize);
            //m_segMetaMod->updateHeap(logSeg.first->segmentId, 0-chunkSize);
            // data segment
            dataSegment->logLocations.erase(chunkId);
            dataSegment->curLogId[chunkId] = INVALID_LOG_SEG_ID;
        }
    }

    for (int i = 0; i < n; i++) {
        free(chunks[i].buf);
        if (!rcw) 
            free(oldChunks[i].buf);
    }
    free(segbuf);
    free(readbuf);
    return true;
}

void RaidMod::syncParityBlocks_mt(SegmentMetaData* dataSegment,
        char* segbuf, char* readbuf, vector<bool> chunkStatus, atomic<int>& cnt, LL ts) {
    vector<Chunk> chunks, oldChunks;
    syncParityBlocks(dataSegment, chunks, oldChunks, segbuf, readbuf, chunkStatus, true, ts);
    cnt--;
}

void RaidMod::syncParityBlocks(SegmentMetaData* dataSegment, 
        const vector<Chunk>& chunks, const vector<Chunk>& oldChunks, 
        char* segbuf, char* readbuf, vector<bool> chunkStatus, bool rcw, LL ts) {

    int k = m_raidSetting[DATA].codeSetting.k;
    int n = m_raidSetting[DATA].codeSetting.n;
    int chunkSize = ConfigMod::getInstance().getSegmentSize() / k;
    vector<off_len_t> offLens;
    vector<off_len_t> chunkOffLens;                           // for readChunkSymbols()
    chunkOffLens.push_back(make_pair(0,chunkSize));

    std::atomic_int rcnt;
    rcnt = 0;

    readbuf = (char*) buf_malloc(sizeof(char) * chunkSize * n);

    // copy the new parities into segment buffer
    for (int i = k; i < n; i++) {
        // obtain the new parity
        if (rcw) {
            // no need to copy data anymore for batched update
            //memcpy(segbuf + i * chunkSize, chunks[i].buf, chunkSize);
        } else {
            // read old parity
            memset(readbuf + (i-k)* chunkSize, 0, chunkSize);
            if (chunkStatus[i]) {
                // normal read
                disk_id_t diskId = dataSegment->locations[i].first;
                lba_t lba = dataSegment->locations[i].second;
                //readChunkSymbols(diskId, lba, readbuf + (i-k) * chunkSize, 
                //        chunkOffLens, ts);
                rcnt++;
                m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                        diskId, lba, readbuf + (i-k) * chunkSize,
                        chunkOffLens, boost::ref(rcnt), ts));
            } else {
                // degraded read
                // TODO : get old version of chunk?
            }

        }

        // mark the parity chunk as "to-be-overwritten"
        if (chunkStatus[i]) {
            debug("look into parity chunk %d to sync [%c]\n", i, segbuf[i* chunkSize + chunkSize / 2]);
            offLens.push_back(make_pair(i * chunkSize, chunkSize));
        }
    }

    while (rcnt > 0);

    for (int i = k; i < n && !rcw; i++) {
        // apply delta to parity 
        // (1) add new parity to old parity (2) cancel part of the old parity
        Coding::bitwiseXor(segbuf + i * chunkSize, chunks[i].buf, 
                readbuf + (i-k) * chunkSize, chunkSize);
        Coding::bitwiseXor(segbuf + i * chunkSize, oldChunks[i].buf, 
                segbuf + i * chunkSize, chunkSize);
    }

    // TODO : parallel write to data segment
    writePartialSegment(dataSegment, segbuf, offLens, ts);
    free(readbuf);
}

// Varient of the original verison of readSegment(), mainly on the read using raidSetting[LOG]
void RaidMod::readLogSegment(SegmentMetaData* segmentMetaData, char* buf, off_len_t offLen, LL ts) {
    assert(segmentMetaData->locations.size() > 0);
    // obtain chunk status
    vector<disk_id_t> diskIds;
    for (SegmentLocation location : segmentMetaData->locations) {
        diskIds.push_back(location.first);
    }
    vector<bool> chunkStatus = m_diskMod->getDiskStatus(diskIds);

    // read reqSymbols into chunk list
    vector<ChunkSymbols> reqSymbols = m_coding[LOG]->getReqSymbols(chunkStatus,
            m_raidSetting[LOG].codeSetting, offLen);

    vector<Chunk> chunks;

	std::atomic_int rcnt;
	rcnt = 0;

    for (ChunkSymbols chunkSymbols : reqSymbols) {
        chunk_id_t chunkId = chunkSymbols.chunkId;
        SegmentLocation location = segmentMetaData->locations[chunkId];

        // read chunk out (packed) according to chunkSymbols
        Chunk chunk;
        chunk.chunkId = chunkId;
        chunk.length = getChunkLength(chunkSymbols.offLens);
        chunk.buf = (char*) buf_calloc(chunk.length, sizeof(char));
        debug("chunk %d on disk %d at %d [%c]\n", chunkId, location.first, 
                location.second, chunk.buf[0]);

        if (location.first != INVALID_DISK) {
            //readChunkSymbols(location.first, location.second, chunk.buf,
            //        chunkSymbols.offLens, ts);
			rcnt++;
            m_rtp.schedule(boost::bind(&RaidMod::readChunkSymbols, this,
                    location.first, location.second, chunk.buf, 
                    chunkSymbols.offLens, boost::ref(rcnt), ts));
        }

        chunks.push_back(chunk);
    }

	while (rcnt > 0);

    m_coding[LOG]->decode(reqSymbols, chunks, buf, m_raidSetting[LOG].codeSetting, offLen);
    
    for (Chunk chunk : chunks) {
        free(chunk.buf);
    }
}

#endif
void RaidMod::readChunkSymbols(disk_id_t diskId, lba_t lba, char* buf,
        vector<off_len_t> offLens, std::atomic_int &rcnt, LL ts) {
    readChunkSymbols(diskId, lba, buf, offLens, ts);
    rcnt--;
}

void RaidMod::readChunkSymbols(disk_id_t diskId, lba_t lba, char* buf,
        vector<off_len_t> offLens, LL ts) {
    int len = 0;
    int blockSize = ConfigMod::getInstance().getBlockSize();
    for (off_len_t offLen : offLens) {
        int end = offLen.first + offLen.second;
        chunk_id_t startBlock = offLen.first / blockSize;
        chunk_id_t endBlock = (end - 1) / blockSize;
        int numPages = 0;
        for (int i = startBlock; i <= endBlock; i++) {
            int inBlockOff = max(offLen.first - i * blockSize, 0);
            int inBlockLen = min(end, blockSize) - inBlockOff;
            off_len_t inBlockOffLen (inBlockOff, inBlockLen);
            numPages += m_diskMod->readBlock(diskId, lba, buf + len + i * blockSize, inBlockOffLen, ts);
        }
        debug ("Read %d pages\n", numPages);
        len += offLen.second;
    }
}

int RaidMod::getChunkLength(vector<off_len_t> offLen) const {
    int len = 0;
    for (auto ol : offLen) {
        len += ol.second;
    }
    return len;
}
