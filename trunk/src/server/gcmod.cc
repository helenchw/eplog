#include "common/lowlockqueue.hh"
#include "common/struct/segmentmetadata.hh"
#include "gcmod.hh"

GCMod::GCMod(SegmentMetaDataMod* segMetaMod,
        StorageMod* storageMod, LogMod* logMod, RaidMod* raidMod,
        DiskMod* diskMod, LowLockQueue<SegmentMetaData*>* gcQueue) :
    m_segMetaMod(segMetaMod),
    m_storageMod(storageMod),
    m_logMod(logMod),
    m_raidMod(raidMod),
    m_diskMod(diskMod),
    m_totGC(0),
    m_gcAliveLowerBound(ConfigMod::getInstance().getGCAliveLowerBound()),
    m_segmentSize(ConfigMod::getInstance().getSegmentSize()),
    m_pageSize(ConfigMod::getInstance().getPageSize()),
    m_gcQueue(gcQueue) {
        // empty in constructor
        m_finishQueue = new vector< pair<int, SegmentMetaData*> >;
    }

GCMod::~GCMod() {
}

void GCMod::setLowerBound(int x) {
    m_gcAliveLowerBound = x;
}

int GCMod::getLowerBound() {
    return m_gcAliveLowerBound;
}

void GCMod::start(int interval) {
    SegmentMetaData* smd;
    while (1) {
        while (m_gcQueue->pop(smd) != false) {
            if (!doGC(smd)) {   // GC not performed
                m_gcQueue->push(smd);
            }
            usleep(interval);
        }
    }
}

bool cmp(const pair<int, SegmentMetaData*>& a, const pair<int, SegmentMetaData*>& b) {
    /* seg. 'a' has a smaller total bytes alive || (both seg. have same total bytes alive
     * && SegmentMetadata of 'a' is "larger" than that of 'b')
     */
    return a.first < b.first || (a.first == b.first && a.second > b.second);
}

void GCMod::doGCTimes(LL ts, int times) {
    /* reconstruct the GC segment queue */
    m_finishQueue->clear();
    m_finishQueue->reserve(m_segMetaMod->m_metaMap.size());
    for(auto entry: m_segMetaMod->m_metaMap) {
        if (entry.second->locations.size() == 0) continue;
        int totalAliveBytes = 0;
        /* obtain the amount of alive bytes by scanning segment bitmap */
        vector<off_len_t> aliveBytes = entry.second->bitmap.getAllOne();
        for (off_len_t& offLen : aliveBytes) {
            offLen.first *= m_pageSize;
            offLen.second *= m_pageSize;
            totalAliveBytes += offLen.second;
        }
        m_finishQueue->push_back(make_pair(totalAliveBytes, entry.second));
    }
    //sort(m_finishQueue->begin(), m_finishQueue->end(), cmp);
    int i = 0;
    for (auto smd: *m_finishQueue) {
        doGC(smd.second, ts, false);
        i++;
        if (i == times && times != 0) break;
    }
}

void GCMod::finish2(LL ts, double bound) {
    int cnt = 0;
    double rem;
    pair<int, SegmentMetaData*> target;
    do {
        target = m_segMetaMod->getminHeap();
        if (target.first == -1) break;
        cnt++;
        doGC(target.second, ts, false);
        rem = m_diskMod->getRemainingSpace();
    //} while (cnt < 10 || target.first == 0);
    } while (rem < bound);
}

void GCMod::finish(LL ts, double bound) {
    m_finishQueue->clear();
    m_finishQueue->reserve(m_segMetaMod->m_metaMap.size());
    for(auto entry: m_segMetaMod->m_metaMap) {
        if (entry.second->locations.size() == 0) continue;
        int totalAliveBytes = 0;
        vector<off_len_t> aliveBytes = entry.second->bitmap.getAllOne();
        for (off_len_t& offLen : aliveBytes) {
            offLen.first *= m_pageSize;
            offLen.second *= m_pageSize;
            totalAliveBytes += offLen.second;
        }
        m_finishQueue->push_back(make_pair(totalAliveBytes, entry.second));
    }
    sort(m_finishQueue->begin(), m_finishQueue->end(), cmp);
    fprintf (stderr, "queue size = %zu\n", m_finishQueue->size());
    for (auto smd: *m_finishQueue) {
        doGC(smd.second, ts, false);
        double rem = m_diskMod->getRemainingSpace();
        if (rem >= bound) break;
    }
}


// this is stale ??
bool GCMod::doGConce(LL ts) {
    bool doneGC = false;
    SegmentMetaData *smd, *same = nullptr;
    while (m_gcQueue->pop(smd) != false) {
        if (m_gcAliveLowerBound == 160) fprintf (stderr, "DO GC FOR SMD=%d\n", smd->segmentId);
        if (smd == same) return false;
        if (!doGC(smd, ts)) {   // GC not performed
            if (same == nullptr) same = smd;
            m_gcQueue->push(smd);
		} else {
			doneGC = true;
			break;
		}
    }
    return doneGC;
}

// return true if GC is performed
bool GCMod::doGC(SegmentMetaData* smd, LL ts, bool useLowerbound) {

    // init segment buf as null, only need to readSegment once
    int segmentId = smd->segmentId;
    char* buf = nullptr;

    for (FileMetaData* fmd : smd->fileSet) {
        fmd->fileMutex.lock();
    }

    // TODO: using byte for bitmap, may use page instead
    int totalAliveBytes = 0;
    vector<off_len_t> aliveBytes = smd->bitmap.getAllOne();
    for (off_len_t& offLen : aliveBytes) {
        offLen.first *= m_pageSize;
        offLen.second *= m_pageSize;
        totalAliveBytes += offLen.second;
    }


    // TODO: simple heuristics, may use LFS cost-benefit schemes instead
    if (useLowerbound) {
        if ((double) totalAliveBytes / m_segmentSize * 100 > m_gcAliveLowerBound) 
        {
            // not enough stale bytes to do GC
            // unlock each file
            for (FileMetaData* fmd: smd->fileSet) {
                fmd->fileMutex.unlock();
            }
            debug ("Finished GC for segment %d without reclaim\n", segmentId);
            return false;
        }
    }

    if (totalAliveBytes == m_segmentSize) {
        // not enough stale bytes to do GC
        // unlock each file
        for (FileMetaData* fmd: smd->fileSet) {
            fmd->fileMutex.unlock();
        }
        debug ("Finished GC for segment %d without reclaim\n", segmentId);
        return false;
    }

    // do GC, read whole segment
    if (buf == nullptr) {
        buf = (char*) malloc(m_segmentSize);
        m_raidMod->readSegment(smd, buf, make_pair(0, m_segmentSize), ts);
    }

    long long roundBytes = 0;

    for (off_len_t offLen : aliveBytes) {
        debug ("Processing Segment %d, Offset = %d, Length = %d\n", smd->segmentId, offLen.first, offLen.second);
        OffFmdIt lit, rit;
        tie (lit, rit) = smd->findCover (offLen.first, offLen.second);

        int curOff = offLen.first;
        for (OffFmdIt it = lit; it != rit; it++) {
            debug ("Covering file %d, curOff = %d, it->first = %d\n", it->second->fileId, curOff, it->first);
            assert (curOff >= it->first);
            FileMetaData *fmd = it->second;

            // find no. of bytes to write
            int writeLen = offLen.second;
            if (it + 1 != smd->lookupVector.end()) { // find right boundary
                writeLen = min (writeLen, (it + 1)->first - curOff);
            }

            // write back to log
            const int level = 0;

            assert (smd->fileOffsetMap.count(it->first));
            const LL fileOff = smd->fileOffsetMap[it->first] + (curOff - it->first);
            m_logMod->writeLog(buf + curOff, level, fmd, make_pair (fileOff, writeLen), true, ts);
            roundBytes += writeLen;

            curOff += writeLen;
            offLen.second -= writeLen;
        }
    }

    m_totGC += roundBytes;
    //printf("%lld\t%lld\t%lld\n", ts, roundBytes, m_totGC);

    for (SegmentLocation location : smd->locations) {
        m_diskMod->setLBAFree(location.first, location.second);
    }

    // unlock each file
    for (FileMetaData* fmd: smd->fileSet) {
        fmd->fileMutex.unlock();
    }

    debug ("Finished GC for segment %d with reclaim\n", segmentId);
    delete smd;
    m_segMetaMod->removeEntry(segmentId);
    free (buf);
    return true;
}
