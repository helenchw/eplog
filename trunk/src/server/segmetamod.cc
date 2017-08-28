#include "server/segmetamod.hh"
#include "common/configmod.hh"

#include <unistd.h>
#include <stdexcept>
#include "common/debug.hh"

SegmentMetaDataMod::SegmentMetaDataMod() {
    m_segmentIdCounter = 0;
    m_logSegmentIdCounter = MAX_SEGMENTS;
    m_segmentSize = ConfigMod::getInstance().getSegmentSize();
    heap = (int*)malloc(sizeof(int)*MAX_SEGMENTS);
    ps = (int*)malloc(sizeof(sid_t)*MAX_SEGMENTS);
    value = (int*)malloc(sizeof(int)*MAX_SEGMENTS);
    memset(ps, 0, sizeof(int)*MAX_SEGMENTS);
    memset(value, 0, sizeof(int)*MAX_SEGMENTS);
    debug ("ps %p heap %p\n", ps, heap);
    heaplen = 0;
}

SegmentMetaDataMod::~SegmentMetaDataMod() {
    free(heap);
    free(ps);
    free(value);
}

// TODO: NOT Thread-safe
int SegmentMetaDataMod::updateHeap(sid_t sid, int delv) {
    if (ps[sid] == -1) return -1; // Logmod called in GC should not update heap
    if (m_metaMap[sid]->locations.size() == 0) { // segment still in ram
        value[sid] += delv;
        return value[sid];
    }
    if (ps[sid] == 0) { // first time added
        //fprintf (stderr, "insert heap %d %d %d\n", r, delv, value[r]);
        heap[++heaplen] = sid;
        ps[sid] = heaplen;
        value[sid] += (delv == 0)? m_segmentSize: 0;
        debug("heap add SID:%d at pos %d\n", sid, ps[sid]);
    }
    value[sid] += delv;
    //fprintf (stderr, "update heap %d %d %d\n", r, delv, value[r]);
    sid_t q = ps[sid], p = q >> 1; /* q indicates the latest item, p is the mid one */
    while (p && value[heap[p]] > value[sid]) { 
        ps[heap[p]] = q; heap[q] = heap[p];
        q = p; p = q >> 1;
    } /* keep swapping until heap is in order */
    heap[q] = sid; ps[sid] = q; 
    //printHeap();
    return value[sid];
}

// TODO: NOT Thread-safe
pair<sid_t,SegmentMetaData*> SegmentMetaDataMod::getminHeap() {
    if (heaplen == 0) {
        return make_pair(-1, nullptr);
    }
    int ret = heap[1], p = 1, q = 2, r = heap[heaplen--];
    while (q <= heaplen) {
        if (q < heaplen && value[heap[q+1]] < value[heap[q]]) q++;
        if (value[heap[q]] < value[r]) {
            ps[heap[q]] = p; heap[p] = heap[q];
            p = q; q = p << 1;
        } else break; /* keep sorting the heap until min is found */
    }
    heap[p] = r; ps[r] = p;
    ps[ret] = -1;
    //fprintf (stderr, "get min heap %d\n", ret);
    return make_pair(value[ret], m_metaMap[ret]);
}

void SegmentMetaDataMod::printHeap() {
    for (int i = 1; i <= heaplen; i++)
        fprintf (stderr,"heap[%d] = segment %d, value = %d, ps = %d\n", i, heap[i], value[heap[i]], ps[i]);
}

SegmentMetaData* const SegmentMetaDataMod::createEntry(bool isUpdateLog, sid_t sid) {
    m_metaOptMutex.lock();
    
    int segSize = (isUpdateLog)?ConfigMod::getInstance().getLogSegmentSize():
            ConfigMod::getInstance().getSegmentSize();
    int pageSize = ConfigMod::getInstance().getPageSize();

    if (sid != INVALID_SEG && m_metaMap.count(sid)) { 
        return m_metaMap[sid]; 
    }

    SegmentMetaData* smd = new SegmentMetaData(segSize/pageSize);
    sid_t segmentId = -1;
    if (isUpdateLog) {
        do {
            segmentId = --m_logSegmentIdCounter;
        } while (m_metaMap.count(segmentId));
    } else if (sid != INVALID_SEG && !m_metaMap.count(sid)) {
        m_segmentIdCounter = (sid > m_segmentIdCounter)? sid:m_segmentIdCounter;
        segmentId = sid;
    } else {
        segmentId = ++m_segmentIdCounter;
    }
    assert(!m_metaMap.count(segmentId));
    assert(m_logSegmentIdCounter > m_segmentIdCounter);
    m_metaMap[segmentId] = smd;

    // Some initialization
    smd->segmentId = segmentId;
    smd->locations.clear();
    smd->fileSet.clear();
    smd->bitmap.setAllOne(); // 1 means alive data
    // hard-code 1 chunk = 1 block
    int numBlock = (isUpdateLog)? ConfigMod::getInstance().getNumBlockPerLogSegment(): 
            ConfigMod::getInstance().getNumBlockPerSegment();
    smd->curLogId.reserve(numBlock);
    for (int i = 0; i < numBlock; i++) {
        smd->curLogId[i] = INVALID_LOG_SEG_ID;
    }
    smd->isUpdateLog = isUpdateLog;

    m_metaOptMutex.unlock();
    return smd;
}

SegmentMetaData* const SegmentMetaDataMod::getEntry(sid_t segmentId) {
    m_metaOptMutex.lock_shared();
    SegmentMetaData* ret = nullptr;
    try {
        ret = m_metaMap.at(segmentId);
    } catch (const std::out_of_range& oor) {
        debug_error ("No element called %d is found in metadata\n", segmentId);
        exit(0);
    }
    m_metaOptMutex.unlock_shared();
    return ret;
}

bool SegmentMetaDataMod::existEntry(sid_t segmentId) {
    m_metaOptMutex.lock_shared();
    bool ret = m_metaMap.count(segmentId);
    m_metaOptMutex.unlock_shared();
    return ret;
}

bool SegmentMetaDataMod::removeEntry(sid_t segmentId) {
    m_metaOptMutex.lock();
    bool ret = false;
    try {
        m_metaMap.erase(segmentId);
        ret = true;
    } catch (const std::out_of_range& oor) {
        debug ("No element called %d is found in metadata\n", segmentId);
        ret = false;
    }
    m_metaOptMutex.unlock();

    return ret;
}

sid_t SegmentMetaDataMod::probeNextSegmentId() {
    return m_segmentIdCounter+1;
}

void SegmentMetaDataMod::resetLogSegmentId() {
    m_logSegmentIdCounter = MAX_SEGMENTS;
}
