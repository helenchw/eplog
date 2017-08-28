#include <thread>
#include "configmod.hh"


void ConfigMod::setConfigPath (const char* path) {
    boost::property_tree::ini_parser::read_ini(path, m_pt);
    assert (!m_pt.empty());

    m_pageSize = readInt("ssd.pageSize");
    m_numPagePerBlock = readInt("ssd.numPagePerBlock");
    m_numBlockPerSegment = readInt("coding.numBlockPerSegment");
    m_numBlockPerLogSegment = readInt("coding.numBlockPerLogSegment");

    m_blockSize = m_pageSize * m_numPagePerBlock;
    m_segmentSize = m_blockSize * m_numBlockPerSegment;
    m_logSegmentSize = m_blockSize * m_numBlockPerLogSegment;

    m_diskLogMode = readInt("update.logMode");
    m_logLevel = readInt("log.logLevel");
    m_updateLogLevel = readInt("log.updateLogLevel");
    m_logZoneSize = readInt("log.logZoneSize");
    m_logZoneSegments = readInt("log.logZoneSegments");
    m_chunkGroupSize = readInt("log.chunkGroupSize");

    m_gcAliveLowerBound = readInt("gc.aliveLowerBound");

    m_logTh = readInt("sync.logThreshold");
    m_dataTh = readInt("sync.dataThreshold");
    m_isDataInplace = readBool("sync.dataInplace");

    m_numThread = std::thread::hardware_concurrency();
    if (m_numThread <= 0) { m_numThread = NUM_THREAD; }

    m_recoveryBatchSize = readInt("recovery.batchSize");
    if (m_recoveryBatchSize <= 0) { m_recoveryBatchSize = 100; }

    m_keyCacheSize = readInt("keyvalue.keyCacheSize");
    m_cacheAllKeys = readBool("keyvalue.cacheAllKeys");
}

bool ConfigMod::readBool (const char* key) {
    return m_pt.get<bool>(key);
}

int ConfigMod::readInt (const char* key) {
    return m_pt.get<int>(key);
}

double ConfigMod::readFloat (const char* key) {
    return m_pt.get<double>(key);
}

string ConfigMod::readString (const char* key) {
    return m_pt.get<string>(key);
}

ULL ConfigMod::readULL (const char* key) {
    return m_pt.get<ULL>(key);
}


int ConfigMod::getPageSize() const {
    assert (!m_pt.empty());
    return m_pageSize;
}

int ConfigMod::getNumPagePerBlock() const {
    assert (!m_pt.empty());
    return m_numPagePerBlock;
}

int ConfigMod::getBlockSize() const {
    assert (!m_pt.empty());
    return m_blockSize;
}

int ConfigMod::getNumBlockPerSegment() const {
    assert (!m_pt.empty());
    return m_numBlockPerSegment;
}

int ConfigMod::getNumBlockPerLogSegment() const {
    assert (!m_pt.empty());
    return m_numBlockPerLogSegment;
}

int ConfigMod::getSegmentSize() const {
    assert (!m_pt.empty());
    return m_segmentSize;
}

int ConfigMod::getLogSegmentSize() const {
    assert (!m_pt.empty());
    return m_logSegmentSize;
}

int ConfigMod::getLogLevel() const {
    assert (!m_pt.empty());
    return m_logLevel;
}

int ConfigMod::getUpdateLogLevel() const {
    assert (!m_pt.empty());
    return m_updateLogLevel;
}

int ConfigMod::getLogZoneSize() const {
    assert(!m_pt.empty());
    return m_logZoneSize;
}

int ConfigMod::getLogZoneSegments() const {
    assert(!m_pt.empty());
    return m_logZoneSegments;
}

int ConfigMod::getChunkGroupSize() const {
    assert(!m_pt.empty());
    return m_chunkGroupSize;
}

int ConfigMod::getGCAliveLowerBound() const {
    assert (!m_pt.empty());
    return m_gcAliveLowerBound;
}

int ConfigMod::getDiskLogMode() const {
    assert (!m_pt.empty());
    return m_diskLogMode;
}

int ConfigMod::getDataTh() const {
    assert (!m_pt.empty());
    return m_dataTh;
}

int ConfigMod::getLogTh() const {
    assert (!m_pt.empty());
    return m_logTh;
}

int ConfigMod::getNumThread() const {
    assert(!m_pt.empty());
    return m_numThread;
}

int ConfigMod::getRecoveryBatchSize() const {
    assert(!m_pt.empty());
    return m_recoveryBatchSize;
}

bool ConfigMod::isCacheAllKeys() const {
    assert(!m_pt.empty());
    return m_cacheAllKeys;
}

size_t ConfigMod::getKeyCacheSize() const {
    assert(!m_pt.empty());
    return m_keyCacheSize;
}
