#ifndef CONFIGMOD_HH_
#define CONFIGMOD_HH_

#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "common/define.hh"

using namespace std;

/**
 * Module of configurations
 */

class ConfigMod {
public:

    static ConfigMod& getInstance() {
        static ConfigMod instance; // Guaranteed to be destroyed
        // Instantiated on first use
        return instance;
    }

    void setConfigPath (const char* key);

    int getPageSize() const;
    int getNumPagePerBlock() const;
    int getBlockSize() const;
    int getNumBlockPerSegment() const;
    int getNumBlockPerLogSegment() const;
    int getSegmentSize() const;
    int getLogSegmentSize() const;
    int getLogLevel() const;
    int getUpdateLogLevel() const;
    int getLogZoneSize() const;
    int getLogZoneSegments() const;
    int getChunkGroupSize() const;
    int getGCAliveLowerBound() const;
    int getDiskLogMode() const;
    int getLogTh() const;
    int getDataTh() const;
    int getNumThread() const;

private:
    ConfigMod() {}
    ConfigMod(ConfigMod const&);            // Don't Implement
    void operator=(ConfigMod const&);       // Don't implement
    
    int readInt (const char* key);
    double readFloat(const char* key);
    ULL readULL (const char* key);
    string readString (const char* key);

    boost::property_tree::ptree m_pt;

    int m_pageSize;                         /**< Size of a page in bytes */
    int m_numPagePerBlock;                  /**< Number of pages per block */
    int m_blockSize;                        /**< Size of a block in bytes */
    int m_numBlockPerSegment;               /**< Number of blocks per segment */
    int m_segmentSize;                      /**< Segment size */

    int m_numBlockPerLogSegment;            /**< Number of blocks per log segment */
    int m_logSegmentSize;                   /**< Size of a log segment */
    int m_logLevel;                         /**< New write buffer level */
    int m_updateLogLevel;                   /**< Update buffer level */
    int m_logZoneSize;                      /**< Size of each log zone */
    int m_logZoneSegments;                  /**< Number of segments in each log zone */
    int m_chunkGroupSize;                   /**< Number of chunk per group for flush */

    int m_gcAliveLowerBound;                /**< Max. percentage of alive page in a GC victim block */
    int m_diskLogMode;                      /**< Disk (HDD) logging mode */

    int m_logTh;                            /**< HDD log threshold for triggering sync */
    int m_dataTh;                           /**< Data log threshold for triggering sync */

    int m_numThread;                        /**< Number of threads supported by CPU */
};

#endif /* CONFIGMOD_HH_ */
