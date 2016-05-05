#ifndef GCMOD_HH_
#define GCMOD_HH_

#include <list>
#include "common/struct/segmentmetadata.hh"
#include "server/segmetamod.hh"
#include "server/logmod.hh"
#include "server/raidmod.hh"
#include "server/storagemod.hh"
#include "common/define.hh"

/**
 * Module for garbage collection
 */
class GCMod {
public:
    /**
     * Constructor
     * \parm segMetaMod SegmentMetaDataMod
     * \parm storageMod StorageMod
     * \parm logMod LogMod
     * \parm raidMod RaidMod
     * \parm diskMod DiskMod
     * \parm gcQueue GC queue for segments
     */
    GCMod(SegmentMetaDataMod* segMetaMod, StorageMod* storageMod, LogMod* logMod, 
        RaidMod* raidMod, DiskMod* diskMod, LowLockQueue<SegmentMetaData*>* gcQueue);

    /**
     * Destructor
     */
    virtual ~GCMod();

    /**
     * Periodic GC
     * \param interval intervals between GC in micro-seconds (us)
     */
    void start(int interval);

    /**
     * Determine if the current GC operation should continue
     * \param ts timestamp
     * \param bound percentage of storage used
     */
    void finish(LL ts, double bound = 0);

    /**
     * Determine if the current GC operation should continue
     * \param ts timestamp
     * \param bound percentage of storage used
     */
    void finish2(LL ts, double bound = 0);

    /**
     * Determine if the current GC operation should continue
     * \param ts timestamp
     * \param times max. of iterations, i.e., max. number of segments to clean
     */
    void doGCTimes(LL ts, int times);

    /**
     * Determine if the current GC operation should continue
     * \param ts timestamp
     */
    bool doGConce(LL ts);

    /**
     * Return the max. amount of alive data in a segment before GC can be performed
     * \return max. amount of alive data in a segment in percentage
     */
    int getLowerBound();

    /**
     * Set the max. amount of alive data in a segment before GC can be performed
     * \param x max. amount of alive data in a segment in percentage
     */
    void setLowerBound(int x);

private:
    /**
     * Perform GC on a segment
     * \param smd segment metadata
     * \param ts timestamp
     * \param useLB whether the lower bound of alive data should be imposed 
     * \return whether GC is performed on the segment
     */
    bool doGC(SegmentMetaData* smd, LL ts = 0, bool useLB = true);

    SegmentMetaDataMod* const m_segMetaMod;                   /**< Pointer to SegmentMetaDataMod */
    StorageMod* const m_storageMod;                           /**< Pointer to StroageMod */
    LogMod* const m_logMod;                                   /**< Pointer to LogMod */
    RaidMod* const m_raidMod;                                 /**< Pointer to RaidMod */
    DiskMod* const m_diskMod;                                 /**< Pointer to DiskMod */
    LL m_totGC;                                               /**< Total number of bytes GCed */

    int m_gcAliveLowerBound;                                  /**< Max. amount of alive data in a segment in percentage */
    const int m_segmentSize;                                  /**< Size of segments */
    const int m_pageSize;                                     /**< Size of pages */

    vector<pair<int,SegmentMetaData*>>* m_finishQueue;        /**< List of recently GCed segments */
    LowLockQueue<SegmentMetaData*>* const m_gcQueue;          /**< List of candidate segments for GC */

};

#endif /* GCMOD_HH_ */
