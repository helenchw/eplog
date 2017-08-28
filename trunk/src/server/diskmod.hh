#ifndef DISKMOD_HH_
#define DISKMOD_HH_

#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <threadpool.hpp>
#include "server/bitmap.hh"
#include "common/struct/diskinfo.hh"
#include "common/define.hh"

using namespace std;

/**
 * Module for disk array management
 */
class DiskMod {
public:
    /**
     * Constructor
     * \param v_DiskInfo the list of disk information 
     */
    DiskMod(const vector<DiskInfo> v_DiskInfo);

    /**
     * Destructor
     */
    virtual ~DiskMod();

    /**
     * Write a new block to a disk
     * \param diskId disk id of the disk
     * \param buf buffer containing the data to write
     * \param ts timestamp
     * \return the address of the block written
     */
    int writeBlock (disk_id_t diskId, const char* buf, LL ts = 0);

    /**
     * Write a new block to a disk (for multi-thread)
     * \param diskId the disk id of the disk
     * \param buf buffer containing the data to write
     * \param lba new address of the block
     * \param cnt thread counter to update (decrement)
     * \param ts timestamp
     */
    void writeBlock_mt (disk_id_t diskId, const char* buf, lba_t& lba, std::atomic_int& cnt, LL ts = 0);

    /**
     * Write blocks to a disk from a designated address
     * \param diskId the disk id of the disk
     * \param lba starting address to write/overwrite
     * \param len number of blocks to write/overwrite
     * \param buf buffer containing the data to write
     * \param ts timestamp
     * \return the starting address of the block written
     */
    int writeBlock (disk_id_t diskId, lba_t lba, int len, void* buf, LL ts = 0);

    /**
     * Write blocks to a disk from a designated address (for multi-thread)
     * \param diskId the disk id of the disk
     * \param buf buffer containing the data
     * \param lba starting address to write/overwrite
     * \param len number of blocks to write/overwrite
     * \param cnt thread counter to update (decrement)
     * \param ts timestamp
     */
    void writeBlockToLba_mt (disk_id_t diskId, const char* buf, lba_t lba, int len, std::atomic_int& cnt, LL ts = 0);

    /**
     * Write a sequence of blocks to a disk from a designated address (for multi-thread)
     * \param diskId the disk id of the disk
     * \param buf buffer containing the data
     * \param lba starting address to write/overwrite
     * \param len number of blocks to write/overwrite
     * \param cnt thread counter to update (decrement)
     * \param ts timestamp
     */
    void writeSeqBlocks_mt (disk_id_t diskId, const char* buf, int len, lba_t &lba, std::atomic_int& cnt, LL ts = 0);

    /**
     * Write multiple new blocks to a disk 
     * \param diskId the disk id of the disk
     * \param buf buffer containing the data to write
     * \param ts timestamp
     * \return the starting address of the sequence of blocks
     */
    int writeBlocks (disk_id_t diskId, const char* buf, int len, LL ts = 0);

    /**
     * Write multiple new blocks to a disk (for multi-thread)
     * \param diskId the disk id of the disk
     * \param buf buffer containing the data to write
     * \param len number of blocks to write
     * \param lbaV new addresse of the blocks 
     * \param cnt thread counter to update (decrement)
     * \param ts timestamp
     */
    void writeBlocks_mt (disk_id_t diskId, const char* buf, int len, vector<lba_t>& lbaV, std::atomic_int& cnt, LL ts = 0);

    /**
     * Write (part of) a block to a disk
     * \param diskId disk id of the disk
     * \param lba address to write/overwrite
     * \param diskOffset exact disk offset where the write starts
     * \param len length of data (within the block)
     * \param buf buffer containing the data to write
     * \param ts timestamp
     * \return the address of the block written
     */
    int writePartialBlock (const disk_id_t diskId, const lba_t lba, ULL diskOffset, 
            int len, const char* buf, LL ts = 0);
    /**
     * Write a block to a disk (for multi-thread)
     * \param diskId disk id of the disk
     * \param lba address to write/overwrite
     * \param diskOffset texact disk offset where the write starts
     * \param len length of data (within the block)
     * \param buf buffer containing the data to write
     * \param cnt thread counter to update (decrement)
     * \param ts timestamp
     * \return address of the block written
     */
    int writePartialBlock_mt (const disk_id_t diskId, const lba_t lba, ULL diskOffset, 
            int len, const char* buf, std::atomic_int& cnt, LL ts = 0);

    /**
     * Read a block from a disk
     * \param diskId disk id of the disk
     * \param lba address to read
     * \param buf buffer receiving the data read
     * \param blockOffLen offset and length of data to read within a block
     * \param ts timestamp
     * \return number of pages read
     */
    int readBlock (disk_id_t diskId, lba_t lba, char* buf, off_len_t blockOffLen, LL ts = 0);

    /**
     * Read a block from a disk
     * \param diskId disk id of the disk
     * \param lba address to read
     * \param buf buffer receiving the data read
     * \param blockOffLen offset and length of data to read within a block
     * \param ts timestamp
     * \return number of pages read
     */
    int readBlock (disk_id_t diskId, lba_t lba, void* buf, off_len_t blockOffLen, LL ts = 0);

    /**
     * readBlocks_mt
     * Read multiple sequences of blocks from a disk (for multi-thread)
     * \param diskId the disk id of the disk to read
     * \param LBASet the set of address of blocks to read
     * \param buf the buffer receiving the data read
     * \param cnt the thread counter to update (decrement)
     * \param ts timestamp
     */
    void readBlocks_mt (disk_id_t diskId, set<lba_t> &LBASet, char* buf, 
            std::atomic_int& cnt, LL ts = 0);

    /**
     * Read a sequence of blocks from a disk (for multi-thread)
     * \param diskId disk id of the disk
     * \param startLBA starting address of blocks
     * \param endLBA ending address of blocks
     * \param buf buffer receiving the data read
     * \param cnt thread counter to update (decrement)
     * \param ts timestamp
     */
    void readBlocks_mt (disk_id_t diskId, lba_t startLBA, lba_t endLBA, char* buf, 
            std::atomic_int& cnt, LL ts = 0);
    void readBlocks_mt (disk_id_t diskId, lba_t startLBA, lba_t endLBA, char* buf, 
            int& ready, std::mutex& lock, LL ts = 0);

    /**
     * Read a sequence of blocks from a disk 
     * \param diskId disk id of the disk
     * \param startLBA starting address of blocks
     * \param endLBA ending address of blocks
     * \param buf buffer receiving the data read
     * \param ts timestamp
     * \return number of pages read
     */
    int readBlocks (disk_id_t diskId, lba_t startLBA, lba_t endLBA, char* buf, LL ts = 0);

    /**
     * Free an lba on a disk
     * \param diskId disk id of the disk to free LBA
     * \param lba address to free
     * \return address freed
     */
    int setLBAFree (disk_id_t diskId, lba_t lba);

    /**
     * Free a continuous list of LBAs on a disk
     * \param diskId disk id of the disk
     * \param lba starting address to free (inclusive)
     * \param n number of address to free
     * \return starting address freed
     */
    int setLBAsFree (disk_id_t diskId, lba_t startLBA, int n);

    /**
     * Print all the disks managed
     */
    void listDisks() const;

    /**
     * Return the list ids of data disks
     * \return list of data disk id in ascending order
     */
    vector<disk_id_t> getDataDiskIds(void) const;

    /**
     * Get the number of disks of a type
     * \param type type of disks to count
     * \return number of disks of the type specified
     */
    int getNumDisks(int type = -1) const;

    /**
     * Select a number of n disks
     * \param n number of disks required
     * \param hint hint for disk selection
     * \return list of disk selected
     */
    vector<disk_id_t> selectDisks(int n, unsigned int hint = -1) const;

    /**
     * Select a number of n disks
     * \param n number of disks required
     * \param logDisks whether only log disks should be chose
     * \param hint hint for disk selection (Deprecated)
     * \param filter set of disks that should not be chose
     * \return list of disk selected
     */
    vector<disk_id_t> selectDisks(int n, bool logDisks = false, 
            unsigned int hint = -1, 
            set<disk_id_t> filter = set<disk_id_t>()) const;

    /**
     * Get the alive/offline status of disks
     * \param diskIds id of the disks for enquiry
     * \return list of status corresponding to the list of disks selected
     */
    vector<bool> getDiskStatus(const vector<disk_id_t> diskIds);

    /**
     * Set the alive/offline status of a disk
     * \param diskId id of the disk
     * \param value status of the disk
     */
    void setDiskStatus (disk_id_t diskId, bool value);

    /**
     * Get the set of id of offline disks
     * \return set of id of offline disks
     */
    set<disk_id_t> getFailDisks();

    /**
     * Issue fsync to all disks
     * \param logDisks whether log disks are also sync (by default)
     */
    void fsyncDisks(bool logDisks = true) ;

    /**
     * Issue fsync to a disk
     * \param diskId disk id of the disk
     */
    void fsyncDisk(disk_id_t diskId) ;

    /**
     * Obtain the remaining storage capacity of the array
     * \param type type of storage (data/log/all)
     * \return percentage of remaining capacity 
     */
    double getRemainingSpace(int type = -1) const;

    /**
     * Reset the disks write front
     * \return whether the operation is successful (always success)
     */
    bool resetDisksWriteFront(void);

    /**
     * Print the next address for searching a new block for write of each disk
     */
    void printDisksWriteFront(void);

private:
    /** Mapping of disk id and disk infomation */
    unordered_map<disk_id_t, DiskInfo> m_diskInfo;   

    /** Orderred list of data disk id **/
    vector<disk_id_t> m_dataDiskIdVector;

    /** Mapping of disk id and address bitmap */
    nv_unordered_map<disk_id_t, BitMap*> m_usedLBA;

    /** Mapping of disk id and disk lock */
    unordered_map<disk_id_t, mutex*> m_diskMutex;

    /** Mapping of disk id and disk status */
    unordered_map<disk_id_t, bool> m_diskStatus;

    /** Avoid race conditions for failure simulations **/
    mutex m_diskStatusMutex;

    int m_numDisks;                   /**< Number of all disks */
    int m_numLogDisks;                /**< Number of log disks */
    int m_numDataDisks;               /**< Number of data disks */
    int m_logZoneSize;                /**< Number of chunks in each log zone */
    int m_segmentPerLogZone;          /**< Number of segments in each log zone */

    boost::threadpool::pool m_stp;    /**< Thread pool for parallel I/O */

#ifdef DISKLBA_OUT
    FILE* fp;                         /**< File pointer for LBA printing */
#endif

};

#endif /* DISKMOD_HH_ */
