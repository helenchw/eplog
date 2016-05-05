#ifndef STORAGEMOD_HH_
#define STORAGEMOD_HH_

#include "server/filemetamod.hh"
#include "server/raidmod.hh"
#include "server/logmod.hh"
#include "server/permetamod.hh"
#include "common/define.hh"

/**
 * Module for managing file-based/block-based user writes and reads
 */

class StorageMod {
public:
    /**
     * Original Constructor (Deprecated)
     */
    //StorageMod(SegmentMetaDataMod* segMetaMod, FileMetaDataMod* fileMetaMod, 
    //    LogMod* logMod, RaidMod* raidMod, PersistMetaMod* perMetaMod);
    /**
     * Constructor
     */
    StorageMod(SegmentMetaDataMod* segMetaMod, FileMetaDataMod* fileMetaMod, 
        LogMod* logMod, RaidMod* raidMod, PersistMetaMod* perMetaMod = NULL);

    /**
     * Destructor
     */
    virtual ~StorageMod();

    /**
     * Read
     * file-based read operation
     * \param path the file path
     * \param buf the read buffer that receive data
     * \param offset starting file offset for reading
     * \param length length of the data to read 
     * \param ts timestamp
     * \return the length of data read
     */
#ifndef BLOCK_ITF
    int read(const char* path, char* buf, LL offset, int length, LL ts = 0);
#else 
    int read(char* buf, LL offset, int length, LL ts = 0);
#endif

    /**
     * Write 
     * file-based write operation
     * \param path the file path
     * \param buf the write buffer that contains data
     * \param offset starting file offset for writing
     * \param length length of the data to write
     * \param ts timestamp
     * \return the length of data written
     */
#ifndef BLOCK_ITF
    int write(const char* path, char* buf, LL offset, int length, LL ts = 0);
#else
    /** block-interface */
    int write(char* buf, LL offset, int length, LL ts = 0);
#endif

	/**
	 * Commit all parity updates logged
	 * \return whether the the commit is successful
	 */
    bool syncAllOnDiskLog(void);

    /**
     * getFileMetaData
     * return the file metadata of a specific file
     * \param path the file path 
     * \param fileMeta the file metadata to return
     * \return whether the file metadata already exists
     */
    bool getFileMetaData (const char* path, FileMetaData** fileMeta);

    /**
     * flushUpdateLog
     * flush a number of update log buffers to disk
     * \param numLog the number of log buffers to be flushed
     * \param ts timestamp
     * \return the number of update log buffers flushed
     */
    int flushUpdateLog(LL ts = 0);

private:
    /**
     * m_segMetaMod
     * internal pointer to the segmentMetaDataMod
     */
    SegmentMetaDataMod* const m_segMetaMod;

    /**
     * m_fileMetaMod
     * internal pointer to the fileMetaDataMod
     */
    FileMetaDataMod* const m_fileMetaMod;

    /**
     * m_logMod
     * internal pointer to the logMod 
     */
    LogMod* const m_logMod;

    /**
     * m_raidMod
     * internal pointer to the raidMod 
     */
    RaidMod* const m_raidMod;

    /**
     * m_perMetaMod
     * internal pointer to the PersistMetaMod 
     */
    PersistMetaMod* const m_perMetaMod;

    /**
     * m_logLevel
     * the number of log buffer level in logMod
     */
    const int m_logLevel;

    /**
     * m_updateLogLevel
     * the number of update log buffer level in logMod
     */
    const int m_updateLogLevel;

    /**
     * m_segmentSize
     * the size of a data segment
     */
    const int m_segmentSize;
};

#endif /* STORAGEMOD_HH_ */
