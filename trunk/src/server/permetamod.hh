#ifndef __PER_META_MOD_HH__
#define __PER_META_MOD_HH__
#include <unordered_set>
#include <mutex>
#include "diskmod.hh"
#include "filemetamod.hh"
#include "segmetamod.hh"
#include "syncmod.hh"
#include "../common/define.hh"
#include "../common/struct/codesetting.hh"
#include "../common/struct/segmentmetadata.hh"

#define APPLOG_MAGIC    0xA9910614
#define APPLOG_LOG_FILE 0xA1
#define APPLOG_LOG_SEG  0xA2
#define APPLOG_LOG_LSEG 0xA3

/**
 * Module for maintaining persistent metadata on disk
 * (only support flush to disk for the time being ...)
 */
class PersistMetaMod {

public:
    /**
     * Constructor
     * \param fileMetaMod FileMetaDataMod
     * \param segMetaMod SegMetaDataMod
     * \param syncMod SyncMod
     * \param diskMod DiskMod, maintaining the array of disk for storing metadata
     * \param dataDiskMod DiskMod, maintaining the array of disk for storing data
     * \param dataCodeSetting coding scheme for data
     */
    PersistMetaMod(FileMetaDataMod *fileMetaMod, 
        SegmentMetaDataMod* segMetaMod, SyncMod* syncmod, 
        DiskMod *diskmod, DiskMod* dataDiskmod, 
        CodeSetting dataCodeSetting);

    /**
     * Destructor
     */
    ~PersistMetaMod();

    /**
     * Create a full snapshot on latest version of metadata
     * \return whether the snapshot is flushed successfully
     */
    bool createSnapshot();

    // TODO support snapshot reading and verifying
    //bool restoreSnapshot();
    //bool verifySnapshot();

    /**
     * Log changes on metadata since last full snapshot (incremental snapshot)
     * \return whether the changes are flushed successfully
     */
    bool flushMetaLog();

    /**
     * Add a dirty file for snapshot
     * \param fid file id
     * \return whether the id is added successfully
     */
    bool addDirtyFile(file_id_t fid);

    /**
     * Add a dirty data segment for snapshot
     * \param sid data segment id
     * \return whether the id is added successfully
     */
    bool addDirtySeg(sid_t sid);

    /**
     * Add a dirty log segment for snapshot
     * \param lid log segment id
     * \return whether the id is added successfully
     */
    bool addDirtyLogSeg(sid_t lid);

private:
    /**
     * Superblock
     */
    struct SuperBlock {
        unsigned int magic;                            /**< Magic code to identify EPLog meta */
        unsigned int pageSize;                         /**< Page size */
        unsigned int blockSize;                        /**< Block size */
        unsigned int checkpoint;                       /**< Starting block address of the full checkpoint area */
        unsigned int checkpointSize;                   /**< Size of a full checkpoint, in blocks */
        unsigned int logzoneSize;;                     /**< Size of a log zone (incremental checkpoint), in blocks */
        unsigned int segmentSize;                      /**< Number of blocks in a data segment */
    };

    /**
     * Checkpoint header
     */
    struct CheckpointHeader {
        unsigned int version;                          /**< Version number of a checkpoint */
        unsigned int fileNum;                          /**< Number of files */
        unsigned int segmentNum;                       /**< Number of data segments */
        unsigned int logSegmentNum;                    /**< Number of log segments */
        unsigned int fileMetaStart;                    /**< Starting block address for file metadata */
        unsigned int segmentMetaStart;                 /**< Starting block address for segment metadata */
        unsigned int logSegmentMetaStart;              /**< Starting block address for log metadata */
        unsigned int end;                              /**< Ending block address for this checkpoint */
        CheckpointHeader(): version(-1), fileNum(-1), segmentNum(-1),
            logSegmentNum(-1) {};
    };

    // external modules
    FileMetaDataMod* m_fileMetaMod;                    /**< Pointer to FileMetaMod */
    SegmentMetaDataMod* m_segMetaMod;                  /**< Pointer to SegmentMetaDataMod */
    DiskMod* m_diskMod;                                /**< Pointer to DiskMod for metadata */
    SyncMod* m_syncMod;                                /**< Pointer to SyncMod */
    DiskMod* m_dataDiskMod;                            /**< Pointer to DiskMod for data */

    // internal variables
    int m_curZone;                                     /**< Current full checkpoint zone in-use */
    int m_pageSize;                                    /**< Page size */
    int m_blockSize;                                   /**< Block size */
    struct SuperBlock m_sb;                            /**< Superblock in memory */
    struct CheckpointHeader m_chkpt;                   /**< Checkpoint header in memory */
    CodeSetting m_dataCodeSetting;                     /**< Coding scheme for data segments */
    char* m_chunkbuf;                                  /**< Buffer for write */
    char* m_readbuf;                                   /**< Buffer for read */
    static const int m_chunkbufSize = 2;                      /**< Number of buffers for write */
    static const int m_readbufSize = 128;                     /**< Number of buffers for read */
    std::mutex m_metaOp;                               /**< Lock for metadata operations to/form disks */

    // Checkpoint
    bool writeCheckpointHeader(lba_t& curlba);         /**< Write checkpoint header to disk */
    bool writeAllFileMeta(lba_t& curlba);              /**< Write all file metadata to disk */
    bool writeAllDataSegmentMeta(lba_t& curlba);       /**< Write all data segment metadata to disk */
    bool writeAllLogSegmentMeta(lba_t& curlba);        /**< Write all log segment metadata to disk */
    // TODO support checkpoint reading
    //bool readCheckpoint();                             
    //bool readCheckpointHeader();
    //bool readAllFileMeta();
    //bool readAllDataSegmentMeta();
    //bool readAllLogSegmentMeta();

    // Logging (incremental checkpoint)
    lba_t m_logHead;                                   /**< Starting block address for logging (incremental checkpointing) */ 
    std::unordered_set<file_id_t> m_dirtyFiles;        /**< Set of dirty file to flush */ 
    std::unordered_set<sid_t> m_dirtySegs;             /**< Set of dirty data segments to flush */ 
    std::unordered_set<sid_t> m_dirtyLogSegs;          /**< Set of dirty log segments to flush */
    void resetLogHead();                               /**< Reset the starting block address for logging */ 
    bool writeDirtyFiles(int& curOfs);                 /**< Write changes on file metadata to disk */
    bool writeDirtySegments(int& curOfs);              /**< Write changes on data segment metadata to disk */
    bool writeDirtyLogSegments(int& curOfs);           /**< Write changes on log segment metadata to disk */

    // Helper functions
    /**
     * Write data to disk for checkpointing
     * \param curOfs offset of the buffer accepting new data
     * \param curlba block address for next block write 
     * \param T structure containing the data to write
     */
    template<typename T> void writeToBufCP(int& curOfs, lba_t& curlba, T);

    /**
     * Write data to disk for logging
     * \param curOfs offset of the buffer accepting new data
     * \param curlba block address for next block write 
     * \param T structure containing the data to write
     */
    template<typename T> void writeToBufLog(int& curOfs, lba_t& curlba, T);

    /**
     * Flush data to disk if the buffer is full
     * \param curOfs offset of the buffer accepting new data
     * \param dest block address for next block write 
     */
    void flushBlock(int& curOfs, lba_t& dest);

    /**
     * Force flushing data to disk for checkpointing
     * \param curOfs offset of the buffer accepting new data
     * \param dest block address for next block write 
     */
    void forceBlockToDiskCP(int& curOfs, lba_t& dest);

    /**
     * Force flushing data to disk for logging
     * \param curOfs offset of the buffer accepting new data
     * \param dest block address for next block write 
     */
    void forceBlockToDiskLog(int& curOfs, lba_t& dest);

    /**
     * Force flushing data to disk (general)
     * \param curOfs offset of the buffer accepting new data
     * \param dest block address for next block write 
     */
    void forceBlockToDisk(int& curOfs, lba_t& dest);

    // SuperBlock
    /**
     * Initialize a super block
     * \return whether the initialization is successful
     */
    bool initSuperBlock();

    /**
     * Write a super block to disk
     * \return whether the superblock is successfully written
     */
    bool writeSuperBlock();

    /**
     * Read a super block to disk
     * \return whether the superblock on disk is valid and can be read
     */
    bool readSuperBlock();


/**
 * Mapping and organizations
 *  - Magic Number embedded in the front of each device (partiton)
 *  - Version No.
 *  - 2 switching area for checkpointing
 *  - Area for logging
 */
}; 

#endif
