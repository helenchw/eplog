#ifndef FILEMETADATAMOD_HH_ 
#define FILEMETADATAMOD_HH_

#include <string>
#include <unordered_map>
#include "common/define.hh"
#include "common/struct/filemetadata.hh"
#include "common/struct/segmentmetadata.hh"
#include "server/segmetamod.hh"

/**
 * Module for file metadata management (in-memory)
 */
class FileMetaDataMod {
public:
    /**
     * Construtor
     * \param segmetaMod the segmentMetaDataMod
     */
    FileMetaDataMod(SegmentMetaDataMod* segmetaMod = NULL);

    /**
     * Destructor
     */
    ~FileMetaDataMod();

    /**
     * Create a file metadata entry for a new file
     * \param filename file name
     * \param fileMetaData file metadata data structure for the file
     * \return whether the file is created
     */
    bool createEntry(std::string filename, FileMetaData **fileMetaData);

    /**
     * Get a file metadata entry for a file
     * \param filename file name
     * \param fileMetaData file metadata data structure for the file
     * \return whether the file exists
     */
    bool getEntry(std::string filename, FileMetaData **fileMetaData);

    /**
     * Check if a file already exists
     * \param filename file name
     * \return whether the file exists
     */
    bool existEntry(std::string filename);

    /**
     * Remove a file entry
     * \param filename file name
     * \return whether the file exists for removal
     */
    bool removeEntry(std::string filename);

    /**
     * Update the file location reference of a file
     * \param fmd file metadata data structure
     * \param fileOff file offset of the modification
     * \param len length of the modification
     * \param smd data structure of the new segment for reference
     * \param segOff starting segment offset containing valid data
     * \param isUpdate whether this location change is triggered by file update
     */
    void updateFileLocs(FileMetaData* fmd, LL fileOff, 
                        int len, SegmentMetaData* smd, int segOff, 
                        bool isUpdate = false);

    /** Mapping of file id and file metadata data structure */
    nv_unordered_map<file_id_t, FileMetaData*> m_metaMap;

    /** Mapping of file id and file name */
    nv_unordered_map<std::string, file_id_t> m_idMap;

private:
    RWMutex m_metaOptMutex;                           /**< Lock for operations on metaMap and idMap */
    file_id_t m_fileIdCounter;                        /**< File ID counter, no need to be atomic, we have lock already */
    SegmentMetaDataMod* m_segmetaMod;                 /**< Internal pointer to SegmentMetaDataMod */
    const int m_pageSize;                             /**< Page size */

    /**
     * Get file id by file name
     * \param filename file name
     * \return file id associated with the file name
     */
    file_id_t getFileId(std::string filename);
};

#endif /* FILEMETADATAMOD_HH_ */
