#ifndef FILEMETADATA_HH_
#define FILEMETADATA_HH_

#include <set>
#include <map>
#include <mutex>
#include <climits>
#include "common/define.hh"
#include "common/struct/segmentmetadata.hh"

/**
 * Mapping from a file to (part of) a segment
 */
struct FileLocation {
    LL fileOffset;                     /**< Offset of data within a file */
    int locLength;                     /**< Length of data referenced */
    int segmentOffset;                 /**< Offset of data within a segment */
    /** segment metadata data structure associated with the data */
    struct SegmentMetaData* segmentMetaData;

    FileLocation():
    fileOffset(0),locLength(0),segmentOffset(0),segmentMetaData(0) {}

    FileLocation(LL fileOffset, int locLength, int segmentOffset, SegmentMetaData* smd):
    fileOffset(fileOffset),locLength(locLength),
    segmentOffset(segmentOffset),segmentMetaData(smd) {}

    bool operator < (const struct FileLocation& fl) const {
        return fileOffset < fl.fileOffset || 
                (fileOffset == fl.fileOffset && locLength < fl.locLength);
    };

    bool operator == (const struct FileLocation& fl) const {
        return (segmentMetaData == fl.segmentMetaData);
    };

    void out() { 
        printf ("fileOffset = %lld, locLength = %d in segment offset = %d\n",
                            fileOffset, locLength,
                            segmentOffset); 
    };
    LL size() const {
        // hardcode according to paper
        // sizeof(file offset) + sizeof(segment offset) + sizeof(segment id)
        return sizeof(fileOffset) + sizeof(segmentOffset) + sizeof(LL); 
    }
};

/**
 * File metadata
 */
struct FileMetaData {
    long long fileSize;                /**< File size */
    file_id_t fileId;                  /**< File id */
    /** set of reference to segments composing the file */
    std::set<FileLocation> fileLocs;
    RWMutex fileMutex;                 /**< Lock for operations on file */
    std::mutex hotMutex;               /**< Lock for hotness operations on file */

    /**
     * Find the segments covered by the request
     * \param fileOff the starting file offset of the request
     * \param len the length of request
     * \return a pair of file locations 1) include the starting offset, 2) passing the end of request (exclusive)
     */
    tuple<FileLocIt, FileLocIt> findCover(LL fileOff, int len) {
        // create search lower bound
        FileLocation pin(fileOff, 0, 0, 0);
        FileLocIt lit = fileLocs.lower_bound(pin);
        while (lit != fileLocs.begin()
                && (lit == fileLocs.end() || lit->fileOffset > fileOff))
            lit--;
        FileLocIt rit;
        // create search upper bound
        if (fileLocs.empty() || fileOff + len > lit->fileOffset + lit->locLength) {
            // cross more than one fileLocs
            pin.fileOffset = fileOff + len - 1;
            pin.locLength = INT_MAX;    // looks redundant
            rit = fileLocs.upper_bound(pin);
            while (rit != fileLocs.end() && rit->fileOffset <= fileOff + len - 1)
                rit++;
        } else {
            // included in one fileLocs
            rit = std::next(lit);
        }
        return tuple<FileLocIt, FileLocIt>(lit, rit);
    }

};

#endif /* FILEMETADATA_HH_ */
