#include "server/filemetamod.hh"

#include <tuple>
#include <unistd.h>
#include <stdexcept>
#include <openssl/md5.h>
#include "common/debug.hh"
#include "common/configmod.hh"


FileMetaDataMod::FileMetaDataMod(SegmentMetaDataMod* segmetaMod):
    m_segmetaMod(segmetaMod),
    m_pageSize(ConfigMod::getInstance().getPageSize()) {
    m_fileIdCounter = 0;
}

FileMetaDataMod::~FileMetaDataMod() {

}

// TODO: later, createEntry may combine with request info for new file
bool FileMetaDataMod::createEntry(std::string filename, FileMetaData **fileMetaData) {
    m_metaOptMutex.lock();
    
    // chech exist or not, since two threads may call createEntry simultaneously
    FileMetaData* fmd = nullptr;
    bool isCreated = true;
    nv_unordered_map<std::string, file_id_t>::const_iterator it = m_idMap.find(filename);

    if (it == m_idMap.end()) {
        fmd = new FileMetaData;
        m_idMap[filename] = ++m_fileIdCounter;
        m_metaMap[m_fileIdCounter] = fmd;
        //m_idMap[filename] = fid;
        //m_metaMap[fid] = fmd;
        // Some initialization
        fmd->fileId = m_fileIdCounter;
        fmd->fileSize = 0;
        fmd->fileLocs.clear();
    } else {
        fmd = m_metaMap[it->second];
        isCreated = false;
    }

    m_metaOptMutex.unlock();
    *fileMetaData = fmd;
    return isCreated;
}

bool FileMetaDataMod::getEntry(std::string filename, FileMetaData **fileMetaData) {
    m_metaOptMutex.lock_shared();
    FileMetaData* ret = nullptr;
    bool isExists = false;
    try {
        ret = m_metaMap.at(m_idMap.at(filename));
        isExists = true;
    } catch (const std::out_of_range& oor) {
        debug("No element called %s is found in metadata\n", filename.c_str());
    }
    m_metaOptMutex.unlock_shared();
    *fileMetaData = ret;
    return isExists;
}

bool FileMetaDataMod::existEntry(std::string filename) {
    m_metaOptMutex.lock_shared();
    bool ret = m_idMap.count(filename);
    m_metaOptMutex.unlock_shared();
    return ret;
}

bool FileMetaDataMod::removeEntry(std::string filename) {
    m_metaOptMutex.lock();
    bool ret = false;
    try {
        m_metaMap.erase(m_idMap.at(filename));
        m_idMap.erase(filename);
        ret = true;
    } catch (const std::out_of_range& oor) {
        debug("No element called %s is found in metadata\n", filename.c_str());
        ret = false;
    }
    m_metaOptMutex.unlock();
    return ret;
}

// ** assume the smd is not involved in original file metadata
void FileMetaDataMod::updateFileLocs(FileMetaData* fmd, LL fileOff, int len,
        SegmentMetaData* smd, int segOff, bool isUpdate) {

    FileLocIt lit, rit;
    // find modification coverage
    tie (lit, rit) = fmd->findCover (fileOff, len);

    FileLocation FLeft, FRight;
#ifndef DNDEBUG
    if (isUpdate) { 
        --rit;
        assert (rit == lit || len >= ConfigMod::getInstance().getSegmentSize()); 
        ++rit; 
    }
#endif

    for (FileLocIt it = lit; it != rit; it++) {
        debug("Segment %d involved\n", it->segmentMetaData->segmentId);
        // redefine the offset and effective length of the first affected segment
        int cleanLeft = it->segmentOffset;
        int cleanRight = it->segmentOffset + it->locLength;
        if (it->fileOffset < fileOff) {
            FLeft.fileOffset = it->fileOffset;
            FLeft.locLength = fileOff - it->fileOffset;
            FLeft.segmentMetaData = it->segmentMetaData;
            FLeft.segmentOffset = it->segmentOffset;
            cleanLeft += FLeft.locLength;
        }
        // redefine the offset and effective length of the last affected segment
        if (it->fileOffset + it->locLength > fileOff + len) {
            FRight.fileOffset = fileOff + len;
            FRight.locLength = it->fileOffset + it->locLength - fileOff - len;
            FRight.segmentMetaData = it->segmentMetaData;
            FRight.segmentOffset = it->segmentOffset + it->locLength
                    - FRight.locLength;
            cleanRight -= FRight.locLength;
        }
        // clear bitmap of segments / part of segments in-between
        if (cleanRight - cleanLeft) {
            // TODO: May switch to multithreading
            //it->segmentMetaData->segmentLock.lock();
            if (!isUpdate && !it->segmentMetaData->isUpdateLog) {
                debug("Clean Bitrange of segment %d from %d with len %d\n",
                        it->segmentMetaData->segmentId, cleanLeft,
                        cleanRight - cleanLeft);
                it->segmentMetaData->bitmap.clearBitRange(cleanLeft / m_pageSize,
                        (cleanRight - cleanLeft) / m_pageSize);
                m_segmetaMod->updateHeap(it->segmentMetaData->segmentId, cleanLeft - cleanRight);
            } else {
                debug("Update segment %d from %d with len %d without cleaning bits\n",
                        it->segmentMetaData->segmentId, cleanLeft,
                        cleanRight - cleanLeft);
            }
            //it->segmentMetaData->fileSet.erase(fmd);
            //it->segmentMetaData->segmentLock.unlock();

        }

    }

    bool insertNew = true;

    // erase all the segment refs that involved old data
    fmd->fileLocs.erase(lit, rit);
    // TODO : merge duplicated reference to same segment
    // add back the partial segments refs (first, last)

    if (FLeft.locLength) {
        if (FLeft.segmentMetaData->segmentId == smd->segmentId 
                && FLeft.segmentOffset+FLeft.locLength == segOff) {
            FLeft.locLength += len;
            insertNew = false;
        }

        debug("LEFT(%d,%d,%d)\n",
            FLeft.segmentMetaData->segmentId, FLeft.segmentOffset, FLeft.locLength);
        fmd->fileLocs.insert(FLeft);
    } 

    if (FRight.locLength) {
        if (FRight.segmentMetaData->segmentId == smd->segmentId
                && FRight.segmentOffset == segOff+len) {
            FRight.segmentOffset = segOff;
            insertNew = false;
        }

        debug("RIGHT(%d,%d,%d)\n",
            FRight.segmentMetaData->segmentId, FRight.segmentOffset, FRight.locLength);
        // merge without inserting a duplicated ref to same segment
        fmd->fileLocs.insert(FRight);
    }

    
    if (insertNew) {
        FileLocation fileLoc;
        fileLoc.segmentMetaData = smd;

        debug("NEW(%d,%d,%d)\n", smd->segmentId, segOff, len);
        fmd->fileLocs.insert(FileLocation(fileOff, len, segOff, smd));
    }

}

unsigned int FileMetaDataMod::getFileId(std::string filename) {
    unsigned lower = 0, upper = 0;
    unsigned char hash[MD5_DIGEST_LENGTH];

    MD5((unsigned char*)filename.c_str(), filename.length(), hash);

    for (int i = 0, rp = 1; i < MD5_DIGEST_LENGTH; i++, rp *= 8) {
        if (i == MD5_DIGEST_LENGTH / 2) { rp = 1;}
        if (i < MD5_DIGEST_LENGTH / 2) {
            lower += hash[i] * rp;
        } else {
            upper += hash[i] * rp;
        }
    }
    
    return lower ^ upper;
}
