#ifndef DISKINFO_HH_
#define DISKINFO_HH_

#include <assert.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "common/define.hh"
#include "common/debug.hh"
#include "common/configmod.hh"

/**
 * Disk Information
 */

struct DiskInfo {
    disk_id_t diskId;               /**< Id of disk */
    char diskPath[128];             /**< Path to the disk */
    ULL capacity, remaining;        /**< Total capacity, and remaining capacity, in bytes */
    lba_t numBlock;                 /**< Total number of blocks */
    bool isLogDisk;                 /**< Whether this disk is for log only */
    lba_t dataWriteFront;           /**< Write front for data (in blocks) */

    int fd;                         /**< File descriptor for the disk */
    char* data;                     /**< Pointer for mmap */

    /**
     * Constructor
     */
    DiskInfo() {
        diskId = 0;
        strcpy(diskPath, "");
        capacity = 0;
        remaining = 0;
        fd = 0;
        data = nullptr;
        numBlock = 0;
        dataWriteFront = 0;
    };

    /**
     * Constructor
     * \param diskId id of the disk
     * \param diskPath path to the disk
     * \param capacity total capacity of disk, in bytes
     * \param isLogDisk whether this disk is for log only
     */
    DiskInfo(disk_id_t diskId, const char* diskPath, ULL capacity, bool isLogDisk = false) {
        this->diskId = diskId;
        strncpy(this->diskPath, diskPath, 127);
        this->capacity = capacity;
        this->isLogDisk = isLogDisk;

        // TODO: For benchmark. Assume disk is brand new when starting
        this->remaining = capacity;

        const int blockSize = ConfigMod::getInstance().getBlockSize();
        assert (capacity % blockSize == 0);
        numBlock = capacity / blockSize;

        if (isLogDisk) {
            dataWriteFront = LOG_DISK_RAW_WF;
        } else {
            dataWriteFront = LOG_DISK_RAW_WF;
        }

        int flag = O_RDWR;
#ifdef DISK_DIRECT_IO
        flag |= O_DIRECT;
#endif
#ifdef DISK_DSYNC
        flag |= O_DSYNC;
#endif

        fd = open(diskPath, flag);
        assert(fd != -1);

        // map disk to memory
#ifdef USE_MMAP
        data = (char*) MMAP_FD (fd, capacity);

        if (data == MAP_FAILED) {
            debug_error("mmap failed: %s\n", strerror(errno));
        }
#endif
    }

    void unmap() {
#ifdef USE_MMAP
        msync(data, capacity, MS_SYNC);
        munmap(data, capacity);
#endif
        close(fd);
        data = nullptr;
        fd = 0;
    }

};

#endif /* DISKINFO_HH_ */
