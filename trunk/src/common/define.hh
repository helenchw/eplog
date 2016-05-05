#ifndef __DEFINE_HH__
#define __DEFINE_HH__

#include <set>
#include <vector>
#include <sys/mman.h>
#include <utility>
#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <malloc.h>
#include <boost/thread/shared_mutex.hpp>

using namespace std;

// server/diskmod
//#define USE_MMAP
//#define DISK_DSYNC  
#define DISK_DIRECT_IO  
#define DISK_BLKSIZE    (512)

/** align the buffer with block size in memory for direct I/O **/
static inline void* buf_malloc (unsigned int s) {
#ifdef DISK_DIRECT_IO
    return memalign(DISK_BLKSIZE, s);
#else
    return malloc(s);
#endif
}

static inline void* buf_calloc (unsigned int s, unsigned int unit) {
    void* ret = buf_malloc(s * unit);
    if (ret != nullptr) memset(ret, 0, s * unit);
    return ret;
}

// common/debug.hh
#ifndef DEBUG
#define DEBUG    1
#endif

/**** obsolete code ****/
/** mmap a file */
#define MMAP_FD(fd, size) mmap64(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
/** mmap a file in read-only mode */
#define MMAP_FD_RO(fd, size) mmap64(0, size, PROT_READ, MAP_SHARED, fd, 0)
/** allocate a piece of memory (instead of malloc) */
#define MMAP_MM(size) mmap64(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
/** mmap a file which subsequent changes would not affect the data in the file */
#define MMAP_FD_PV(fd, size) mmap64(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)
/*** END of obsolete code ****/

// all typedef go here
typedef long long LL;
typedef unsigned long long ULL;
typedef unsigned int lba_t;
typedef int disk_id_t;
typedef int chunk_id_t;
typedef unsigned int sid_t;
typedef unsigned int file_id_t;
typedef pair<int, int> off_len_t;
typedef pair<int, int> soff_len_t;
typedef pair<LL, int> foff_len_t;
typedef pair<disk_id_t, lba_t> SegmentLocation; // diskId -> LBA
typedef set<struct FileLocation>::iterator FileLocIt;
typedef struct FileMetaData FileMetaData;
typedef struct FileLocation FileLocation;
typedef set<FileLocation>::iterator FileLocIt;
typedef vector<pair<int, struct FileMetaData*>>::iterator OffFmdIt;
/* boost shared_mutex, can changed to std::shared_mutex if c++14 available */
typedef boost::shared_mutex RWMutex;

// NVMap Wrapper
#ifndef nv_map
#define nv_map std::map
#endif

#ifndef nv_unordered_map
#define nv_unordered_map std::unordered_map
#endif

/** DRY_RUN mode w/o disk I/O, or run w/ disk I/O **/
#ifdef DRY_RUN
    #undef ACTUAL_DISK_IO
    #define DISKLBA_OUT
    /** disk block size, used for printing LBAs **/
    #define DISK_BLOCK_SIZE    (512)
#elif defined DRY_RUN_PERF
    #undef ACTUAL_DISK_IO
#else
    #define ACTUAL_DISK_IO
    #undef DISKLBA_OUT
#endif
//#define READ_AHEAD    (64 * 1024)
#define INVALID_LBA     (lba_t) -1
#define INVALID_DISK    (disk_id_t) -1
#define INVALID_CHUNK   (chunk_id_t) -1
#define INVALID_SEG     (sid_t) -1
#define INVALID_FILE    (file_id_t) -1

/** Data update mode **/
// 1. in-place data update
//#define UPDATE_DATA_IN_PLACE
// 2. stripe-associated parity logging (original PL)
//#define STRIPE_PLOG

/** Log disk write front **/
#define LOG_DISK_RAW_WF    0

/** if not using USE_MD_RAID, use our own encoding schemes **/
//#define USE_MD_RAID

/** default no. of threads, i.e. single thread **/
#define NUM_THREAD    1

#endif
