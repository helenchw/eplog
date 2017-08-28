#include <chrono>
#include "common/debug.hh"
#include "common/define.hh"
#include "common/configmod.hh"
#include "server/storagemod.hh"
#include "server/segmetamod.hh"
#include "server/gcmod.hh"
#include "server/kvmod.hh"

typedef chrono::high_resolution_clock Clock;
typedef chrono::milliseconds milliseconds;
int segmentSize, pageSize, pagePerBlock, blockPerSeg, blockSize;
int testid = 0;

/*! 
    Print the buffer, of which size is the same as a segment
    \param buf Buffer
 */
void printbuf(char* buf) {
    printf("Dumping buffer ... \n");
    for (int i = 0; i < segmentSize; i++) {
        if (i % 30 == 0 && i > 0) {
            printf("\n");
        }
        printf("[%hhx]",buf[i]);
    }
    printf("\n");
}

/*! 
    Print the difference betweeen two buffers, each of which is of the size of a segment
    \param buf First buffer
    \param buf1 Second buffer
 */
void printdiff(char* buf, char* buf1, int len) {
    int start = -1, end = -1;
    for (int i = 0; i < len; i++) {
        if (buf[i] != buf1[i]) {
            if (start == -1) {
                printf("start diff [%x] vs [%x]\n", buf[i], buf1[i]);
                start = i;
            }
            if (i > end) end = i;
        }
    }
    if ( ! (start == -1 && end == -1) ) {
        printf("\nstart %d end %d\n", start, end);
    }
}

int main () {

	// Read the configuration file
    ConfigMod::getInstance().setConfigPath("config.ini");

    printf ("%s\n", "========== Start example tests ==========");
    srand( (unsigned int) time(NULL));

    // List the disks available in the system
    DiskInfo disk1 (0, "disk1",  1048576ULL * 20);
    DiskInfo disk2 (1, "disk2",  1048576ULL * 20);
    DiskInfo disk3 (2, "disk3",  1048576ULL * 20);
    DiskInfo disk4 (3, "disk4",  1048576ULL * 20);
    DiskInfo disk5 (4, "disk5",  1048576ULL * 20);
    DiskInfo disk6 (5, "disk6",  1048576ULL * 20);
    DiskInfo disk7 (6, "disk7",  1048576ULL * 20, true);
    DiskInfo disk8 (7, "disk8",  1048576ULL * 20, true);
    vector<DiskInfo> v_diskInfo {disk1,disk2,disk3,disk4,disk5,disk6,disk7,disk8};
    DiskMod* diskMod = new DiskMod (v_diskInfo);

    // Set the coding scheme
    CodeSetting codeSetting (6,4,8,CAUCHY_CODING);
    CodeSetting codeLogSetting (8,6,8,CAUCHY_CODING,true);
    vector<CodeSetting> codeSettingList;
    codeSettingList.push_back (codeSetting);
    codeSettingList.push_back (codeLogSetting);
    segmentSize = ConfigMod::getInstance().getSegmentSize();
    pageSize = ConfigMod::getInstance().getPageSize();
    pagePerBlock = ConfigMod::getInstance().getNumPagePerBlock();
    blockPerSeg = ConfigMod::getInstance().getNumBlockPerSegment();
    blockSize = pageSize * pagePerBlock;

    // Initialize GC queues
    LowLockQueue<SegmentMetaData*> gcQueue;
    LowLockQueue<SegmentMetaData*> logGcQueue;

    // Initialize and connect modules
    SyncMod* syncMod = new SyncMod();
    SegmentMetaDataMod* segMetaMod = new SegmentMetaDataMod();
    RaidMod* raidMod = new RaidMod (diskMod, codeSettingList, &gcQueue, segMetaMod, syncMod);
    FileMetaDataMod* fileMetaMod = new FileMetaDataMod(segMetaMod);

    // different log buffer but share the same metadata pool
    LogMod* logMod = new LogMod(raidMod, segMetaMod, fileMetaMod, syncMod, codeLogSetting);
    StorageMod * storageMod = new StorageMod(segMetaMod, fileMetaMod, logMod, raidMod);

    cerr << "Segment Size = " << (int)segmentSize << "bytes\n";

    
    bool same = false;
    {

        // TEST1: write -> read
        testid++;
        printf ("\n>>> Test %d: Write a file\n", testid);
        char *buf = (char*) malloc (sizeof(char)*segmentSize);
        int fd = open("/dev/urandom", O_RDONLY);
        int ret = read(fd, buf, segmentSize);
        if (ret < 0) {
            printf("Cannot read from /dev/urandom!\n");
        }
        close(fd);

        // write
#ifndef BLOCK_ITF
        int byteWritten = storageMod->write("testfile", buf, 0, segmentSize);
#else
        int byteWritten = storageMod->write(buf, 0, segmentSize);
#endif
        printf ("byteWriten = %d\n", byteWritten);

        // read
        char *buf1 = (char*) malloc (sizeof(char)*segmentSize);
        memset (buf1, 0, segmentSize);
#ifndef BLOCK_ITF
        int byteRead = storageMod->read("testfile", buf1, 0, segmentSize);
#else
        int byteRead = storageMod->read(buf1, 0, segmentSize);
#endif
        printf ("byteRead = %d\n", byteRead);
        same = (memcmp (buf, buf1, segmentSize) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf (">>> Test %d (write a new file) passed.\n", testid);

        // TEST2: modify
        testid++;
        printf("\n>>> Test %d: Overwrite the file\n", testid);
        char *obuf = (char*) malloc (sizeof(char)*segmentSize);
        memcpy(obuf, buf, segmentSize);
        ret = read (fd, buf + blockSize, blockSize*2);
        // modifiy
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile", buf + blockSize, blockSize, blockSize*2);
#else
        byteWritten = storageMod->write(buf + blockSize, blockSize, blockSize*2);
#endif
        printf ("byteWriten = %d\n", byteWritten);

        // read
        memset(buf1, 0, segmentSize);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile", buf1, 0, segmentSize);
#else
        byteRead = storageMod->read(buf1, 0, segmentSize);
#endif
        printf("byteRead = %d\n", byteRead);

        same = (memcmp(buf, buf1, segmentSize) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf(">>> Test %d (overwrite the file) passed.\n", testid);
        free(buf);
        free(buf1);
        free(obuf);
    }
    
    {
        // TEST3: 2M file, modify, read
        testid++;
        printf ("\n>>> Test %da: Write another new file\n", testid);
        char *buf = (char*) malloc (sizeof(char)*segmentSize * 2);
        char *buf1 = (char*) malloc (segmentSize * 2);
        memset (buf, 'A', segmentSize);
        memset (buf + segmentSize, 'B', segmentSize);

        // write
#ifndef BLOCK_ITF
        int byteWritten = storageMod->write("testfile1", buf, 0, segmentSize * 2);
#else
        int byteWritten = storageMod->write(buf, segmentSize * 2, segmentSize * 2);
#endif
        printf ("Whole segment, byteWriten = %d\n", byteWritten);
        printf (">>> Test %da (write another new file) passed.\n", testid);

        printf (">>> Test %db: Overwrite the file\n", testid);
        int skip = (blockPerSeg < 3)? 0:blockPerSeg/2;

        // modify
        if (blockPerSeg >= 2) {
            memset (buf + blockSize * skip, 'C', blockSize * 2);
#ifndef BLOCK_ITF
            byteWritten = storageMod->write("testfile1", buf + blockSize * skip, blockSize * skip, blockSize * 2);
#else
            byteWritten = storageMod->write(buf + blockSize * skip, segmentSize * 2 + blockSize * skip, blockSize * 2);
#endif
            printf ("2 chunks, byteWriten = %d\n", byteWritten);
        }

        // read
        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        int byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        int byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }

        // modify
        memset (buf + blockSize * (blockPerSeg -1), 'D', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("1 chunk, byteWriten = %d\n", byteWritten);

        // read
        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf (">>> Test %db (overwrite the file) passed.\n", testid);

        printf (">>> Test %dc: Overwrite the file\n", testid);

        // force buffer flush, modify
        memset (buf + blockSize * (blockPerSeg -1), 'T', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("1 chunk, byteWriten = %d\n", byteWritten);

        // read
        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf (">>> Test %dc (overwrite the file) passed.\n", testid);

        printf (">>> Test %dc: Overwrite the file\n", testid);
        // modify 
        memset (buf + blockSize * (blockPerSeg -1), 'L', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("1 chunk, byteWriten = %d\n", byteWritten);

        // modify 
        memset (buf + blockSize * (blockPerSeg -1), 'P', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("1 chunk, byteWriten = %d\n", byteWritten);

        //read
        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf (">>> Test %dd (overwrite the file) passed.\n", testid);

        printf (">>> Test %de: Overwrite the file\n", testid);
        // modify 
        memset (buf + blockSize * (blockPerSeg -1) , 'E', blockSize * 2);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize * 2);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize * 2);
#endif
        printf ("Cross segment, 2 chunks, byteWriten = %d\n", byteWritten);

        // read
        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf (">>> Test %de (overwrite the file) passed.\n", testid);

        printf (">>> Test %df: Partial read\n", testid);
        // read
        memset(buf1, 0, segmentSize * 2);
        if (blockPerSeg < 2) {
#ifndef BLOCK_ITF
            byteRead = storageMod->read("testfile1", buf1, blockSize, blockSize*(blockPerSeg-1));
#else
            byteRead = storageMod->read(buf1, segmentSize * 2 + blockSize, blockSize*(blockPerSeg-1));
#endif
            printf("byteRead = %d\n", byteRead);
            same = (memcmp(buf + blockSize, buf1, blockSize*(blockPerSeg-1)) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf + blockSize, buf1, blockSize*(blockPerSeg-1));
                printf("----------------------------------------\n");
                assert (same);
            }
        } else {
#ifndef BLOCK_ITF
            byteRead = storageMod->read("testfile1", buf1, blockSize, blockSize*(blockPerSeg -2));
#else
            byteRead = storageMod->read(buf1, segmentSize * 2 + blockSize, blockSize*(blockPerSeg -2));
#endif
            printf("byteRead = %d\n", byteRead);
            same = (memcmp(buf + blockSize, buf1, blockSize*(blockPerSeg -2)) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf + blockSize, buf1, blockSize*(blockPerSeg -2));
                printf("----------------------------------------\n");
                assert (same);
            }
        }
        printf (">>> Test %de (partial read) passed.\n", testid);
        
        printf ("\n>>> Repeat Test %da-f\n", testid);
        // REPEAT every test
        memset (buf, 'A', segmentSize);
        memset (buf + segmentSize, 'B', segmentSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf, 0, segmentSize * 2);
#else
        byteWritten = storageMod->write(buf, segmentSize * 2, segmentSize * 2);
#endif
        printf ("[REPEAT] Whole segment, byteWriten = %d\n", byteWritten);

        skip = (blockPerSeg < 3)? 0:blockPerSeg/2;
        if (blockPerSeg >= 2) {
            memset (buf + blockSize * skip, 'C', blockSize * 2);
#ifndef BLOCK_ITF
            byteWritten = storageMod->write("testfile1", buf + blockSize * skip, blockSize * skip, blockSize * 2);
#else
            byteWritten = storageMod->write(buf + blockSize * skip, segmentSize * 2 + blockSize * skip, blockSize * 2);
#endif
            printf ("[REPEAT] 2 chunks, byteWriten = %d\n", byteWritten);
        }

        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("[REPEAT] Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }

        memset (buf + blockSize * (blockPerSeg -1), 'D', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("[REPEAT] 1 chunk, byteWriten = %d\n", byteWritten);

        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("[REPEAT] Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }

        // force buffer flush
        memset (buf + blockSize * (blockPerSeg -1), 'T', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("[REPEAT] 1 chunk, byteWriten = %d\n", byteWritten);

        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("[REPEAT] Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }

        memset (buf + blockSize * (blockPerSeg -1), 'L', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("[REPEAT] 1 chunk, byteWriten = %d\n", byteWritten);

        memset (buf + blockSize * (blockPerSeg -1), 'P', blockSize);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize);
#endif
        printf ("[REPEAT] 1 chunk, byteWriten = %d\n", byteWritten);

        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("[REPEAT] Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize * 2);
            printf("----------------------------------------\n");
            assert (same);
        }

        memset (buf + blockSize * (blockPerSeg -1) , 'E', blockSize * 2);
#ifndef BLOCK_ITF
        byteWritten = storageMod->write("testfile1", buf + blockSize * (blockPerSeg -1), blockSize * (blockPerSeg -1), blockSize * 2);
#else
        byteWritten = storageMod->write(buf + blockSize * (blockPerSeg -1), segmentSize * 2 + blockSize * (blockPerSeg -1), blockSize * 2);
#endif
        printf ("[REPEAT] Cross segments, 2 chunks, byteWriten = %d\n", byteWritten);

        memset(buf1, 0, segmentSize * 2);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize * 2);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("[REPEAT] Whole segment, byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1, segmentSize *2);
            printf("----------------------------------------\n");
            assert (same);
        }

        memset(buf1, 0, segmentSize * 2);
        if (blockPerSeg < 2) {
#ifndef BLOCK_ITF
            byteRead = storageMod->read("testfile1", buf1, blockSize, blockSize*(blockPerSeg-1));
#else
            byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
            printf("[REPEAT] byteRead = %d\n", byteRead);
            same = (memcmp(buf, buf1, segmentSize * 2) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf, buf1, segmentSize *2);
                printf("----------------------------------------\n");
                assert (same);
            }
        } else {
#ifndef BLOCK_ITF
            byteRead = storageMod->read("testfile1", buf1, blockSize, blockSize*(blockPerSeg -2));
#else
            byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
            printf("[REPEAT] byteRead = %d\n", byteRead);
            same = (memcmp(buf, buf1, segmentSize * 2) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf, buf1, segmentSize *2);
                printf("----------------------------------------\n");
                assert (same);
            }
        }
        printf("[REPEAT] Test %da-f passed.\n", testid);

        // Test 4 Parity commit
        testid++;
        printf(">>>> Test %d: Parity commit\n", testid);
        logMod->flushToDiskInBatch();
        storageMod->flushUpdateLog();
        storageMod->syncAllOnDiskLog();
        diskMod->fsyncDisks();
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf + blockSize, buf1, blockSize*(blockPerSeg -2));
            printf("----------------------------------------\n");
            assert (same);
        }
        printf(">>>> Test %d (parity commit) passed\n\n", testid);
        
        // Test 5 Recovery
        testid++;
        printf(">>>> Test %d: Recovery\n", testid);
        vector<disk_id_t> disks;
        disks.push_back(2);
        disks.push_back(3);
        for (disk_id_t d : disks) {
            diskMod->setDiskStatus(d, false);
            diskMod->setLBAsFree(d, 0, INT_MAX);
        }
        uint64_t recovered = raidMod->recoverData(disks, disks);
        printf("bytes recovered = %lu\n", recovered);
#ifndef BLOCK_ITF
        byteRead = storageMod->read("testfile1", buf1, 0, segmentSize);
#else
        byteRead = storageMod->read(buf1, segmentSize * 2, segmentSize * 2);
#endif
        printf("byteRead = %d\n", byteRead);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf + blockSize, buf1, blockSize*(blockPerSeg -2));
            printf("----------------------------------------\n");
            assert (same);
        }
        printf(">>>> Test %d (recovery) passed\n\n", testid);

    }

#ifdef BLOCK_ITF
    {
        delete storageMod;
        delete logMod;
        delete fileMetaMod;
        delete raidMod;
        delete segMetaMod;
        delete syncMod;

        syncMod = new SyncMod();
        segMetaMod = new SegmentMetaDataMod();
        raidMod = new RaidMod (diskMod, codeSettingList, &gcQueue, segMetaMod, syncMod);
        fileMetaMod = new FileMetaDataMod(segMetaMod);

        logMod = new LogMod(raidMod, segMetaMod, fileMetaMod, syncMod, codeLogSetting);
        storageMod = new StorageMod(segMetaMod, fileMetaMod, logMod, raidMod);

        // Test 6 key-value interfacae
        testid++;
        printf(">>>> Test %d: KV-store interface\n", testid);
        
        KVMod *kvMod = new KVMod(storageMod);

        char keys[1024][24];
        char values[1024][4096];

        printf(">>>> Test %da: Set new key-value pairs\n", testid);
        int fd = open("/dev/urandom", O_RDONLY);
        int ret = 0;
        for (int i = 0; i < 1024; i++) {
            ret = read(fd, keys[i], 24);
            ret = read(fd, values[i], 4096);
            if (ret < 0) {
                printf("Cannot read from /dev/urandom\n");
            }
            kvMod->set(keys[i], 24, values[i], 4096, (i+1 == 1024));
        }
        char *readBuf;
        uint32_t valueSize;
        for (int i = 0; i < 1024; i++) {
            kvMod->get(keys[i], 24, &readBuf, valueSize);
            same = (valueSize == 4096 && memcmp(readBuf, values[i], 4096) == 0);
            if (!same) {
                printf("----------------------------------------\n");
                printf("valueSize %u\n", valueSize);
                printdiff(values[i], readBuf, 4096);
                printf("----------------------------------------\n");
                assert (same);
            }
            free(readBuf);
        }
        printf(">>>> Test %da (set new key-value pairs) passed\n", testid);
        
        printf(">>>> Test %db: Update some key-value pairs\n", testid);
        for (int i = 0; i < 1024; i++) {
            if (i % 100 < 30)
                continue;
            ret = read(fd, values[i], 4096);
            if (ret < 0) {
                printf("Cannot read from /dev/urandom\n");
            }
            kvMod->update(keys[i], 24, values[i], 4096, (i+1 == 1024));
        }
        for (int i = 0; i < 1024; i++) {
            kvMod->get(keys[i], 24, &readBuf, valueSize);
            same = (valueSize == 4096 && memcmp(readBuf, values[i], 4096) == 0);
            if (!same) {
                printf("----------------------------------------\n");
                printf("valueSize %u\n", valueSize);
                printdiff(values[i], readBuf, 4096);
                printf("----------------------------------------\n");
                assert (same);
            }
            free(readBuf);
        }
        printf(">>>> Test %db (update some key-value pairs) passed\n", testid);

        printf(">>>> Test %d (KV-store interface) passed.\n\n", testid);

        close(fd);

        delete storageMod;
        delete kvMod;
    }
#else //ifdef BLOCK_ITF
    delete storageMod;
    delete logMod;
    delete fileMetaMod;
    delete raidMod;
    delete segMetaMod;
    delete syncMod;
#endif //ifdef BLOCK_ITF
    printf ("%s\n", "========== Finish example tests ==========");

    return 0;
}
