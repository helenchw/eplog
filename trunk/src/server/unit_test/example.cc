#include <chrono>
#include "common/debug.hh"
#include "common/define.hh"
#include "common/configmod.hh"
#include "server/storagemod.hh"
#include "server/segmetamod.hh"
#include "server/gcmod.hh"
typedef chrono::high_resolution_clock Clock;
typedef chrono::milliseconds milliseconds;
int segmentSize, pageSize, pagePerBlock, blockPerSeg, blockSize;

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
void printdiff(char* buf, char* buf1) {
    int start = -1, end = -1;
    for (int i = 0; i < segmentSize; i++) {
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
    DiskInfo disk1 (0, "disk1",  1048576ULL * 10);
    DiskInfo disk2 (1, "disk2",  1048576ULL * 10);
    DiskInfo disk3 (2, "disk3",  1048576ULL * 10);
    DiskInfo disk4 (3, "disk4",  1048576ULL * 10);
    DiskInfo disk5 (4, "disk5",  1048576ULL * 10);
    DiskInfo disk6 (5, "disk6",  1048576ULL * 10);
    DiskInfo disk7 (6, "disk7",  1048576ULL * 10, true);
    DiskInfo disk8 (7, "disk8",  1048576ULL * 10, true);
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
        printf ("\n%s\n", ">>> Test 1: Write a file");
        char *buf = (char*) malloc (sizeof(char)*segmentSize);
        int fd = open("/dev/urandom", O_RDONLY);
        int ret = read(fd, buf, segmentSize);
        if (ret < 0) {
            printf("Cannot read from /dev/urandom!\n");
        }

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
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf ("%s\n", ">>> Test 1 (write a new file) passed.");

    // TEST2: modify
        printf("\n%s\n", ">>> Test 2: Overwrite the file");
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
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf("%s\n", ">>> Test 2 (overwrite the file) passed.");
        free(buf);
        free(buf1);
        free(obuf);
    }
    
    {
    // TEST3: 2M file, modify, read
        printf ("\n%s\n", ">>> Test 3a: Write another new file");
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
        printf ("%s\n", ">>> Test 3a (write another new file) passed.");

        printf ("%s\n", ">>> Test 3b: Overwrite the file");
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
        printdiff(buf, buf1);
        printdiff(buf + segmentSize, buf1 + segmentSize);
            same = (memcmp(buf, buf1, segmentSize * 2) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf, buf1);
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
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf ("%s\n", ">>> Test 3b (overwrite the file) passed.");

        printf ("%s\n", ">>> Test 3c: Overwrite the file");

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
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf ("%s\n", ">>> Test 3c (overwrite the file) passed.");

        printf ("%s\n", ">>> Test 3c: Overwrite the file");
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
        byteRead = storageMod->read(buf1, 0, segmentSize * 2);
#endif
        printf("Whole segment, byteRead = %d\n", byteRead);
        printdiff(buf, buf1);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf ("%s\n", ">>> Test 3d (overwrite the file) passed.");

        printf ("%s\n", ">>> Test 3e: Overwrite the file");
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
        printdiff(buf, buf1);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }
        printf ("%s\n", ">>> Test 3e (overwrite the file) passed.");

        printf ("%s\n", ">>> Test 3f: Partial read");
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
                printdiff(buf, buf1);
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
                printdiff(buf, buf1);
                printf("----------------------------------------\n");
                assert (same);
            }
        }
        printf ("%s\n", ">>> Test 3e (partial read) passed.");
        
        printf ("\n%s\n", ">>> Repeat Test 3a-f");
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
            printdiff(buf, buf1);
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
            printdiff(buf, buf1);
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
            printdiff(buf, buf1);
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
        printdiff(buf, buf1);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1);
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
        printdiff(buf, buf1);
        same = (memcmp(buf, buf1, segmentSize * 2) == 0);
        if ( ! same ) {
            printf("----------------------------------------\n");
            printdiff(buf, buf1);
            printf("----------------------------------------\n");
            assert (same);
        }

        memset(buf1, 0, segmentSize * 2);
        if (blockPerSeg < 2) {
#ifndef BLOCK_ITF
            byteRead = storageMod->read("testfile1", buf1, blockSize, blockSize*(blockPerSeg-1));
#else
            byteRead = storageMod->read(buf1, segmentSize * 2 + blockSize, blockSize*(blockPerSeg-1));
#endif
            printf("[REPEAT] byteRead = %d\n", byteRead);
            same = (memcmp(buf + blockSize, buf1, blockSize*(blockPerSeg-1)) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf, buf1);
                printf("----------------------------------------\n");
                assert (same);
            }
        } else {
#ifndef BLOCK_ITF
            byteRead = storageMod->read("testfile1", buf1, blockSize, blockSize*(blockPerSeg -2));
#else
            byteRead = storageMod->read(buf1, segmentSize * 2 + blockSize, blockSize*(blockPerSeg -2));
#endif
            printf("[REPEAT] byteRead = %d\n", byteRead);
            same = (memcmp(buf + blockSize, buf1, blockSize*(blockPerSeg -2)) == 0);
            if ( ! same ) {
                printf("----------------------------------------\n");
                printdiff(buf, buf1);
                printf("----------------------------------------\n");
                assert (same);
            }
        }
        printf("%s\n", "[REPEAT] Test 3a-f passed.\n");

    }
    
    delete storageMod;
    printf ("%s\n", "========== Finish example tests ==========");

    return 0;
}
