#include <assert.h>
#include <string.h>
#include <algorithm>
#include <set>
#include "common/define.hh"
#include "common/debug.hh"
#include "coding.hh"
#include "raid5coding.hh"

using namespace std;

Raid5Coding::Raid5Coding() {

}

Raid5Coding::~Raid5Coding() {

}

vector<Chunk> Raid5Coding::encode(char* buf, CodeSetting codeSetting) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    vector<Chunk> chunkList(n);
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    // prepare parity block
    chunkList[n - 1].chunkId = n - 1;
    chunkList[n - 1].length = chunkSize;
    chunkList[n - 1].buf = (char*) buf_calloc(1, chunkSize);

    for (int i = 0; i < k; i++) {
        chunkList[i].chunkId = i;
        chunkList[i].length = chunkSize;
        chunkList[i].buf = (char*) buf_malloc(chunkSize);
        memcpy(chunkList[i].buf, buf + i * chunkSize, chunkSize);

        // update parity block
        bitwiseXor(chunkList[n - 1].buf, chunkList[n - 1].buf, buf + i * chunkSize, chunkSize);
    }
    return chunkList;
}

void Raid5Coding::decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
        char* buf, CodeSetting codeSetting, off_len_t offLen) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    if (chunks.back().chunkId == n-1) { // if parity is involved
        int missingId = getMissingDataChunk(chunks, codeSetting);

        char* chunkBuf = (char *)buf_malloc(chunkSize);

        debug ("Repair needed for chunk %d\n", missingId);
        Chunk repair;   // only 1 repair for RAID-5
        repair.chunkId = missingId;
        repair.length = chunkSize;
        repair.buf = (char*) buf_calloc(1, chunkSize);

        for (Chunk chunk : chunks) {
            bitwiseXor(repair.buf, repair.buf, chunk.buf, chunkSize);
        }

        // add repaired chunk and sort chunks by chunkId
        // changes to chunks here will not go back to caller
        chunks.push_back(repair);
        sort(chunks.begin(), chunks.end(),
                [](Chunk a, Chunk b) {return a.chunkId < b.chunkId;});

        int offset = 0;
        for (int i = 0; i < k; i++) {
            memcpy(chunkBuf + offset, chunks[i].buf, chunkSize);
            offset += chunkSize;
        }

        memcpy (buf, chunkBuf + offLen.first, offLen.second);

        free(chunkBuf);
        free(repair.buf);

    } else {  // no failure, use RAID-0 logic
        StripingCoding::decode(reqSymbols, chunks, buf, codeSetting, offLen);
    }
}

vector<ChunkSymbols> Raid5Coding::getReqSymbols(vector<bool> chunkStatus,
        CodeSetting codeSetting, off_len_t offLen) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int fail = (int) count(chunkStatus.begin(), chunkStatus.end(), false);
    if (fail > n - k) {
        debug_error("Too many failures: %d\n", fail);
        exit(-1);
    }

    // use RAID-0 logic if no failures or parity chunk failure
    if (fail == 0 || (fail == 1 && chunkStatus.back() == false)) {
        chunkStatus.pop_back(); // ignore parity
        return StripingCoding::getReqSymbols(chunkStatus, codeSetting, offLen);
    }

    // when there is one failure, select first k available blocks
    vector<ChunkSymbols> reqSymbols;
    reqSymbols.reserve(k);
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    int selected = 0;
    for (int i = 0; i < (int) chunkStatus.size(); i++) {
        if (chunkStatus[i] == true) {
            off_len_t symbol = make_pair(0, chunkSize);
            vector<off_len_t> symbolList { symbol };
            ChunkSymbols chunkSymbols (i, symbolList);
            reqSymbols.push_back(chunkSymbols);

            if (++selected == k)
                break;
        }
    }

    return reqSymbols;
}

int Raid5Coding::getMissingDataChunk(vector<Chunk> chunks, CodeSetting codeSetting) {
    set<int> current;
    for (Chunk chunk : chunks) {
        current.insert(chunk.chunkId);
    }
    for (int i = 0; i < (int) codeSetting.k; i++) {
        if (!current.count(i))
            return i;
    }
    return -1;
}
