#include <assert.h>
#include <string.h>
#include <algorithm>
#include "common/define.hh"
#include "common/debug.hh"
#include "coding.hh"
#include "raid1coding.hh"

using namespace std;

Raid1Coding::Raid1Coding(){

}

Raid1Coding::~Raid1Coding() {

}

vector<Chunk> Raid1Coding::encode(char* buf, CodeSetting codeSetting) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    vector<Chunk> chunkList(n);
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    for (int i = 0; i < n; i++) {

        chunkList[i].chunkId = i;
        chunkList[i].length = chunkSize;
        chunkList[i].buf = (char*) buf_malloc(chunkSize);

        memcpy(chunkList[i].buf, buf, chunkSize);
    }

    return chunkList;
}

void Raid1Coding::decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
        char* buf, CodeSetting codeSetting, off_len_t offLen) {

    assert (reqSymbols.size() == chunks.size());

    // simply copy the first chunk to the segment buf
    memcpy(buf, chunks[0].buf, reqSymbols[0].offLens[0].second);
}

vector<ChunkSymbols> Raid1Coding::getReqSymbols(vector<bool> chunkStatus,
        CodeSetting codeSetting, off_len_t offLen) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int fail = (int) count(chunkStatus.begin(), chunkStatus.end(), false);
    if (fail > n-k) {
        debug_error("Too many failures: %d\n", fail);
        exit(-1);
    }

    // for Raid1 Coding, require first available block
    vector<ChunkSymbols> reqSymbols(1);
    const int chunkSize = getChunkSize(k, codeSetting.isLog);
    assert (offLen.first + offLen.second <= chunkSize);

    for (int i = 0; i < (int)chunkStatus.size(); i++) {
        if (chunkStatus[i] == true) {
            off_len_t symbol = make_pair(offLen.first, offLen.second);
            vector<off_len_t> symbolList { symbol };
            ChunkSymbols chunkSymbols (i, symbolList);
            reqSymbols[0] = chunkSymbols;
            break;
        }
    }

    return reqSymbols;
}
