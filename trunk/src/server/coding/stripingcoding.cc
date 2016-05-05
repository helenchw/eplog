#include <assert.h>
#include <string.h>
#include <algorithm>
#include "common/define.hh"
#include "common/debug.hh"
#include "coding.hh"
#include "stripingcoding.hh"

using namespace std;

StripingCoding::StripingCoding() {

}

StripingCoding::~StripingCoding() {

}

vector<Chunk> StripingCoding::encode(char** buf, CodeSetting codeSetting) {

    const int chunkSize = getChunkSize(codeSetting.k, codeSetting.isLog);
    char* interBuf = (char*) buf_malloc (codeSetting.k * chunkSize);
    // using first chunk by default
    for (int i = 0; i < codeSetting.k; i++) {
        memcpy(interBuf + (i*chunkSize), buf[i] + 0, chunkSize);
    }

    vector<Chunk> ret = encode(interBuf, codeSetting);

    free(interBuf);
    return ret;
}

vector<Chunk> StripingCoding::encode(char* buf, CodeSetting codeSetting) {

    const int k = codeSetting.k;
    vector<Chunk> chunkList(k);
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    for (int i = 0; i < k; i++) {

        chunkList[i].chunkId = i;
        chunkList[i].length = chunkSize;
        chunkList[i].buf = (char*) buf_malloc(chunkSize);

        memcpy(chunkList[i].buf, buf + i * chunkSize, chunkSize);
    }

    return chunkList;
}

void StripingCoding::decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
        char* buf, CodeSetting codeSetting, off_len_t offLen) {

    assert (reqSymbols.size() == chunks.size());
    int bufOffset = 0;
    for (int i = 0; i < (int)reqSymbols.size(); i++){
        assert (reqSymbols[i].chunkId == chunks[i].chunkId);
        assert (reqSymbols[i].offLens.size() == 1); // for RAID-0
        const int ofs = reqSymbols[i].offLens[0].first;
        const int len = reqSymbols[i].offLens[0].second;
        debug ("bufOffset = %d, len = %d\n", bufOffset, len);
        memcpy(buf + bufOffset, chunks[i].buf + ofs, len);
        bufOffset += len;
    }
}

vector<ChunkSymbols> StripingCoding::getReqSymbols(vector<bool> chunkStatus,
        CodeSetting codeSetting, off_len_t offLen) {

    assert (offLen.second > 0);

    const int k = codeSetting.k;
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    const int startChunk = offLen.first / chunkSize;
    const int endChunk = (offLen.first + offLen.second - 1) / chunkSize;

    assert (endChunk >= startChunk);

    vector<ChunkSymbols> reqSymbols;

    int curOff = offLen.first;
    int curLen = offLen.second;
    for (int i = startChunk; i <= endChunk; i++) {
        // if check is dead
        if (!chunkStatus[i]) {
            debug_error("No failure is accepted for RAID-0, failed chunk = %d\n", i);
            exit(-1);
        }
        debug ("startChunk = %d, endChunk = %d, i = %d\n", startChunk, endChunk, i);
        const int off = curOff - chunkSize * i;
        const int len = min (curLen, chunkSize * (i+1) - curOff); // curLen vs len to chunk end
        debug ("curOff = %d, curLen = %d, len_to_end = %d\n", curOff, curLen, chunkSize * (i+1) - curOff);

        debug ("off = %d, len = %d\n", off, len);
        assert (off >= 0 && len >= 0);
        off_len_t symbol = make_pair(off, len);
        vector<off_len_t> symbolList { symbol }; // only one symbol for RAID-0
        ChunkSymbols chunkSymbols (i,symbolList);
        reqSymbols.push_back(chunkSymbols);

        curOff = curOff + len;
        curLen -= len;
    }

    return reqSymbols;
}

uint32_t StripingCoding::roundTo(uint32_t numToRound, uint32_t multiple) {
    if (multiple == 0) { return numToRound; }
    
    uint32_t remainder = numToRound % multiple;
    if (remainder == 0) { return numToRound; }
    return numToRound + multiple - remainder;
}
