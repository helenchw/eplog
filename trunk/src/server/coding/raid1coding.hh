#ifndef __RAID1CODING_HH__
#define __RAID1CODING_HH__

#include "coding.hh"
#include "stripingcoding.hh"

/**
 * N-way mirroring
 */
class Raid1Coding: public StripingCoding {
public:

    Raid1Coding();
    ~Raid1Coding();

    vector<Chunk> encode(char* buf, CodeSetting codeSetting);

    void decode(vector<ChunkSymbols> chunkSymbols, vector<Chunk> chunks,
            char* buf, CodeSetting codeSetting, off_len_t offLen);

    vector<ChunkSymbols> getReqSymbols(vector<bool> chunkStatus,
            CodeSetting codeSetting, off_len_t offLen);

};

#endif
