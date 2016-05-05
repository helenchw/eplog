#ifndef __RAID5CODING_HH__
#define __RAID5CODING_HH__

#include "coding.hh"
#include "stripingcoding.hh"

/**
 * RAID-5 coding
 */
class Raid5Coding: public StripingCoding {
public:

    Raid5Coding();
    ~Raid5Coding();

    vector<Chunk> encode(char* buf, CodeSetting codeSetting);

    void decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
            char* buf, CodeSetting codeSetting, off_len_t offLen);

    vector<ChunkSymbols> getReqSymbols(vector<bool> chunkStatus,
            CodeSetting codeSetting, off_len_t offLen);

private:
    int getMissingDataChunk (vector<Chunk> chunks, CodeSetting codeSetting);

};

#endif
