#ifndef __EVENODDCODING_HH__
#define __EVENODDCODING_HH__

#include "coding.hh"
#include "stripingcoding.hh"

/**
 * EVENODD coding
 */
class EvenOddCoding: public StripingCoding {
public:

    EvenOddCoding();
    ~EvenOddCoding();

    vector<Chunk> encode(char* buf, CodeSetting codeSetting);

    void decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
            char* buf, CodeSetting codeSetting, off_len_t offLen);

    vector<ChunkSymbols> getReqSymbols(vector<bool> chunkStatus,
            CodeSetting codeSetting, off_len_t offLen);

private:
    virtual char** repairDataBlocks(vector<Chunk> chunks,
            vector<ChunkSymbols> reqSymbols, CodeSetting codeSetting,
            bool recovery = false);

};

#endif
