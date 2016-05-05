#ifndef STRIPINGCODING_HH_
#define STRIPINGCODING_HH_

#include "coding.hh"

/**
 * Basic interface for striping codings
 */
class StripingCoding: public Coding {
public:

    StripingCoding();
    ~StripingCoding();

    vector<Chunk> encode (char** buf, CodeSetting codeSetting);

    vector<Chunk> encode(char* buf, CodeSetting codeSetting);

    void decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
            char* buf, CodeSetting codeSetting, off_len_t offLen);

    vector<ChunkSymbols> getReqSymbols(vector<bool> chunkStatus,
            CodeSetting codeSetting, off_len_t offLen);

protected:
    uint32_t roundTo(uint32_t numToRound, uint32_t multiple);
};


#endif /* STRIPINGCODING_HH_ */
