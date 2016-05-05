#ifndef __RDPCODING_HH__
#define __RDPCODING_HH__

#include "coding.hh"
#include "evenoddcoding.hh"

/**
 * RDP coding
 */
class RdpCoding: public EvenOddCoding {
public:

    RdpCoding();
    ~RdpCoding();

    vector<Chunk> encode(char* buf, CodeSetting codeSetting);
private:
    char** repairDataBlocks(vector<Chunk> chunks,
            vector<ChunkSymbols> reqSymbols, CodeSetting codeSetting,
            bool recovery = false);

};

#endif
