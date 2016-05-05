#ifndef __CODING_HH__
#define __CODING_HH__

#include <vector>
#include <utility>
#include "common/define.hh"
#include "common/struct/chunk.hh"
#include "common/struct/codesetting.hh"
#include "common/struct/segmentmetadata.hh"
#include "common/struct/chunksymbols.hh"
#include "common/configmod.hh"

using namespace std;

/**
 * Basic interface for coding schemes
 */
class Coding {
public:

    Coding() {
    }

    virtual ~Coding() {
    }

    virtual vector<Chunk> encode(char* buf, CodeSetting codeSetting) = 0;

    virtual vector<Chunk> encode(char** buf, CodeSetting codeSetting) = 0;

    virtual void decode(vector<ChunkSymbols> chunkSymbols,
            vector<Chunk> chunks, char* buf, CodeSetting codeSetting, off_len_t offLen) = 0;

    virtual vector<ChunkSymbols> getReqSymbols(vector<bool> chunkStatus,
            CodeSetting codeSetting, off_len_t offLen) = 0;

    static inline void bitwiseXor(char* result, char* srcA, char* srcB,
            uint32_t length) {

        uint64_t* srcA64 = (uint64_t*) srcA;
        uint64_t* srcB64 = (uint64_t*) srcB;
        uint64_t* result64 = (uint64_t*) result;

        uint64_t xor64Count = length / sizeof(uint64_t);
        uint64_t offset = 0;

        // finish all the word-by-word XOR
        for (uint64_t i = 0; i < xor64Count; i++) {
            result64[i] = srcA64[i] ^ srcB64[i];
            offset += sizeof(uint64_t); // processed bytes
        }

        // finish remaining byte-by-byte XOR
        for (uint64_t j = offset; j < length; j++) {
            result[j] = srcA[j] ^ srcB[j];
        }

    }

    // TODO : relationship between block vs. chunk, e.g. 1 chunk = multiple blocks
    int getChunkSize(int k, bool isLog = false) const {
        int logSegmentSize = ConfigMod::getInstance().getLogSegmentSize();
        int segmentSize = ConfigMod::getInstance().getSegmentSize();
		assert(segmentSize % k == 0 || logSegmentSize % k == 0);
		if (isLog) {
			return logSegmentSize / k;
		} else {
			return segmentSize / k;
		}
        //return ConfigMod::getInstance().getBlockSize();
    }

    //int getChunkSize(int k) const {
    //    const int segmentSize = ConfigMod::getInstance().getSegmentSize();
    //    assert(segmentSize % k == 0);
    //    return segmentSize / k;
    //}
};

#endif
