#ifndef __CAUCHYCODING_HH__
#define __CAUCHYCODING_HH__

#include "coding.hh"
#include "stripingcoding.hh"

using namespace std;

/**
 * Cauchy Reed-Solomon codes
 */
class CauchyCoding: public StripingCoding {
public:

    CauchyCoding();
    CauchyCoding(CodeSetting codeSetting);
    ~CauchyCoding();

    vector<Chunk> encode(char** buf, CodeSetting codeSetting);

    vector<Chunk> encode(char* buf, CodeSetting codeSetting);

    void decode(vector<ChunkSymbols> chunkSymbols,
            vector<Chunk> chunks, char* buf, CodeSetting codeSetting, 
            off_len_t offLen);

    vector<ChunkSymbols> getReqSymbols(vector<bool> chunkStatus,
            CodeSetting codeSetting, off_len_t offLen);

private:
    void genCodeMatrix(const uint32_t n, const uint32_t k, 
            const uint32_t m, const uint32_t w, int **matrix,
            int **bitmatrix);

    CodeSetting m_codeSetting;
    int *m_matrix;
    int *m_bitmatrix;
    int **m_schedule;
};

#endif /* __CAUCHYCODING_HH__ */
