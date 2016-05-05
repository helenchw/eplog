#include <iostream>
#include "common/define.hh"
#include "common/debug.hh"
#include "coding.hh"
#include "cauchycoding.hh"

extern "C" {
#include "jerasure.h"
#include "cauchy.h"
}

using namespace std;

CauchyCoding::CauchyCoding() {
    m_matrix = nullptr;
    m_bitmatrix = nullptr;
    m_schedule = nullptr;
}

CauchyCoding::CauchyCoding(CodeSetting codeSetting) {
    m_codeSetting = codeSetting;
    int n = codeSetting.n;
    int k = codeSetting.k;
    int w = codeSetting.w;
    int m = n-k;
    genCodeMatrix(n, k, m, w, &m_matrix, &m_bitmatrix);
    m_schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, m_bitmatrix);
}

CauchyCoding::~CauchyCoding() {
    free(m_matrix);
    free(m_bitmatrix);
    // TODO : free m_schedule (double pointer)
    free(m_schedule);
}

vector<Chunk> CauchyCoding::encode(char** buf, CodeSetting codeSetting) {
    /* coding parameters */
    const int k = codeSetting.k;
    const int n = codeSetting.n;
    const int m = n-k;
    const int w = codeSetting.w;  /* size of a symbol */

    // align all chunks to symbols 
    /* no. of bytes of a data chunk */
    const int chunkSize = roundTo(getChunkSize(k, codeSetting.isLog),w);  
    /* no. of data symbols within a chunk */
    const int symbolSize = chunkSize / w; 

    /* symbols buffers */
    char **data, **code;
    vector<Chunk> chunkList (n);
    int *matrix, *bitmatrix, **schedule;
    
    /* temp variables */
    int i;
    char ** curPos = NULL;

    if (m_matrix == nullptr || m_bitmatrix == nullptr) {
        debug("Generate Coding matrix (%d,%d,%d)\n",n,k,w);
        genCodeMatrix(n, k, m, w, &matrix, &bitmatrix);
    } else {
        matrix = m_matrix;
        bitmatrix = m_bitmatrix;
    }

    if (m_schedule == nullptr) {
        schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
    } else {
        schedule = m_schedule;
    }
    
    // init data buffer
    data = (char**) malloc(k*sizeof(char*));
    // TODO : divide and handle data here
    for (i = 0, curPos = buf; i < k; i++, curPos = curPos + chunkSize) { 
        chunkList[i].chunkId = i; 
        chunkList[i].length = chunkSize;
        chunkList[i].buf = buf[i];
        data[i] = chunkList[i].buf;
        debug("DATA buffer for encoding %d,%d,[%x], size %d\n",i,k,data[i][0],chunkSize);
    }
    // init code buffer
    code = (char**) malloc(m*sizeof(char*));
    for (i = 0; i < m; i++) { 
        chunkList[i+k].chunkId = i+k; 
        chunkList[i+k].length = chunkSize;
        chunkList[i+k].buf = buf[i+k];
        code[i] = chunkList[i+k].buf;
        debug("CODE buffer for encoding %d,%d,[%x], size %d\n",i,k,code[i][0],chunkSize);
    }

    debug("Start Encoding with matrix (%d,%d,%d)\n",n,k,w);
    jerasure_schedule_encode(k, m, w, schedule, data, code, chunkSize, symbolSize);
    debug("Finish Coding (%d,%d,%d)\n",n,k,w);

    if (m_matrix == nullptr) free(matrix);
    if (m_bitmatrix == nullptr) free (bitmatrix);
    if (m_schedule == nullptr) free (schedule);
    free(data);
    free(code);

    return chunkList;
}

// TODO : remove the assumption, buf size = seg size ? 
vector<Chunk> CauchyCoding::encode(char* buf, CodeSetting codeSetting) {
    /* coding parameters */
    const int k = codeSetting.k;
    const int n = codeSetting.n;
    const int m = n-k;
    const int w = codeSetting.w;  /* size of a symbol */

    // align all chunks to symbols 
    /* no. of bytes of a data chunk */
    const int chunkSize = roundTo(getChunkSize(k, codeSetting.isLog),w);  
    /* no. of data symbols within a chunk */
    const int symbolSize = chunkSize / w; 

    /* symbols buffers */
    char **data, **code;
    vector<Chunk> chunkList (n);
    int *matrix, *bitmatrix, **schedule;
    
    /* temp variables */
    int i;
    char * curPos = NULL;

    if (m_matrix == nullptr || m_bitmatrix == nullptr) {
        debug("Generate Coding matrix (%d,%d,%d)\n",n,k,w);
        genCodeMatrix(n, k, m, w, &matrix, &bitmatrix);
    } else {
        matrix = m_matrix;
        bitmatrix = m_bitmatrix;
    }

    if (m_schedule == nullptr) {
        schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
    } else {
        schedule = m_schedule;
    }
    
    // init data buffer
    data = (char**) malloc(k*sizeof(char*));
    // TODO : divide and handle data here
    for (i = 0, curPos = buf; i < k; i++, curPos = curPos + chunkSize) { 
        chunkList[i].chunkId = i; 
        chunkList[i].length = chunkSize;
        chunkList[i].buf = (char*) buf_calloc (sizeof(char),chunkSize);
        data[i] = chunkList[i].buf;
        debug("Init DATA buffer for encoding %d,%d,%p, size %d\n",i,k,curPos,chunkSize);
        memcpy(data[i], curPos, chunkSize);
    }
    // init code buffer
    code = (char**) malloc(m*sizeof(char*));
    for (i = 0; i < m; i++) { 
        chunkList[i+k].chunkId = i+k; 
        chunkList[i+k].length = chunkSize;
        chunkList[i+k].buf = (char*) buf_calloc (1, chunkSize);
        code[i] = chunkList[i+k].buf;
    }

    debug("Start Encoding with matrix (%d,%d,%d)\n",n,k,w);
    jerasure_schedule_encode(k, m, w, schedule, data, code, chunkSize, symbolSize);
    debug("Finish Coding (%d,%d,%d)\n",n,k,w);

    if (m_matrix == nullptr) free(matrix);
    if (m_bitmatrix == nullptr) free (bitmatrix);
    if (m_schedule == nullptr) free (schedule);
    free(data);
    free(code);

    return chunkList;
}

void CauchyCoding::decode(vector<ChunkSymbols> reqSymbols, 
        vector<Chunk> chunks, char* buf, CodeSetting codeSetting, off_len_t offLen) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int m = n-k;
    const int w = codeSetting.w;  /* size of a symbol */

    // align all chunks to symbols 
    /* no. of bytes of a data chunk */
    const int chunkSize = roundTo(getChunkSize(k, codeSetting.isLog),w);  
    /* no. of data symbols within a chunk */
    const int symbolSize = chunkSize / w; 

    /* symbols buffers */
    char **data, **code;
    vector<Chunk> chunkList (n);
    int *matrix, *bitmatrix;
    int *erasures, numErasures;

    const int startChunk = offLen.first / chunkSize;
    const int endChunk = (offLen.first + offLen.second -1) / chunkSize;
    int curOff = offLen.first, curLen = offLen.second, curPos = 0, len = 0;

    int i, j;
    vector<char*> newbuf;

    if (chunks.back().chunkId >= k) {
        // need to decode with parities
        if (chunks.size() < (uint32_t)k) {
            cerr << "Not enough blocks for decode " << chunks.size() << endl;
            exit(-1);
        }
        debug("Generate Coding matrix (%d,%d,%d)\n",n,k,w);

        if (m_matrix == nullptr || m_bitmatrix == nullptr) {
            genCodeMatrix(n, k, m, w, &matrix, &bitmatrix);
        } else {
            matrix = m_matrix;
            bitmatrix = m_bitmatrix;
        }
        
        data = (char**) malloc(k*sizeof(char*));
        code = (char**) malloc(m*sizeof(char*));
        numErasures = n - chunks.size(); 
        erasures = (int*) buf_calloc(numErasures + 1,sizeof(int));

        chunks.resize(n);

        // TODO : 2log(n) complexity, need improvement 
        // find the missing chunks' id, and order chunks in asc order
        // data blocks: ref. to buf; code blocks: allocate temp buf
        for (i = n - 1; i >= 0; i--) {
            if (chunks[i].chunkId != i && chunks[i].chunkId != -1) {
                debug ("swap %d with %d\n", chunks[i].chunkId, i);
                swap(chunks[i], chunks[chunks[i].chunkId]);
            } 
        }

        // update reference for decoding buffers
        for (i = 0, j=0; i < n; i++) {
            if (chunks[i].chunkId == -1) {
                if (chunks[i].buf != nullptr) { free(chunks[i].buf); }
                chunks[i].chunkId = i;
                chunks[i].length = chunkSize;
                chunks[i].buf = (char*) buf_calloc (chunkSize, sizeof(char));
                newbuf.push_back(chunks[i].buf);
                debug ("Chunk %d failed\n", i);
                erasures[j++] = i;
            }
            i < k? data[i] = chunks[i].buf: code[i-k] = chunks[i].buf;
        }

        erasures[j] = -1;
        debug("Start Decoding with matrix (%d,%d,%d)\n",n,k,w);
        jerasure_schedule_decode_lazy(k, m, w, bitmatrix, erasures, data, code, chunkSize, symbolSize, 1);
        debug("Finish Decoding with matrix (%d,%d,%d)\n",n,k,w);

        for (i = startChunk; i <= endChunk; i++) {
            // relative offset to each chunk
            curOff = (curOff - i * chunkSize > 0)? curOff - i * chunkSize: 0;
            assert(curOff >= 0);
            len = min(chunkSize - curOff, curLen);
            assert(len > 0);
            memcpy(buf + curPos, chunks[i].buf + curOff, len);
            curLen -= len;
            curPos += len;
        }

        free(erasures);
        // chunks are pass-by value, caller free chunks pass-in, 
        // decoder free chunks added
        for (char* chunkBuf : newbuf) {
            free(chunkBuf);
        }
        if (m_matrix == nullptr) free(matrix);
        if (m_bitmatrix == nullptr) free (bitmatrix);
        free(data);
        free(code);
    } else {
        // only data chunks are involved
        StripingCoding::decode(reqSymbols, chunks, buf, codeSetting, offLen);
    }
}

vector<ChunkSymbols> CauchyCoding::getReqSymbols(vector<bool> chunkStatus,
        CodeSetting codeSetting, off_len_t offLen) {

    vector<ChunkSymbols> reqSymbols;
    
    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    int i, selected;
    off_len_t symbolLen;
    vector<off_len_t> symbolLenList;
    ChunkSymbols chunkSymbol;
    bool raid0Decode = true;
    int startChunk = offLen.first / chunkSize;
    int endChunk = (offLen.first + offLen.second) / chunkSize;

    const int fail = (int) count(chunkStatus.begin(), chunkStatus.end(), false);
   
    assert ((int)chunkStatus.size() == n);

    if (fail > n - k) {
        cerr << "Too many failure to recover data (" << fail << ")\n";
        exit(-1);
    }

    for (i = 0; i < k; i++) {
        if (chunkStatus[i] == false && i >= startChunk && i <= endChunk) {
            raid0Decode = false;
            break;
        }
    }
    
    // direct read data (normal read)
    if (raid0Decode) {
        debug("%s from %d to %d\n", "Using raid0 decode", offLen.first, offLen.first+offLen.second);
        for (i = 0; i < n-k; i++) {
            chunkStatus.pop_back();     // ignore m parities
        }
        return StripingCoding::getReqSymbols(chunkStatus, codeSetting, offLen);
    }

    reqSymbols.reserve(k);

    for (i = 0, selected = 0; i < n; ++i) {
        debug ("Chunk i = %d, status = %s\n", i, chunkStatus[i]?"true":"false");
        if (chunkStatus[i] == true) {
            symbolLen = make_pair(0, chunkSize);
            symbolLenList = { symbolLen };
            chunkSymbol.chunkId = i;
            chunkSymbol.offLens = symbolLenList;
            reqSymbols.push_back(chunkSymbol);
            if (++selected == k) {
                    break;
            }
        }
    }

    return reqSymbols;
}

void CauchyCoding::genCodeMatrix(const uint32_t n, const uint32_t k, 
        const uint32_t m, const uint32_t w, int **matrix, 
        int **bitmatrix) {

    // check coding parameters
    if (k <= 0 || m <= 0 || w <= 0 || w > 32 || 
            (w < 30 && (k+m) > (uint32_t)(1 << w))) {
        cerr << "Bad parameters for cauchy coding\n";
        // TODO: handle error
        exit(-1);
    }

    // generate the coding matrix
    *matrix = cauchy_good_general_coding_matrix(k, m, w);    
    if (matrix == NULL) {
        cerr << "Couldn't make cauchy coding matrix k,m,w (" << k << "," << m << "," << w << ")\n";
        exit(-1);
    }
    *bitmatrix = jerasure_matrix_to_bitmatrix(k, m, w, *matrix);
}
