#ifndef CHUNKSYMBOL_HH_
#define CHUNKSYMBOL_HH_

#include <vector>
#include <common/define.hh>
using namespace std;

/**
 * Basic information for symbols within a chunk
 */
struct ChunkSymbols {
    chunk_id_t chunkId;                            /**< Id of a chunk within the segment */
    vector<off_len_t> offLens;                     /**< List of pairs of (offset, length) within the chunk */

    /**
     * Constructor
     * \param chunkId Id of the chunk
     * \param offLens a list of pairs of in-chunk offset and length of symbols
     */
    ChunkSymbols (chunk_id_t chunkId, vector<off_len_t> offLens) : chunkId (chunkId), offLens(offLens) {
    }

    /**
     * Constructor
     */
    ChunkSymbols () : chunkId(-1), offLens() {
    }
};

#endif /* CHUNKSYMBOL_HH_ */
