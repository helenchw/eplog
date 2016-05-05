#ifndef CHUNK_HH_
#define CHUNK_HH_
#include <common/define.hh>

/**
 * Basic metadata for chunks
 */
struct Chunk {
    chunk_id_t chunkId;      /**< Id of a chunk within a segment */
    char* buf;               /**< Data buffer */
    int length;              /**< Size of a chunk */

    /**
     * Constructor
     */
    Chunk() {
        chunkId = -1;
        buf = nullptr;
        length = -1;
    }
};

#endif /* CHUNK_HH_ */
