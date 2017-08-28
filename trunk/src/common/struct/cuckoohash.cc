#include <cassert>

#include "../define.hh"
#include "cuckoohash.hh"
#include "keyvalue.hh"

#ifdef BLOCK_ITF

void CuckooHash::init( uint32_t power ) {
    this->size = ( uint32_t ) 1 << power;
    this->hashPower = power;
    this->hashMask = this->size - 1;
    this->buckets = ( Bucket * ) malloc( this->size * sizeof( struct Bucket ) );
    this->readBuf = ( char *) buf_malloc( 3 * DISK_BLKSIZE );

#ifdef KV_ON_MD
    this->m_md = 0;
#else /* ifdef KV_ON_MD */
    this->m_storageMod = 0;
#endif /* ifdef KV_ON_MD */

    this->kickCount = 0;

    if ( ! this->buckets ) {
        fprintf( stderr, "CuckooHash (init) Cannot initialize hashtable." );
        exit( 1 );
    }
    for (uint32_t i = 0; i < this->size; i++) {
        for (uint32_t j = 0; j < BUCKET_SIZE; j++) {
            this->buckets[i].addr[j] = INVALID_ADDR;
        }
    }
    //memset( this->buckets, 0, sizeof( struct Bucket ) * this->size );

    #ifdef CUCKOO_HASH_LOCK_OPT
        memset( this->keyver_array, 0, sizeof( keyver_array ) );
    #endif

    #ifdef CUCKOO_HASH_LOCK_FINEGRAIN
        for ( size_t i = 0; i < FG_LOCK_COUNT; i++ )
            pthread_spin_init( &this->fg_locks[ i ], PTHREAD_PROCESS_PRIVATE );
    #endif
}

size_t CuckooHash::indexHash( uint32_t hashValue ) {
    return ( hashValue >> ( 32 - this->hashPower ) );
}

uint8_t CuckooHash::tagHash( uint32_t hashValue ) {
    uint32_t r =  hashValue & TAG_MASK;
    return ( uint8_t ) r + ( r == 0 );
}

size_t CuckooHash::altIndex( size_t index, uint8_t tag ) {
    // 0x5bd1e995 is the hash constant from MurmurHash2
    return ( index ^ ( ( tag & TAG_MASK ) * 0x5bd1e995 ) ) & this->hashMask;
}

size_t CuckooHash::lockIndex( size_t i1, size_t i2, uint8_t tag ) {
    return i1 < i2 ? i1 : i2;
}

CuckooHash::CuckooHash() {
    this->init( HASHPOWER_DEFAULT );
}

CuckooHash::CuckooHash( uint32_t power ) {
    this->init( power );
}

CuckooHash::~CuckooHash() {
    free( this->buckets );
    free( this->readBuf );
}

uint64_t CuckooHash::find( char *key, uint8_t keySize ) {
    uint32_t hashValue = HashFunc::hash( key, keySize );
    uint8_t tag = this->tagHash( hashValue );
    size_t i1 = this->indexHash( hashValue );
    size_t i2 = this->altIndex( i1, tag );
    uint64_t result = INVALID_ADDR;

#ifdef CUCKOO_HASH_LOCK_OPT
    size_t lock = this->lockIndex( i1, i2, tag );
    uint32_t vs, ve;
TryRead:
    vs = READ_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
    this->fg_lock( i1, i2 );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
    volatile uint32_t tags1, tags2;
    tags1 = *( ( uint32_t * ) &( buckets[ i1 ] ) );
    tags2 = *( ( uint32_t * ) &( buckets[ i2 ] ) );
#endif

    for ( size_t j = 0; j < BUCKET_SIZE; j++ ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
        uint8_t ch = ( ( uint8_t * ) &tags1 )[ j ];
        if ( ch == tag )
#endif
        {
            if ( IS_SLOT_EMPTY( i1, j ) ) continue;

            uint64_t addr = buckets[ i1 ].addr[ j ];
            uint8_t keySize1 = readKey( addr );

            // key size not equal
            if ( keySize1 != keySize ) continue;

            // compare key of same size
            if ( memcmp( readBuf + sizeof( uint8_t ) + sizeof( uint32_t ), key, keySize ) == 0 ) {
                result = addr;
                break;
            }
        }
    }

    if ( result == INVALID_ADDR ) {
        for ( size_t j = 0; j < 4; j++ ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
            uint8_t ch = ( ( uint8_t * ) &tags2 )[ j ];
            if ( ch == tag )
#endif
            {
                if ( IS_SLOT_EMPTY( i2, j ) ) continue;

                uint64_t addr = buckets[ i2 ].addr[ j ];
                uint8_t keySize2 = readKey( addr );

                // key size not equal
                if ( keySize2 != keySize ) continue;

                // compare key of same size
                if ( memcmp( readBuf + sizeof( uint8_t ) + sizeof( uint32_t ), key, keySize ) == 0 ) {
                    result = addr;
                    break;
                }
            }
        }
    }

#ifdef CUCKOO_HASH_LOCK_OPT
    ve = READ_KEYVER( lock );

    if ( vs & 1 || vs != ve )
        goto TryRead;
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
    this->fg_unlock( i1, i2 );
#endif

    return result;
}

int CuckooHash::cpSearch( size_t depthStart, size_t *cpIndex ) {
    size_t depth = depthStart;
    while(
        ( this->kickCount < MAX_CUCKOO_COUNT ) &&
        ( depth >= 0 ) &&
        ( depth < MAX_CUCKOO_COUNT - 1 )
    ) {
        size_t *from = &( this->cp[ depth     ][ 0 ].bucket );
        size_t *to   = &( this->cp[ depth + 1 ][ 0 ].bucket );

        for ( size_t index = 0; index < CUCKOO_HASH_WIDTH; index++ ) {
            size_t i = from[ index ], j;
            for ( j = 0; j < BUCKET_SIZE; j++ ) {
                if ( IS_SLOT_EMPTY( i, j ) ) {
                    this->cp[ depth ][ index ].slot = j;
                    *cpIndex = index;
                    return depth;
                }
            }

            j = rand() % BUCKET_SIZE;
            this->cp[ depth ][ index ].slot = j;
            this->cp[ depth ][ index ].addr = this->buckets[ i ].addr[ j ];
#ifdef CUCKOO_HASH_ENABLE_TAG
            to[ index ] = this->altIndex( i, this->buckets[ i ].tags[ j ] );
#else
            char *key = readBuf + sizeof( uint8_t ) + sizeof( uint32_t );
            uint8_t keySize;

            // get back the key by reading from device
            keySize = readKey( this->buckets[ i ].addr[ j ] ); 

            uint32_t hashValue = HashFunc::hash( key, keySize );
            uint8_t tag = this->tagHash( hashValue );

            to[ index ] = this->altIndex( i, tag );
#endif
        }

        this->kickCount += CUCKOO_HASH_WIDTH;
        depth++;
    }

    fprintf( stderr, "CuckooHash (cpSearch) %u max cuckoo achieved, abort", this->kickCount );
    return -1;
}

int CuckooHash::cpBackmove( size_t depthStart, size_t index ) {
    int depth = depthStart;
    while( depth > 0 ) {
        size_t i1 = this->cp[ depth - 1 ][ index ].bucket,
               i2 = this->cp[ depth     ][ index ].bucket,
               j1 = this->cp[ depth - 1 ][ index ].slot,
               j2 = this->cp[ depth     ][ index ].slot;

        if ( this->buckets[ i1 ].addr[ j1 ] != this->cp[ depth - 1 ][ index ].addr )
            return depth;

        assert( IS_SLOT_EMPTY( i2, j2 ) );

#ifdef CUCKOO_HASH_LOCK_OPT
        size_t lock = this->lockIndex( i1, i2, 0 );
        INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
        this->fg_lock( i1, i2 );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
        this->buckets[ i2 ].tags[ j2 ] = this->buckets[ i1 ].tags[ j1 ];
        this->buckets[ i1 ].tags[ j1 ] = 0;
#endif

        this->buckets[ i2 ].addr[ j2 ] = this->buckets[ i1 ].addr[ j1 ];
        this->buckets[ i1 ].addr[ j1 ] = INVALID_ADDR;

#ifdef CUCKOO_HASH_LOCK_OPT
        INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
        this->fg_unlock( i1, i2 );
#endif

        depth--;
    }

    return depth;
}

int CuckooHash::cuckoo( int depth ) {
    int current;
    size_t index;

    this->kickCount = 0;

    while( 1 ) {
        current = this->cpSearch( depth, &index );
        if ( current < 0 )
            return -1;
        assert( index >= 0 );
        current = this->cpBackmove( current, index );
        if ( current == 0 )
            return index;

        depth = current - 1;
    }

    return -1;
}

uint8_t CuckooHash::readKey( uint64_t addr ) {
    readBuf[0] = 0;
    uint64_t keyOffset = sizeof(uint8_t) + sizeof(uint32_t);
#ifdef KV_ON_MD
    assert(m_md != 0);
    int ret = 0;
#ifdef DISK_DIRECT_IO
    uint64_t alignedOffset = addr / DISK_BLKSIZE * DISK_BLKSIZE;
    uint32_t readSize = 0;
    // read key (size)
    if (SingleIO) {
        readSize = (ReadBufferSize + (addr - alignedOffset) + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
    } else {
        readSize = (sizeof(uint8_t) + (addr - alignedOffset) + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
    }
    ret = pread(m_md, readBuf, readSize, alignedOffset); 
    if (ret < 0) {
        printf("Read key failed at %lu->%lu (%s) %d\n", addr, alignedOffset, strerror(errno), __LINE__);
    }
    if (SingleIO) {
        memmove(readBuf, readBuf + (addr - alignedOffset), ReadBufferSize);
    } else {
        memmove(readBuf, readBuf + (addr - alignedOffset), sizeof(uint8_t));
        int keySize = readBuf[0];
        // read key
        addr += keyOffset;
        alignedOffset = addr / DISK_BLKSIZE * DISK_BLKSIZE;
        readSize = (keySize + (addr - alignedOffset) + DISK_BLKSIZE - 1) / DISK_BLKSIZE * DISK_BLKSIZE;
        ret = pread(m_md, readBuf, readSize, alignedOffset); 
        memmove(readBuf + keyOffset, readBuf + (addr - alignedOffset), keySize);
        readBuf[0] = keySize;
    }
#else // ifdef DISK_DIRECT_IO
    if (SingleIO) {
        ret = pread(m_md, readBuf, ReadBufferSize, addr);
    } else {
        ret = pread(m_md, readBuf, sizeof(uint8_t) , addr);
        ret = pread(m_md, readBuf + keyOffset, readBuf[0], addr + keyOffset);
    }
#endif //ifdef DISK_DIRECT_IO
    if (ret < 0) {
        printf("Read key failed at %lu (%s)\n", addr, strerror(errno));
    }
#else /* ifdef KV_ON_MD */
    assert(m_storageMod != 0);
    if (SingleIO) {
        m_storageMod->read(readBuf, addr, ReadBufferSize);
    } else {
        m_storageMod->read(readBuf, addr, sizeof(uint8_t));
        m_storageMod->read(readBuf + keyOffset, addr + keyOffset, readBuf[0]);
    }
#endif /* ifdef KV_ON_MD */
    return readBuf[0];
}

bool CuckooHash::tryAdd( uint64_t addr, uint32_t valueSize, uint8_t tag, size_t i, size_t lock ) {
    for ( size_t j = 0; j < BUCKET_SIZE; j++ ) {
        if ( IS_SLOT_EMPTY( i, j ) ) {
#ifdef CUCKOO_HASH_LOCK_OPT
            INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
            this->fg_lock( i, i );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
            this->buckets[ i ].tags[ j ] = tag;
#endif
            this->buckets[ i ].addr[ j ] = addr;

#ifdef CUCKOO_HASH_LOCK_OPT
            INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
            this->fg_unlock( i, i );
#endif

            return true;
        }
    }

    return false;
}

bool CuckooHash::insert( char *key, uint8_t keySize, uint64_t addr, uint32_t valueSize ) {
    uint32_t hashValue = HashFunc::hash( key, keySize );
    uint8_t tag = this->tagHash( hashValue );
    size_t i1 = this->indexHash( hashValue );
    size_t i2 = this->altIndex( i1, tag );
    size_t lock = this->lockIndex( i1, i2, tag );

    if ( this->tryAdd( addr, valueSize, tag, i1, lock ) ) { return 1; }
    if ( this->tryAdd( addr, valueSize, tag, i2, lock ) ) { return 1; }

    int index;
    size_t depth = 0;
    for ( index = 0; index < CUCKOO_HASH_WIDTH; index++ ) {
        this->cp[ depth ][ index ].bucket = ( index < CUCKOO_HASH_WIDTH / 2 ) ? i1 : i2;
    }

    size_t j;
    index = this->cuckoo( depth );
    if ( index >= 0 ) {
        i1 = this->cp[ depth ][ index ].bucket;
        j  = this->cp[ depth ][ index ].slot;

        if ( this->buckets[ i1 ].addr[ j ] != INVALID_ADDR )
            fprintf( stderr, "CuckooHash (insert) Error: this->buckets[ i1 ].addr[ j ] != INVALID_ADDR." );

        if ( this->tryAdd( addr, valueSize, tag, i1, lock ) )
            return true;

        fprintf( stderr, "CuckooHash (insert) Error: i1 = %zu, i = %d.", i1, index );
    }

    fprintf( stderr, "CuckooHash (insert) Error: Hash table is full: power = %d. Need to increase power...", this->hashPower );
    return false;
}

#endif //ifdef BLOCK_ITF

