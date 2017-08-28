#ifndef __COMMON_CUCKOO_HASH_HH__
#define __COMMON_CUCKOO_HASH_HH__

#include <stdint.h>
#include <pthread.h>
#include "../server/storagemod.hh"
#include "../define.hh"

#ifdef BLOCK_ITF

/********** Configuration **********/
// #define CUCKOO_HASH_LOCK_OPT
#define CUCKOO_HASH_LOCK_FINEGRAIN

// #define CUCKOO_HASH_ENABLE_TAG

/********** Constants **********/
#ifdef CUCKOO_HASH_LOCK_OPT
	#define KEYVER_COUNT ( ( uint32_t ) 1 << 13 )
	#define KEYVER_MASK ( KEYVER_COUNT - 1 )
#endif
#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
	#define FG_LOCK_COUNT ( ( uint32_t ) 1 << 13 )
	#define FG_LOCK_MASK  ( FG_LOCK_COUNT - 1 )
#endif

#define CUCKOO_HASH_WIDTH 1
#define HASHPOWER_DEFAULT 25
#define MAX_CUCKOO_COUNT 500
#define BUCKET_SIZE      4
#define TAG_MASK         ( ( uint32_t ) 0x000000FF )

/********** Data structures **********/
struct Bucket {
#ifdef CUCKOO_HASH_ENABLE_TAG
	uint8_t tags[ BUCKET_SIZE ]; // Tag: 1-byte summary of key
	char reserved[ 4 ];
#endif
    //uint32_t valueSize [ BUCKET_SIZE ];    // size of value
    uint64_t addr [ BUCKET_SIZE ];
} __attribute__( ( __packed__ ) );

/********** Macros **********/
#ifdef CUCKOO_HASH_ENABLE_TAG
	#define IS_SLOT_EMPTY( i, j ) ( buckets[ i ].tags[ j ] == 0 )
#else
	#define IS_SLOT_EMPTY( i, j ) ( buckets[ i ].addr[ j ] == INVALID_ADDR )
#endif
#define IS_TAG_EQUAL( bucket, j, tag ) ( ( bucket.tags[ j ] & TAG_MASK ) == tag )

#ifdef CUCKOO_HASH_LOCK_OPT
	#define READ_KEYVER( lock ) \
		__sync_fetch_and_add( &this->keyver_array[ lock & KEYVER_MASK ], 0 )

	#define INCR_KEYVER( lock ) \
		__sync_fetch_and_add( &this->keyver_array[ lock & KEYVER_MASK ], 1 )
#endif

/********** CuckooHash class definition **********/
class CuckooHash {
private:
	uint32_t size;
	uint32_t hashPower;
	uint32_t hashMask;
	struct Bucket *buckets;

#ifdef DISK_DIRECT_IO
    const static uint32_t ReadBufferSize = 2 * DISK_BLKSIZE;
#else //ifdef DISK_DIRECT_IO
    const static uint32_t ReadBufferSize = sizeof( uint8_t ) + sizeof( uint32_t ) + 256;
#endif //ifdef DISK_DIRECT_IO
    char* readBuf; /* buffer for holding keySize, valueSize, key for comparison */
    const static bool SingleIO = false;

	int kickCount;
	struct {
		size_t bucket;
		size_t slot;
		uint64_t addr;
	} cp[ MAX_CUCKOO_COUNT ][ CUCKOO_HASH_WIDTH ];

	#ifdef CUCKOO_HASH_LOCK_OPT
		uint32_t keyver_array[ KEYVER_COUNT ];
	#endif
	#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		pthread_spinlock_t fg_locks[ FG_LOCK_COUNT ];
	#endif

	void init( uint32_t power );

	size_t indexHash( uint32_t hashValue );
	uint8_t tagHash( uint32_t hashValue );
	size_t altIndex( size_t index, uint8_t tag );
	size_t lockIndex( size_t i1, size_t i2, uint8_t tag );

	//uint64_t *tryRead( char *key, uint8_t keySize, uint8_t tag, size_t i );
	bool tryAdd( uint64_t addr, uint32_t valueSize, uint8_t tag, size_t i, size_t lock );
	//bool tryDel( char *key, uint8_t keySize, uint8_t tag, size_t i, size_t lock );

	int cpSearch( size_t depthStart, size_t *cpIndex );
	int cpBackmove( size_t depthStart, size_t index );
	int cuckoo( int depth );

    inline uint8_t readKey( uint64_t addr );

	#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		void fg_lock( uint32_t i1, uint32_t i2 ) {
			uint32_t j1, j2;
			j1 = i1 & FG_LOCK_MASK;
			j2 = i2 & FG_LOCK_MASK;

			if ( j1 < j2 ) {
				pthread_spin_lock( &fg_locks[ j1 ] );
				pthread_spin_lock( &fg_locks[ j2 ] );
			} else if ( j1 > j2 ) {
				pthread_spin_lock( &fg_locks[ j2 ] );
				pthread_spin_lock( &fg_locks[ j1 ] );
			} else {
				pthread_spin_lock( &fg_locks[ j1 ] );
			}
		}

		void fg_unlock( uint32_t i1, uint32_t i2 ) {
			uint32_t j1, j2;
			j1 = i1 & FG_LOCK_MASK;
			j2 = i2 & FG_LOCK_MASK;

			if ( j1 < j2 ) {
				pthread_spin_unlock( &fg_locks[ j2 ] );
				pthread_spin_unlock( &fg_locks[ j1 ] );
			} else if ( j1 > j2 ) {
				pthread_spin_unlock( &fg_locks[ j1 ] );
				pthread_spin_unlock( &fg_locks[ j2 ] );
			} else {
				pthread_spin_unlock( &fg_locks[ j1 ] );
			}
		}
	#endif

public:
	CuckooHash();
	CuckooHash( uint32_t power );
	~CuckooHash();

	void setKeySize( uint8_t keySize );

    // find the object on-disk location
	uint64_t find( char *key, uint8_t keySize );
    // insert using a buffered key with its object on-disk location as value
	bool insert( char *key, uint8_t keySize, uint64_t addr, uint32_t valueSize );
    // delete using a buffered key
	//void del( char *key, uint8_t keySize );

    /* hack for key checking */
#ifdef KV_ON_MD
    int m_md;
#else /* ifdef KV_ON_MD */
    StorageMod *m_storageMod;
#endif /* ifdef KV_ON_MD */

};

#endif //ifdef BLOCK_ITF

#endif
