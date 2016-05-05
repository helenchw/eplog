#ifndef BITMAP_HH_ 
#define BITMAP_HH_

#include <mutex>
#include <vector>

#include "common/define.hh"

using namespace std;

/**
  * Bitmap
  */
class BitMap {
public:
    /**
     * Constructor
     */
    BitMap();

    /**
     * Constructor
     * \param size size of the bitmap
     */
    BitMap(int size);

    /**
     * Destructor
     */
    virtual ~BitMap();

    /**
     * Return the bit corresponding to the address
     * \param addr the address 
     * \return the bit representing the address
     */
    bool getBit(int addr);

    /**
     * Set the bit corresponding to the address to 1
     * \param addr the address 
     */
    void setBit(int addr);

    /**
     * Set the bit corresponding to the address to 0
     * \param addr the address 
     */
    void clearBit(int addr);

    /**
     * Set all the bits in bitmap to 1
     */
    void setAllOne();

    /**
     * Search for the first bit 0 from writeFront and set it to 1
     * \param addr starting address for the search (inclusive)
     * \return address of the bit set to 1, -1 means no bits can be set
     */
    int getFirstZeroAndFlip(int addr = 0);

    /**
     * Search for the first bit sequence of 0 of len from writeFront and set them to 1
     * \param addr starting address for the search (inclusive)
     * \param len length of the sequence
     * \return starting address of the bit set to 1, -1 means no bits can be set
     */
    int getFirstZerosAndFlip(int addr = 0, int len = 1);

    /**
     * Search for the first bit 0 from an address in the bitmap
     * \param addr starting address for the search (inclusive)
     * \return first address of the bit 0, -1 means no 0 is found
     */
    int getFirstZeroSince(int addr);

    /**
     * Set the n bits from an address to 0
     * \param addr starting address for bit setting (inclusive)
     * \param n number of bits to set
     */
    void clearBitRange(int addr, int n);

    /**
     * Get all bit positions that are set to 1
     * \return a list of (starting address, length) pair indicating the ranges of bit 1
     */
    vector<off_len_t> getAllOne();

private:
    ULL* m_bv;                               /**< Bitmap*/
    int m_size;                              /**< Size of bitmap*/
    int m_num;                               /**< Number of internal bitmap units */

    static const int m_unit;                 /**< Size of a bitmap unit */
    static const ULL m_onemask;              /**< "all 1s" bitmap unit mask */
    static const ULL m_bitmask[];            /**< Bitmap mask array for setting 1s*/
    static const ULL m_clearmask[];          /**< Bitmap mask array for setting 0s*/

    
    mutex* m_bitmapMutex;                    /**< Internal lock for bitmap operations */

    void clearBitInternal(int addr);
};

#endif /* BITMAP_HH_ */
