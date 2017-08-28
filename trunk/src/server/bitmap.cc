#include "server/bitmap.hh"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "common/debug.hh"
#include "common/configmod.hh"

const int BitMap::m_unit = sizeof(ULL)<<3;

const ULL BitMap::m_onemask = 0xFFFFFFFFFFFFFFFFULL;

const ULL BitMap::m_bitmask[] = {
    0x0000000000000001ULL,0x0000000000000002ULL,0x0000000000000004ULL,0x0000000000000008ULL,
    0x0000000000000010ULL,0x0000000000000020ULL,0x0000000000000040ULL,0x0000000000000080ULL,
    0x0000000000000100ULL,0x0000000000000200ULL,0x0000000000000400ULL,0x0000000000000800ULL,
    0x0000000000001000ULL,0x0000000000002000ULL,0x0000000000004000ULL,0x0000000000008000ULL,
    0x0000000000010000ULL,0x0000000000020000ULL,0x0000000000040000ULL,0x0000000000080000ULL,
    0x0000000000100000ULL,0x0000000000200000ULL,0x0000000000400000ULL,0x0000000000800000ULL,
    0x0000000001000000ULL,0x0000000002000000ULL,0x0000000004000000ULL,0x0000000008000000ULL,
    0x0000000010000000ULL,0x0000000020000000ULL,0x0000000040000000ULL,0x0000000080000000ULL,
    0x0000000100000000ULL,0x0000000200000000ULL,0x0000000400000000ULL,0x0000000800000000ULL,
    0x0000001000000000ULL,0x0000002000000000ULL,0x0000004000000000ULL,0x0000008000000000ULL,
    0x0000010000000000ULL,0x0000020000000000ULL,0x0000040000000000ULL,0x0000080000000000ULL,
    0x0000100000000000ULL,0x0000200000000000ULL,0x0000400000000000ULL,0x0000800000000000ULL,
    0x0001000000000000ULL,0x0002000000000000ULL,0x0004000000000000ULL,0x0008000000000000ULL,
    0x0010000000000000ULL,0x0020000000000000ULL,0x0040000000000000ULL,0x0080000000000000ULL,
    0x0100000000000000ULL,0x0200000000000000ULL,0x0400000000000000ULL,0x0800000000000000ULL,
    0x1000000000000000ULL,0x2000000000000000ULL,0x4000000000000000ULL,0x8000000000000000ULL
};

const ULL BitMap::m_clearmask[] = {
    0xFFFFFFFFFFFFFFFEULL,0xFFFFFFFFFFFFFFFDULL,0xFFFFFFFFFFFFFFFBULL,0xFFFFFFFFFFFFFFF7ULL,
    0xFFFFFFFFFFFFFFEFULL,0xFFFFFFFFFFFFFFDFULL,0xFFFFFFFFFFFFFFBFULL,0xFFFFFFFFFFFFFF7FULL,
    0xFFFFFFFFFFFFFEFFULL,0xFFFFFFFFFFFFFDFFULL,0xFFFFFFFFFFFFFBFFULL,0xFFFFFFFFFFFFF7FFULL,
    0xFFFFFFFFFFFFEFFFULL,0xFFFFFFFFFFFFDFFFULL,0xFFFFFFFFFFFFBFFFULL,0xFFFFFFFFFFFF7FFFULL,
    0xFFFFFFFFFFFEFFFFULL,0xFFFFFFFFFFFDFFFFULL,0xFFFFFFFFFFFBFFFFULL,0xFFFFFFFFFFF7FFFFULL,
    0xFFFFFFFFFFEFFFFFULL,0xFFFFFFFFFFDFFFFFULL,0xFFFFFFFFFFBFFFFFULL,0xFFFFFFFFFF7FFFFFULL,
    0xFFFFFFFFFEFFFFFFULL,0xFFFFFFFFFDFFFFFFULL,0xFFFFFFFFFBFFFFFFULL,0xFFFFFFFFF7FFFFFFULL,
    0xFFFFFFFFEFFFFFFFULL,0xFFFFFFFFDFFFFFFFULL,0xFFFFFFFFBFFFFFFFULL,0xFFFFFFFF7FFFFFFFULL,
    0xFFFFFFFEFFFFFFFFULL,0xFFFFFFFDFFFFFFFFULL,0xFFFFFFFBFFFFFFFFULL,0xFFFFFFF7FFFFFFFFULL,
    0xFFFFFFEFFFFFFFFFULL,0xFFFFFFDFFFFFFFFFULL,0xFFFFFFBFFFFFFFFFULL,0xFFFFFF7FFFFFFFFFULL,
    0xFFFFFEFFFFFFFFFFULL,0xFFFFFDFFFFFFFFFFULL,0xFFFFFBFFFFFFFFFFULL,0xFFFFF7FFFFFFFFFFULL,
    0xFFFFEFFFFFFFFFFFULL,0xFFFFDFFFFFFFFFFFULL,0xFFFFBFFFFFFFFFFFULL,0xFFFF7FFFFFFFFFFFULL,
    0xFFFEFFFFFFFFFFFFULL,0xFFFDFFFFFFFFFFFFULL,0xFFFBFFFFFFFFFFFFULL,0xFFF7FFFFFFFFFFFFULL,
    0xFFEFFFFFFFFFFFFFULL,0xFFDFFFFFFFFFFFFFULL,0xFFBFFFFFFFFFFFFFULL,0xFF7FFFFFFFFFFFFFULL,
    0xFEFFFFFFFFFFFFFFULL,0xFDFFFFFFFFFFFFFFULL,0xFBFFFFFFFFFFFFFFULL,0xF7FFFFFFFFFFFFFFULL,
    0xEFFFFFFFFFFFFFFFULL,0xDFFFFFFFFFFFFFFFULL,0xBFFFFFFFFFFFFFFFULL,0x7FFFFFFFFFFFFFFFULL
};

BitMap::BitMap(): BitMap(ConfigMod::getInstance().getSegmentSize()/ConfigMod::getInstance().getPageSize()) {
    debug("%s\n", "Constructing an default bitmap, using SEGMENT_SIZE / PAGE_SIZE");
}

BitMap::BitMap(int size) {
    assert (!(m_unit & (m_unit-1))); // unit must be power of 2
    int num = size / m_unit;
    if (size % m_unit) ++num;

    debug("Malloc %d unit\n", num);
    m_bv = (ULL*) malloc (sizeof(ULL) * num);

    assert(m_bv);

    m_num = num;
    m_size = size;
    memset (m_bv, 0, sizeof(ULL) * num);

    m_bitmapMutex = new mutex();
}

BitMap::~BitMap() {
    free (m_bv);
    delete m_bitmapMutex;
}

bool BitMap::getBit(int addr) {
    assert (addr < m_size);
    return m_bv[addr/m_unit] & m_bitmask[addr%m_unit];
}

void BitMap::setBit(int addr) {
    assert (addr < m_size);
    m_bv[addr/m_unit] |= m_bitmask[addr%m_unit];
}

void BitMap::clearBit(int addr) {
    lock_guard <mutex> lk(*m_bitmapMutex);
    clearBitInternal(addr);
}

void BitMap::clearBitInternal(int addr) {
    assert (addr < m_size);
    m_bv[addr/m_unit] &= m_clearmask[addr%m_unit];
}

void BitMap::setAllOne() {
    lock_guard <mutex> lk(*m_bitmapMutex);
    for (int i = 0; i < m_num; i++)
        m_bv[i] = ~m_bv[i];    
    if (m_size % m_unit) {
        m_bv[m_num-1] &= (1ULL << (m_size % m_unit)) - 1ULL;
    }
}

int BitMap::getFirstZeroAndFlip(int addr) {
    lock_guard <mutex> lk(*m_bitmapMutex);
    int ret = getFirstZeroSince(addr);
    if (ret != -1) setBit(ret);
    return ret;
}

int BitMap::getFirstZerosAndFlip(int addr, int len) {
    lock_guard <mutex> lk(*m_bitmapMutex);
    int head = -1;
    bool reachEnd = false;
    for (int i = 0; i < len; i++) {
        int ret = getFirstZeroSince(addr+i);
        if (ret != -1 && (ret == head+i || head == -1)) {
            setBit(ret);
        } else if (ret == -1) {
            // search twice before declaring no contiuous space for writing data
            if (reachEnd) {
                fprintf(stderr, "Cannot alloc continuous block of len %d start %d last %d\n", len, head, ret);
                exit(-1);
            } else {
                reachEnd = true;
                for (int j = head; j < i + head; j++) {
                    //fprintf(stderr, "Clear %d\n", j);
                    clearBitInternal(j);
                }
                addr = 0;
                i = 0;
            }
        } else {
            //fprintf(stderr, "Cannot alloc continuous block of len %d start %d last %d, i %d\n", len, head, ret, i);
            for (int j = head; j < i + head; j++) {
                //fprintf(stderr, "Clear %d\n", j);
                clearBitInternal(j);
            }
            addr = ret;
            setBit(ret);
            i = 0;
        }
        if (!i) head = ret;
    }
    return head;
}

int BitMap::getFirstZeroSince(int addr) {
    /* sequential search for now,
     * may switch to random or more advanced later
     * number of element in bitmap is expected to be
     * 128GB / 512 KB / 64 = 4K, should not take long
     */
    int ret = -1;
    int st = addr / m_unit;
    ULL stmask = ~((1ULL << (addr % m_unit)) - 1ULL);
    
    for (int i = st; i < m_num; i++) {
        ULL pos = m_bv[i] ^ m_onemask;
        if (i == st) pos &= stmask;
        if (pos) {
            ULL mask = m_onemask;
            int depth = 0;
            int width = m_unit;
            do {
                width >>= 1;
                mask >>= width;
                if (pos & mask) {
                    pos &= mask;
                } else {
                    pos = (pos >> width) & mask;
                    depth += width;
                }
            } while(width);
            ret = i * m_unit + depth;
            break;
        }
    }
    if (ret >= m_size) ret = -1;
    return ret;
}

void BitMap::clearBitRange(int addr, int n) {
    lock_guard <mutex> lk(*m_bitmapMutex);
    assert (n > 0);

    int addr_ed = std::min(addr + n, m_size) - 1;
    int st = addr / m_unit;
    int ed = addr_ed / m_unit;

    assert (st < m_num && ed < m_num);

    if (st == ed) {
        ULL mask = 0;
        if (addr_ed % m_unit != m_unit - 1)
            mask = ((1ULL << (m_unit - 1 - addr_ed % m_unit)) - 1ULL) << (addr_ed % m_unit + 1);
        if (addr % m_unit) 
            mask |= (1ULL << (addr % m_unit)) - 1ULL;
        m_bv[st] &= mask;
    } else {
        // first 
        ULL mask = 0;
        if (addr % m_unit) 
            mask = (1ULL << (addr % m_unit)) - 1ULL;
        m_bv[st] &= mask;

        // middle
        for (int i = st+1; i < ed; i++) m_bv[i] = 0ULL;

        // last
        mask = 0;
        if (addr_ed % m_unit != m_unit - 1)
            mask = ((1ULL << (m_unit - 1 - addr_ed % m_unit)) - 1ULL) << (addr_ed % m_unit + 1);
        m_bv[ed] &= mask;
    }
}

vector<off_len_t> BitMap::getAllOne() {
    lock_guard <mutex> lk(*m_bitmapMutex);
    vector<off_len_t> retv;
    int off = 0, bit;
    while ((bit = getFirstZeroSince(off)) != -1) {
        if (bit != off) retv.push_back(make_pair(off, bit-off));
        off = bit+1;
    }
    if (off < m_size) {
        retv.push_back(make_pair(off,m_size-off));
    }
    return retv;
}
