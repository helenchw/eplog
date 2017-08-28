#ifndef ARC_HH_
#define ARC_HH_

/*
 * Implemented according to Figure 4 in 
 * ARC: A Self-tuning, low overhead replacement cache
 * (FAST'03)
 *
 */

#include "lru.hh"

class ArcList {
public:
    ArcList(size_t size = DEFAULT_LRU_SIZE);
    ~ArcList();
    /*
     * Insert key to list
     * 
     * @parm key the key to insert
     * @return a pair of key to fetch and key to remove
     */
    std::pair<Data<uint8_t>, std::vector<std::pair<Data<uint8_t>, bool> > > insert (Data<uint8_t> key);

    bool exists(Data<uint8_t> key, bool samePtr = false);

    void reset();

    void print();

private:
    size_t _ARC_SIZE;
    double _p;
    LruList *_t1, *_t2, *_b1, *_b2;
    std::unordered_set<Data<uint8_t> > _t1b1Joint;

    Data<uint8_t> replace (Data<uint8_t> key);

};

#endif //ifndef ARC_HH_
