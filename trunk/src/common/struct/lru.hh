#ifndef __SERVER_HOTNESS_LRU_HH__
#define __SERVER_HOTNESS_LRU_HH__

#include <string>
#include <unordered_map>
#include <unordered_set>
#include "../define.hh"
#include "keyvalue.hh"

extern "C" {
#include "list.hh"
}

#define DEFAULT_LRU_SIZE (1024 * 1024)

struct LruListRecord {
    Data<uint8_t> key;
    struct list_head listPtr;
};

class LruList {
public:

    LruList();
    LruList(size_t listSize);
    ~LruList();

    /*
     * Insert a new key to the list
     *
     * @parm key an existing instance of key 
     * @return key removed from the list; 0, if none is removed from list
     */
    Data<uint8_t> insert (Data<uint8_t> key);

    /*
     * Check if a key exists
     * 
     * @parm key an existing instance of key
     * @return whether the key exists
     */
    bool exists(Data<uint8_t> key, Data<uint8_t> *copy = 0);

    /*
     * Read all items in the list
     *
     * @return all items in the list
     */
    std::vector<Data<uint8_t> > getItems();

    /*
     * Read top N items in the list
     *
     * @parm n N
     * @return top N items in the list
     */
    std::vector<Data<uint8_t> > getTopNItems(size_t n);

    /*
     * Get all items in the list as a set
     *
     * @return all items in the list
     */
    std::unordered_set<Data<uint8_t> > getAllItems();

    /*
     * Remove an item from the list
     *
     * @parm key the key to remove from list 
     * @return the matched key if any
     */
    Data<uint8_t> removeItem(Data<uint8_t> &key);
    
    /*
     * Get number of items in the list
     *
     * @return number of items in the list 
     */

    size_t getItemCount();

    /*
     * Get number of items to insert before the list becomes full
     *
     * @return number of items to insert before the list becomes full
     */
    size_t getFreeItemCount();

    /*
     * Reset the list, i.e., clear all items
     */
    void reset();

    /*
     * Read top N items in the list, and reset the list
     *
     * @parm dest top N items in the list
     * @parm n N; 0 means to retrieve all items from the list
     * @return number of items retrieved
     */
    size_t getAndReset(std::vector<Data<uint8_t> > &dest, size_t n = 0);

    /*
     * Print the list
     * 
     * @parm output output file stream
     */
    void print(FILE *output = stdout, bool countOnly = false);

private:
    std::unordered_map<Data<uint8_t>, LruListRecord*> _existingRecords;
    struct LruListRecord *_slots;
    struct list_head _freeRecords;
    struct list_head _lruList;

    size_t _listSize;

    RWMutex _lock;

    /*
     * Init the list
     * 
     * @parm listSize list size, i.e., max number of items in list
     */
    void init(size_t listSize);
};

#endif

