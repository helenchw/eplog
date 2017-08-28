#include "lru.hh"

LruList::LruList() {
    this->init(DEFAULT_LRU_SIZE);
}

LruList::LruList(size_t listSize) {
    this->init(listSize);
}

LruList::~LruList() {
    if (_slots) 
        delete [] _slots;
}

Data<uint8_t> LruList::insert(Data<uint8_t> key) {
    struct LruListRecord *target = 0;
    Data<uint8_t> ret;

    if (key.getData() == 0) {
        assert(key.getData() != 0);
        return ret;
    }

    _lock.lock();

    if (_existingRecords.count(key) > 0) {
        // reuse existing record
        target = _existingRecords.at(key);
        key = target->key;
        list_move(&target->listPtr, &_lruList);
    } else if (!list_empty(&_freeRecords)) {
        // allocate a new record
        target = container_of(_freeRecords.next, LruListRecord, listPtr);
        list_move(&target->listPtr, &_lruList);
    } else {
        // replace the oldest record
        target = container_of(_lruList.prev, LruListRecord, listPtr);
        list_move(&target->listPtr, &_lruList);
        ret = target->key;
        _existingRecords.erase(target->key);

    }

    // mark the metadata as exists in LRU list
    std::pair<Data<uint8_t>, LruListRecord*> record(key, target);
    target->key = key;
    if (_existingRecords.count(key) == 0) {
        _existingRecords.insert(record);
    }

    _lock.unlock();

    return ret;
}

bool LruList::exists(Data<uint8_t> key, Data<uint8_t> *copy) {
    bool ret = _existingRecords.count(key) > 0;
    if (ret && copy) {
        *copy = _existingRecords.find(key)->first;
    }
    return ret;
}

std::vector<Data<uint8_t> > LruList::getItems() {
    std::vector<Data<uint8_t> > list;

    _lock.lock_shared();

    struct list_head *rec;
    list_for_each_prev(rec, &_lruList) {
        list.push_back(container_of(rec, LruListRecord, listPtr)->key);
    }

    _lock.unlock_shared();

    return list;
}

std::vector<Data<uint8_t> > LruList::getTopNItems(size_t n) {
    std::vector<Data<uint8_t> > list;

    _lock.lock();

    struct list_head *rec;
    list_for_each_prev(rec, &_lruList) {
        list.push_back(container_of(rec, LruListRecord, listPtr)->key);
        if (list.size() > n)
            break;
    }

    _lock.unlock();

    return list;
}

std::unordered_set<Data<uint8_t> > LruList::getAllItems() {
    std::unordered_set<Data<uint8_t> > items;

    _lock.lock();

    struct list_head *rec;
    list_for_each_prev(rec, &_lruList) {
        items.insert(container_of(rec, LruListRecord, listPtr)->key);
    }

    _lock.unlock();

    return items;
}

Data<uint8_t> LruList::removeItem(Data<uint8_t> &key) {
    Data<uint8_t> ret;
    _lock.lock();

    if (key.getData() == 0) {
        assert(key.getData() != 0);
        return ret;
    }

    if (_existingRecords.count(key) > 0) {
        ret = _existingRecords.find(key)->first;
        list_move(&_existingRecords.at(key)->listPtr, &_freeRecords);
        _existingRecords.erase(key);
    }
    _lock.unlock();

    return ret;
}

size_t LruList::getItemCount() {
    size_t count = 0;

    _lock.lock_shared();

    count = _existingRecords.size();

    _lock.unlock_shared();

    return count;
}

size_t LruList::getFreeItemCount() {
    size_t count = 0;

    _lock.lock_shared();

    count = _listSize - _existingRecords.size();

    _lock.unlock_shared();

    return count;
}

size_t LruList::getAndReset(std::vector<Data<uint8_t> >& dest, size_t n) {

    _lock.lock();

    struct list_head *rec, *savePtr;
    list_for_each_safe(rec, savePtr, &_lruList) {
        Data<uint8_t> key = container_of(rec, LruListRecord, listPtr)->key;
        if (n == 0 || dest.size() < n) {
            dest.push_back(key);
        }
        _existingRecords.erase(key);
        list_move(rec, &_freeRecords);
    }

    _lock.unlock();

    return dest.size();
}

void LruList::reset() {
    _lock.lock();

    struct list_head *rec, *savePtr;

    list_for_each_safe(rec, savePtr, &_lruList) {
        _existingRecords.erase(container_of(rec, LruListRecord, listPtr)->key);
        list_move(rec, &_freeRecords);
    }

    _lock.unlock();
}

void LruList::init(size_t listSize) {
    _listSize = listSize;

    // init record
    INIT_LIST_HEAD(&_freeRecords);
    INIT_LIST_HEAD(&_lruList);
    _slots = new struct LruListRecord[ _listSize ];
    for (size_t i = 0; i < _listSize; i++) {
        INIT_LIST_HEAD(&_slots[ i ].listPtr);
        list_add(&_slots[ i ].listPtr, &_freeRecords);
    }

}

void LruList::print(FILE *output, bool countOnly) {
    struct list_head *rec;
    size_t i = 0;
    Data<uint8_t> key;

    if (countOnly) 
        fprintf(output, "    ");

    fprintf(output, "Free: %lu; Used: %lu\n", this->getFreeItemCount(), this->getItemCount());

    if (countOnly) return;

    _lock.lock();

    list_for_each(rec, &_lruList) {
        key = container_of(rec, LruListRecord, listPtr)->key;
        fprintf(output, "Record [%lu]: key(%d) = %.*s (%p)\n",
            i, key.getSize(), key.getSize(), key.getData(), key.getData() 
        );
        i++;
    }

    _lock.unlock();
}

