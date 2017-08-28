#ifndef KEY_VALUE_HH_
#define KEY_VALUE_HH_

#include <stdlib.h>
#include "../hash.hh"
#include "../common/define.hh"

template <class Size> class Data {
public:
    Data() {
        _data = 0;
        _size = 0;
    }

    Data(const Data &d) {
        _data = d._data;
        _size = d._size;
    }

    ~Data() {}

    bool set (char *data, Size size, bool align = true) {
        // check size
        if (size == 0)
            return false;
        // malloc space if new data cannot fit in
        if (_data == 0 || _size != size) {
            if (_size != size)
                free(_data);
            if (align) {
                _data = (char*) buf_malloc(sizeof(char) * size);
            } else {
                _data = (char*) malloc(sizeof(char) * size);
            }
            // malloc failed
            if (_data == 0)
                return false;
        }
        // record the size
        _size = size;
        // copy data
        memcpy(_data, data, size);
        return true;
    }

    bool shadow_set(char *data, Size size) {
        _data = data;
        _size = size;
        return true;
    }

    bool dup () {
        if (_data == 0 || _size == 0)
            return false;
        char *tmp = (char *) buf_malloc(sizeof(char) * _size);
        memcpy(tmp, _data, _size);
        _data = tmp;
        return true;
    }

    char *getData () const {
        return _data;
    }

    uint32_t getSize() const {
        return _size;
    }

    bool release () {
        // release resources
        if (_data != 0) {
            free(_data);
            unset();
            return true;
        }
        return false;
    }

    void unset() {
        _data = 0;
        _size = 0;
    }

    bool isValid() const {
        return (_data != 0 && _size != 0);
    }

    bool operator==(const Data &v) const {
        return v._data != 0 && _data != 0 && v._size == _size && memcmp(v._data, _data, _size) == 0;
    }
    
protected:
    char *_data;
    Size _size;

};

template <class Length> class DiskData {
public:
    DiskData() {
        _loc.disk = 0;
        _dataLength = 0;
        _inMemory = false;
    }

    DiskData(uint64_t loc, Length dataLength) {
        _loc.disk = loc;
        _dataLength = dataLength;
        _inMemory = false;
    }

    DiskData(unsigned char *key, Length dataLength) {
        _loc.keyPtr = key;
        _dataLength = dataLength;
        _inMemory = true;
    }

    bool isInMemory() const {
        return _inMemory;
    }

    void removeFromMemory(uint64_t loc) {
        _inMemory = false;
        _loc.disk = loc;
    }

    void addToMemory(unsigned char *key) {
        _inMemory = true;
        _loc.keyPtr = key;
    }

    uint64_t getLocation() const {
        return _loc.dis;
    }

    unsigned char *getKey() const {
        return (_inMemory? _loc.keyPtr : 0);
    }

    Length getDataLength() const {
        return _dataLength;
    }

    bool setDataLength(Length dataLength) {
        _dataLength = dataLength;
        return true;
    }

    bool operator==(const DiskData &v) const {
        return false;
    }

    bool isValid() const {
        return (_dataLength != 0);
    }

protected:

    union {
        uint64_t disk;
        unsigned char *keyPtr;
    } _loc;

    Length _dataLength;
    bool _inMemory;
};

namespace std {
    template<class Size> struct hash<Data<Size> > {
        size_t operator()(const Data<Size> &value) const {
            size_t h = HashFunc::hash(value.getData(), value.getSize());
            return h;
        }
    };
}

#endif // define KEY_VALUE_HH_
