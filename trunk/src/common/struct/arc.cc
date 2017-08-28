#include <unordered_set>
#include "arc.hh"

/*
 * Implemented according to Figure 4 in 
 * ARC: A Self-tuning, low overhead replacement cache
 * (FAST'03)
 *
 */

ArcList::ArcList(size_t size) {
    _ARC_SIZE = size;
    _t1 = new LruList(size);
    _t2 = new LruList(size);
    _b1 = new LruList(size);
    _b2 = new LruList(size);
    _p = 0;
}

ArcList::~ArcList() {
    free(_t1);
    free(_t2);
    free(_b1);
    free(_b2);
}

std::pair<Data<uint8_t>, std::vector<std::pair<Data<uint8_t>, bool> > > ArcList::insert(Data<uint8_t> key) {
    std::unordered_map<Data<uint8_t>, int> victims;
    std::pair<Data<uint8_t>, std::vector<std::pair<Data<uint8_t>, bool> > > ret; // key to fetch, key to remove
    Data<uint8_t> tmp, keyCopy;

    size_t b1Size = _b1->getItemCount(), b2Size = _b2->getItemCount();
    double delta;

    if (_t1->exists(key) || _t2->exists(key)) {
        if (_t1->exists(key)) {
            // remove from Join of T1 and B1
            assert(!_b1->exists(key));
            _t1b1Joint.erase(key);
            // remove from T1 and insert to T2 MRU position
            tmp = _t1->removeItem(key);
            assert(tmp.isValid());
            tmp = _t2->insert(tmp);
            // remember any key to evict
            if (tmp.isValid()) {
                if (victims.count(tmp) == 0) {
                    victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                    ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, false));
                } else {
                    ret.second.at(victims.at(tmp)).second = false;
                }
            }
        } else if (_t2->exists(key)) {
            // reinsert to T2 MRU position
            tmp = _t2->insert(key);
            assert(!tmp.isValid());
        } else {
            // error
        }
    } else if (_b1->exists(key, &keyCopy)) {
        // not in _t1, _t2, but in _b1
        delta = (b1Size >= b2Size ? 1 : (double)(b2Size)/b1Size);
        _p = min(_p + delta, (double)_ARC_SIZE);

        tmp = replace(key);
        if (tmp.isValid()) {
            if (victims.count(tmp) == 0) {
                victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, false));
            } else {
                ret.second.at(victims.at(tmp)).second = false;
            }
        }

        tmp = _b1->removeItem(key);
        assert(tmp.isValid());
        tmp = _t2->insert(tmp);
        assert(!tmp.isValid());
        _t1b1Joint.erase(key);

        ret.first = keyCopy;
    } else if (_b2->exists(key, &keyCopy)) {
        delta = (b2Size >= b1Size ? 1 : (double)(b1Size)/b2Size);
        _p = max(_p - delta, 0.0);

        tmp = replace(key);
        if (tmp.isValid()) {
            if (victims.count(tmp) == 0) {
                victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, false));
            } else {
                ret.second.at(victims.at(tmp)).second = false;
            }
        }

        tmp = _b2->removeItem(key);
        assert(tmp.isValid());
        tmp = _t2->insert(tmp);
        assert(!tmp.isValid());

        ret.first = keyCopy;
    } else {
        if (_t1b1Joint.size() == _ARC_SIZE) {
            if (_t1->getItemCount() < _ARC_SIZE) {
                // still place in T1
                std::vector<Data<uint8_t> > v = _b1->getTopNItems(1);
                if (!v.empty()) {
                    tmp = _b1->removeItem(v[0]);
                    assert(!_t1->exists(v[0]));
                    _t1b1Joint.erase(v[0]);
                    if (tmp.isValid()) {
                        if (victims.count(tmp) == 0) {
                            victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                            ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, true));
                        }
                    }
                    tmp = replace(key);
                    if (tmp.isValid()) {
                        if (victims.count(tmp) == 0) {
                            victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                            ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, false));
                        } else {
                            ret.second.at(victims.at(tmp)).second = false;
                        }
                    }
                }
            } else {
                std::vector<Data<uint8_t> > v = _t1->getTopNItems(1);
                if (!v.empty()) {
                    tmp = _t1->removeItem(v[0]);
                    assert(!_b1->exists(v[0]));
                    _t1b1Joint.erase(v[0]);
                    if (tmp.isValid()) {
                        if (victims.count(tmp) == 0) {
                            victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                            ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, true));
                        }
                    }
                }
            }
        } else if (_t1b1Joint.size() < _ARC_SIZE) {
            size_t totalSize = _t1->getItemCount() + _t2->getItemCount() + _b1->getItemCount() + _b2->getItemCount();
            if (totalSize >= _ARC_SIZE) {
                std::vector<Data<uint8_t> > v = _b2->getTopNItems(1);
                if (totalSize == 2 * _ARC_SIZE && !v.empty()) {
                    tmp = _b2->removeItem(v[0]);
                    if (tmp.isValid()) {
                        if (victims.count(tmp) == 0) {
                            victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                            ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, true));
                        }
                    }
                }
                tmp = replace(key);
                if (tmp.isValid()) {
                    if (victims.count(tmp) == 0) {
                        victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                        ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, false));
                    } else {
                        ret.second.at(victims.at(tmp)).second = false;
                    }
                }
            }
        }

        key.dup();
        tmp = _t1->insert(key);
        if (tmp.isValid()) {
            if (victims.count(tmp) == 0) {
                victims.insert(std::pair<Data<uint8_t>, int> (tmp, ret.second.size()));
                ret.second.push_back(std::pair<Data<uint8_t>, bool> (tmp, true));
            }
        }
        _t1b1Joint.insert(key);
        ret.first = key;
    }
    return ret;
}

void ArcList::reset() {
    _t1->reset();
    _t2->reset();
    _b1->reset();
    _b2->reset();
    _p = 0;
}

void ArcList::print() {
    printf("p = %.3lf\n", _p);
    printf("--- T1 ---\n");
    _t1->print(stdout, true);
    printf("--- T2 ---\n");
    _t2->print(stdout, true);
    printf("--- B1 ---\n");
    _b1->print(stdout, true);
    printf("--- B2 ---\n");
    _b2->print(stdout, true);
}

bool ArcList::exists(Data<uint8_t> key, bool samePtr) {
    Data<uint8_t> copy;

    // check all lists
    if (_t1->exists(key, &copy)) {
        if (!samePtr) {
            return true;
        } else if (copy.getData() == key.getData()) {
            return true;
        }
    }

    if (_t2->exists(key, &copy)) {
        if (!samePtr) {
            return true;
        } else if (copy.getData() == key.getData()) {
            return true;
        }
    }

    if (_b1->exists(key, &copy)) {
        if (!samePtr) {
            return true;
        } else if (copy.getData() == key.getData()) {
            return true;
        }
    }

    if (_b2->exists(key, &copy)) {
        if (!samePtr) {
            return true;
        } else if (copy.getData() == key.getData()) {
            return true;
        }
    }

    return false;
}

Data<uint8_t> ArcList::replace(Data<uint8_t> key) {
    size_t t1Size = _t1->getItemCount();
    Data<uint8_t> ret;
    std::vector<Data<uint8_t> > v;

    if (t1Size > 0 && (t1Size > _p || (_b2->exists(key) && t1Size == (size_t)(_p)))) {
        v = _t1->getTopNItems(1);
        if (!v.empty()) {
            _t1->removeItem(v[0]);
            _b1->insert(v[0]);
        }
    } else {
        v = _t2->getTopNItems(1);
        if (!v.empty()) {
            _t2->removeItem(v[0]);
            _b2->insert(v[0]);
        }
    }

    if (!v.empty()) {
        ret = v[0];
    }

    return ret;
}
