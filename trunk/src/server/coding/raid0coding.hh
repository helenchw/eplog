#ifndef __RAID0CODING_HH__
#define __RAID0CODING_HH__

#include "stripingcoding.hh"
#include "coding.hh"

/**
 * RAID-0 coding
 */
class Raid0Coding: public StripingCoding {
public:

    Raid0Coding();
    ~Raid0Coding();

};

#endif
