#ifndef RAIDSETTING_HH_
#define RAIDSETTING_HH_

#include "codesetting.hh"
#include "common/enum.hh"

/**
 * High level setting of a disk array
 */
struct RaidSetting {
    int numDisks;               /**< Number of disks available */
    vector<int> aliveDisks;     /**< List  of (alive) disks */
    CodeSetting codeSetting;    /**< Coding scheme to use */
};

#endif /* RAIDSETTING_HH_ */
