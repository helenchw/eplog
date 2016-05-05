#ifndef CODESETTING_HH_
#define CODESETTING_HH_

#include <assert.h>
#include "common/enum.hh"
#include "common/configmod.hh"

/**
 * Coding scheme parameters
 */

struct CodeSetting {
    int n;                             /**< Total number of chunks */
    int k;                             /**< Number of data chunks */
    int w;                             /**< Word size */
    CodingScheme codingScheme;         /**< Encoding scheme */
    int isLog;                         /**< Whether this coding scheme is for log segments */

    /**
     * Constructor
     */
    CodeSetting(): n(0), k(0), w(1), codingScheme(DEFAULT_CODING) {}

    /**
     * Constructor
     * \param n total number of chunks
     * \param k total number of data chunks
     * \param codingScheme coding scheme
     * \param isLog whether this coding scheme is for log segments 
     */
    CodeSetting(int n, int k, CodingScheme codingScheme, bool isLog = false) :
            n(n), k(k), w(8), codingScheme(codingScheme), isLog(isLog) {
        assert (ConfigMod::getInstance().getNumBlockPerSegment() % k == 0 || isLog);
    }

    /**
     * Constructor
     * \param n total number of chunks
     * \param k total number of data chunks
     * \param w word size 
     * \param codingScheme coding scheme
     * \param isLog whether this coding scheme is for log segments 
     */
    CodeSetting(int n, int k, int w, CodingScheme codingScheme, bool isLog = false) :
            n(n), k(k), w(w), codingScheme(codingScheme), isLog(isLog) {
        assert (ConfigMod::getInstance().getNumBlockPerSegment() % k == 0 || isLog);
    }
};

#endif /* CODESETTING_HH_ */
